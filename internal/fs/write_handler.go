package fs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/goto/optimus-any2any/internal/archive"
	"github.com/goto/optimus-any2any/internal/component/common"
	xio "github.com/goto/optimus-any2any/internal/io"
)

// WriteHandler is an interface that defines methods for writing data to a destination URI.
type WriteHandler interface {
	// Write writes raw data to the destination URI.
	Write(destinationURI string, raw []byte) error
	// Sync flushes the final data to the destination URI
	Sync() error
	// Close closes the handler and releases any resources.
	Close() error
	// DestinationURIs returns the list of destination URIs that this handler writes to.
	DestinationURIs() []string
}

type CommonWriteHandler struct {
	ctx            context.Context
	logger         *slog.Logger
	schema         string
	newWriter      func(string) (io.Writer, error)
	writers        map[string]xio.WriteFlushCloser
	counters       map[string]int
	concurrentFunc func([]func() error) error
	logBatchSize   int
	chunkOpts      []xio.Option

	// compression properties
	compressionEnabled                           bool
	compressionType                              string
	compressionAutoDetect                        bool
	compressionPassword                          string
	compressionTransientNewWriter                func(string) (xio.WriteFlusher, error)
	compressionTransientFilePathtoDestinationURI map[string]string
}

var _ WriteHandler = (*CommonWriteHandler)(nil)

func NewCommonWriteHandler(ctx context.Context, logger *slog.Logger, opts ...WriteOption) (*CommonWriteHandler, error) {
	w := &CommonWriteHandler{
		ctx:    ctx,
		logger: logger,
		schema: "noschema",
		newWriter: func(destinationURI string) (io.Writer, error) {
			return nil, errors.New("newWriter function not implemented")
		},
		writers:  make(map[string]xio.WriteFlushCloser),
		counters: make(map[string]int),
		concurrentFunc: func(funcs []func() error) error {
			return common.ConcurrentTask(ctx, 1, funcs)
		},
		logBatchSize: 1000,
		chunkOpts:    []xio.Option{},
	}
	for _, opt := range opts {
		if err := opt(w); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	return w, nil
}

func (h *CommonWriteHandler) Context() context.Context {
	return h.ctx
}

func (h *CommonWriteHandler) Logger() *slog.Logger {
	return h.logger
}

func (h *CommonWriteHandler) SetSchema(schema string) {
	h.schema = schema
}

func (h *CommonWriteHandler) SetNewWriterFunc(newWriter func(string) (io.Writer, error)) {
	h.newWriter = newWriter
}

// Write writes raw data to the destination URI.
func (h *CommonWriteHandler) Write(destinationURI string, raw []byte) error {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}
	if u.Scheme != h.schema {
		return errors.Errorf("invalid scheme: '%s', expected '%s'", u.Scheme, h.schema)
	}
	w, ok := h.writers[destinationURI]
	if !ok {
		h.logger.Info(fmt.Sprintf("creating new writer for destination URI: %s", destinationURI))
		var writer io.Writer
		if !h.compressionEnabled {
			writer, err = h.newWriter(destinationURI)
			if err != nil {
				return errors.WithStack(err)
			}
		} else {
			writer, err = h.compressionTransientNewWriter(destinationURI)
			if err != nil {
				return errors.WithStack(err)
			}
		}

		ext, _ := splitExtension(destinationURI)
		opts := append([]xio.Option{xio.WithExtension(ext)}, h.chunkOpts...)
		w = xio.NewChunkWriter(h.logger, writer, opts...)
		h.writers[destinationURI] = w
		h.counters[destinationURI] = 0
	}

	_, err = w.Write(raw)
	if err != nil {
		return errors.WithStack(err)
	}

	h.counters[destinationURI]++
	if h.counters[destinationURI]%h.logBatchSize == 0 {
		if !h.compressionEnabled {
			h.logger.Info(fmt.Sprintf("written %d records to file: %s", h.counters[destinationURI], destinationURI))
		} else {
			h.logger.Info(fmt.Sprintf("written %d records to transient file, future destination: %s", h.counters[destinationURI], destinationURI))
		}
	}

	return nil
}

func (h *CommonWriteHandler) Sync() error {
	funcs := []func() error{}
	for _, writer := range h.writers {
		funcs = append(funcs, writer.Flush)
	}
	// flush all writers concurrently
	if err := h.concurrentFunc(funcs); err != nil {
		return errors.WithStack(err)
	}

	if h.compressionEnabled {
		h.logger.Info("compression enabled, starting compression")
		if err := h.compress(); err != nil {
			return errors.WithStack(err)
		}
		h.logger.Info("compression completed")

		// after compression, we need to write the compressed files to their final destination URIs
		funcs := []func() error{}
		for filePath, destinationURI := range h.compressionTransientFilePathtoDestinationURI {
			funcs = append(funcs, func() error {
				// create writer for destination URI
				w, err := h.newWriter(destinationURI)
				if err != nil {
					return errors.WithStack(err)
				}
				h.logger.Debug(fmt.Sprintf("opening writer for %s", destinationURI))

				// open the transient file
				f, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
				if err != nil {
					return errors.WithStack(err)
				}
				defer func() {
					f.Close()
					os.Remove(filePath) // remove the transient file after writing
				}()
				h.logger.Debug(fmt.Sprintf("opened transient file %s", filePath))

				// copy its content to the destination URI
				if _, err := io.Copy(w, f); err != nil {
					return errors.WithStack(err)
				}
				h.logger.Info(fmt.Sprintf("written compressed file to %s", destinationURI))
				return nil
			})
		}
		// write the compressed files to their final destinations concurrently
		if err := h.concurrentFunc(funcs); err != nil {
			return errors.WithStack(err)
		}
	}
	totalRecords := 0
	for _, count := range h.counters {
		totalRecords += count
	}
	h.logger.Info(fmt.Sprintf("total %d records have been written", totalRecords))
	return nil
}

func (h *CommonWriteHandler) Close() error {
	for uri, writer := range h.writers {
		if err := writer.Close(); err != nil {
			return errors.WithStack(err)
		}
		h.logger.Info(fmt.Sprintf("closed writer for %s", uri))
		delete(h.writers, uri)
		delete(h.counters, uri)
	}
	return nil
}

func (h *CommonWriteHandler) DestinationURIs() []string {
	uris := make([]string, 0, len(h.writers))
	for uri := range h.writers {
		uris = append(uris, uri)
	}
	return uris
}

func (h *CommonWriteHandler) compress() error {
	if !h.compressionAutoDetect {
		return h.compressPerType(h.compressionType, h.compressionTransientFilePathtoDestinationURI)
	}

	h.logger.Info("compression auto detect enabled, compressing files based on their extensions")
	compressionTypeMap := map[string]map[string]string{}
	for filePath, destinationURI := range h.compressionTransientFilePathtoDestinationURI {
		// get the file extension and determine the compression type
		_, rightExt := splitExtension(destinationURI)
		compressionType := strings.TrimPrefix(rightExt, ".")
		destinationURI = strings.TrimSuffix(destinationURI, rightExt)

		// register the file path and destination URI in the map
		if _, ok := compressionTypeMap[compressionType]; !ok {
			compressionTypeMap[compressionType] = make(map[string]string)
		}
		compressionTypeMap[compressionType][filePath] = destinationURI
	}

	// compress each type separately
	h.compressionTransientFilePathtoDestinationURI = make(map[string]string) // reset the map to store the final destination URIs
	for compressionType, compressionTransientFilePathtoDestinationURI := range compressionTypeMap {
		if err := h.compressPerType(compressionType, compressionTransientFilePathtoDestinationURI); err != nil {
			return errors.WithStack(err)
		}
		// merge the compressed file paths into the main map
		for transientFilePath, destinationURI := range compressionTransientFilePathtoDestinationURI {
			h.compressionTransientFilePathtoDestinationURI[transientFilePath] = destinationURI
		}
	}

	return nil
}

// TODO: find a better way to refactor this
func (h *CommonWriteHandler) compressPerType(compressionType string, compressionTransientFilePathtoDestinationURI map[string]string) error {
	if compressionType == "" {
		return nil // No compression
	}

	// get all transient file paths and their corresponding destination URIs
	filePaths := []string{}
	destinationPaths := []string{}
	for transientFilePath, destinationURI := range compressionTransientFilePathtoDestinationURI {
		filePaths = append(filePaths, transientFilePath)
		u, _ := url.Parse(destinationURI)
		destinationPaths = append(destinationPaths, u.Path)
	}
	h.logger.Info(fmt.Sprintf("compressing %d files with compression type: %s", len(destinationPaths), compressionType))

	// compress based on the compression type
	var archivedPaths []string
	switch compressionType {
	case "gz", "gzip":
		for _, filePath := range filePaths {
			fileName := fmt.Sprintf("%s.%s", filepath.Base(filePath), compressionType)
			archivedPath := filepath.Join(filepath.Dir(filePath), fileName)
			archivedPaths = append(archivedPaths, archivedPath)

			f, err := os.OpenFile(archivedPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
			if err != nil {
				return errors.WithStack(err)
			}
			defer f.Close()

			archiver := archive.NewFileArchiver(h.Logger(), archive.WithExtension(compressionType))
			if err := archiver.Archive([]string{filePath}, f); err != nil {
				return errors.WithStack(err)
			}
			// remove the transient file after archiving
			if err := os.Remove(filePath); err != nil {
				return errors.WithStack(err)
			}
			// update the destination URI mapping
			if destinationURI, ok := compressionTransientFilePathtoDestinationURI[filePath]; ok {
				compressionTransientFilePathtoDestinationURI[archivedPath] = fmt.Sprintf("%s.%s", destinationURI, compressionType)
				delete(compressionTransientFilePathtoDestinationURI, filePath)
			}
		}
	case "zip", "tar.gz":
		// for zip & tar.gz file, the whole file is archived into a single archive file
		// single archive file is created in the same directory as the nearest common parent directory of the filepaths
		// eg. if filepaths are /tmp/a/b/c.txt and /tmp/a/d/e.txt, the archive file will be created as /tmp/a/archive.zip

		f, err := os.CreateTemp(os.TempDir(), "compression-*")
		if err != nil {
			return errors.WithStack(err)
		}
		defer f.Close()

		archiver := archive.NewFileArchiver(h.Logger(), archive.WithExtension(compressionType), archive.WithPassword(h.compressionPassword))
		if err := archiver.Archive(filePaths, f); err != nil {
			return errors.WithStack(err)
		}
		// remove the transient files after archiving
		for _, filePath := range filePaths {
			if err := os.Remove(filePath); err != nil {
				return errors.WithStack(err)
			}
		}
		// update the destination URI mapping
		for _, filePath := range filePaths {
			if destinationURI, ok := compressionTransientFilePathtoDestinationURI[filePath]; ok {
				compressionTransientFilePathtoDestinationURI[f.Name()] = destinationURI
				delete(compressionTransientFilePathtoDestinationURI, filePath)
			}
		}
		destinationURI := compressionTransientFilePathtoDestinationURI[f.Name()]
		u, err := url.Parse(destinationURI)
		if err != nil {
			return errors.WithStack(err)
		}

		// get the nearest common parent directory of the destination paths
		destinationDir := nearestCommonParentDir(destinationPaths)
		fileName := fmt.Sprintf("archive.%s", compressionType)
		if len(destinationPaths) == 1 {
			fileName = filepath.Base(destinationPaths[0])
			fileName = strings.TrimSuffix(fileName, filepath.Ext(fileName))
			fileName = fmt.Sprintf("%s.%s", fileName, compressionType)
		}
		u.Path = filepath.Join(destinationDir, fileName)
		// update the destination URI mapping
		compressionTransientFilePathtoDestinationURI[f.Name()] = u.String()
	default:
		return fmt.Errorf("unsupported compression type: %s", compressionType)
	}
	return nil
}

func nearestCommonParentDir(filePaths []string) string {
	dir := filepath.Dir(filePaths[0])
	for _, filePath := range filePaths[1:] {
		parentDir := filepath.Dir(filePath)
		i := 0
		for ; i < len(strings.Split(dir, "/")) && strings.Split(dir, "/")[i] == strings.Split(parentDir, "/")[i]; i++ {
		}
		dir = strings.Join(strings.Split(dir, "/")[:i], "/")
	}
	return dir
}

func splitExtension(path string) (string, string) {
	// get left most extension
	leftExt := ""
	rightExt := ""
	for {
		if filepath.Ext(path) == "" {
			break
		}
		rightExt = leftExt + rightExt
		leftExt = filepath.Ext(path)
		path = path[:len(path)-len(leftExt)]
	}
	return leftExt, rightExt
}
