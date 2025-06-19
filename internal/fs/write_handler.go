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
	ctx          context.Context
	logger       *slog.Logger
	schema       string
	newWriter    func(string) (io.Writer, error)
	writers      map[string]xio.WriteFlushCloser
	counters     map[string]int
	logBatchSize int
	chunkOpts    []xio.Option

	// compression properties
	compressionEnabled                           bool
	compressionType                              string
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
		writers:      make(map[string]xio.WriteFlushCloser),
		counters:     make(map[string]int),
		logBatchSize: 1000,
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
		writer, err := h.newWriter(destinationURI)
		if err != nil {
			return errors.WithStack(err)
		}
		if h.compressionEnabled {
			writer, err = h.compressionTransientNewWriter(destinationURI)
			if err != nil {
				return errors.WithStack(err)
			}
		}

		ext := filepath.Ext(destinationURI)
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
		}
	}

	return nil
}

func (h *CommonWriteHandler) Sync() error {
	for _, writer := range h.writers {
		if err := writer.Flush(); err != nil {
			return err
		}
	}
	if h.compressionEnabled {
		if err := h.compress(); err != nil {
			return errors.WithStack(err)
		}
		h.logger.Info("compression completed")
		for filePath, destinationURI := range h.compressionTransientFilePathtoDestinationURI {
			// create writer for destination URI
			w, err := h.newWriter(destinationURI)
			if err != nil {
				return errors.WithStack(err)
			}

			// open the transient file
			f, err := os.OpenFile(filePath, os.O_RDONLY, 0666)
			if err != nil {
				return errors.WithStack(err)
			}
			defer func() {
				f.Close()
				os.Remove(filePath) // remove the transient file after writing
			}()

			// copy its content to the destination URI
			if _, err := io.Copy(w, f); err != nil {
				return errors.WithStack(err)
			}
			h.logger.Info(fmt.Sprintf("written compressed file to %s", destinationURI))
		}
	}
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
	// get all transient file paths
	filePaths := make([]string, 0, len(h.compressionTransientFilePathtoDestinationURI))
	for transientFilePath := range h.compressionTransientFilePathtoDestinationURI {
		filePaths = append(filePaths, transientFilePath)
	}

	// compress based on the compression type
	compressionType := h.compressionType
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
			if destinationURI, ok := h.compressionTransientFilePathtoDestinationURI[filePath]; ok {
				h.compressionTransientFilePathtoDestinationURI[archivedPath] = destinationURI
				delete(h.compressionTransientFilePathtoDestinationURI, filePath)
			}
		}
	case "zip", "tar.gz":
		// for zip & tar.gz file, the whole file is archived into a single archive file
		// single archive file is created in the same directory as the nearest common parent directory of the filepaths
		// eg. if filepaths are /tmp/a/b/c.txt and /tmp/a/d/e.txt, the archive file will be created as /tmp/a/archive.zip

		// get the nearest common parent directory of the filepaths
		dir := filepath.Dir(filePaths[0])
		for _, filePath := range filePaths[1:] {
			parentDir := filepath.Dir(filePath)
			i := 0
			for ; i < len(strings.Split(dir, "/")) && strings.Split(dir, "/")[i] == strings.Split(parentDir, "/")[i]; i++ {
			}
			dir = strings.Join(strings.Split(dir, "/")[:i], "/")
		}

		fileName := fmt.Sprintf("archive.%s", compressionType)
		archivedPath := filepath.Join(dir, fileName)
		archivedPaths = append(archivedPaths, archivedPath)

		f, err := os.OpenFile(archivedPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
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
			if destinationURI, ok := h.compressionTransientFilePathtoDestinationURI[filePath]; ok {
				h.compressionTransientFilePathtoDestinationURI[archivedPath] = destinationURI
				delete(h.compressionTransientFilePathtoDestinationURI, filePath)
			}
		}
		destinationURI := h.compressionTransientFilePathtoDestinationURI[archivedPath]
		u, err := url.Parse(destinationURI)
		if err != nil {
			return errors.WithStack(err)
		}
		u.Path = archivedPath
		h.compressionTransientFilePathtoDestinationURI[archivedPath] = u.String()
	default:
		return fmt.Errorf("unsupported compression type: %s", compressionType)
	}
	return nil
}
