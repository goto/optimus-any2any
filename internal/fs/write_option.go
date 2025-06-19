package fs

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"github.com/pkg/errors"

	xio "github.com/goto/optimus-any2any/internal/io"
)

type WriteOption func(w *CommonWriteHandler) error

func WithWriteCompression(compressionType string) WriteOption {
	return func(w *CommonWriteHandler) error {
		if compressionType == "" {
			return nil // No compression
		}
		switch compressionType {
		case "gz", "gzip", "tar.gz", "zip":
			w.compressionEnabled = true
			w.compressionType = compressionType
			w.compressionTransientFilePathtoDestinationURI = make(map[string]string)
			w.compressionTransientNewWriter = func(destinationURI string) (xio.WriteFlusher, error) {
				// prepare transient directory for compression
				dir, err := os.MkdirTemp(os.TempDir(), "compression-*")
				if err != nil {
					return nil, errors.Wrap(err, "failed to create temp directory for compression")
				}

				// use transient file path for compression
				u, _ := url.Parse(destinationURI)
				transientFilePath := filepath.Join(dir, u.Path)
				w.logger.Info(fmt.Sprintf("using transient file path for compression: %s", transientFilePath))
				w.compressionTransientFilePathtoDestinationURI[transientFilePath] = destinationURI

				return xio.NewWriteHandler(w.logger, transientFilePath)
			}
		default:
			return errors.New("unsupported compression type: " + compressionType)
		}
		return nil
	}
}

func WithWriteCompressionPassword(password string) WriteOption {
	return func(w *CommonWriteHandler) error {
		if password == "" {
			return nil // No compression password
		}
		if w.compressionType != "zip" {
			return errors.New("compression password is only supported for zip compression")
		}
		w.compressionPassword = password
		return nil
	}
}

func WithWriteChunkOptions(chunkOpts ...xio.Option) WriteOption {
	return func(w *CommonWriteHandler) error {
		if len(chunkOpts) == 0 {
			return nil // No chunk options
		}
		w.chunkOpts = chunkOpts
		return nil
	}
}

func WithWriteSchema(schema string) WriteOption {
	return func(w *CommonWriteHandler) error {
		if schema == "" {
			return errors.New("schema cannot be empty")
		}
		w.schema = schema
		return nil
	}
}

func WithWriteNewWriterFunc(newWriter func(string) (io.Writer, error)) WriteOption {
	return func(w *CommonWriteHandler) error {
		if newWriter == nil {
			return errors.New("newWriter function cannot be nil")
		}
		w.newWriter = newWriter
		return nil
	}
}
