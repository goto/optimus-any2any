package fs

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/goto/optimus-any2any/internal/component/common"
	xio "github.com/goto/optimus-any2any/internal/io"
)

type WriteOption func(w *CommonWriteHandler) error

func WithWriteCompression(compressionType string) WriteOption {
	return func(w *CommonWriteHandler) error {
		if compressionType == "" {
			return nil // No compression
		}
		switch compressionType {
		case "auto":
			w.compressionAutoDetect = true
			fallthrough
		case "gz", "gzip", "tar.gz", "zip":
			w.compressionEnabled = true
			w.compressionType = compressionType
			w.compressionTransientFilePathtoDestinationURI = make(map[string]string)
			w.compressionTransientNewWriter = func(destinationURI string) (xio.WriteFlusher, error) {
				// prepare transient directory for compression
				dir, err := os.MkdirTemp(os.TempDir(), "*")
				if err != nil {
					return nil, errors.WithStack(err)
				}

				// use transient file path for compression
				u, _ := url.Parse(destinationURI)
				_, compressionExt := SplitExtension(destinationURI)
				transientFilePath := filepath.Join(dir, strings.TrimSuffix(u.Path, compressionExt))
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

func WithWriteCompressionStaticDestinationURI(destinationURI string) WriteOption {
	return func(w *CommonWriteHandler) error {
		// only set static destination URI if it does not contain template placeholders
		if !(strings.Contains(destinationURI, "[[") && strings.Contains(destinationURI, "]]")) {
			w.compressionStaticDestinationURI = destinationURI
		}
		return nil
	}
}

func WithConcurrentLimiter(concurrentLimiter common.ConcurrentLimiter) WriteOption {
	return func(w *CommonWriteHandler) error {
		if concurrentLimiter == nil {
			return errors.New("concurrent limiter cannot be nil")
		}
		w.ConcurrentLimiter = concurrentLimiter
		return nil
	}
}

func WithWriteCompressionPassword(password string) WriteOption {
	return func(w *CommonWriteHandler) error {
		if password == "" {
			return nil // No compression password
		}
		if w.compressionType == "zip" || w.compressionAutoDetect {
			w.compressionPassword = password
			return nil
		}
		return errors.New("compression password is only supported for zip compression")
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
