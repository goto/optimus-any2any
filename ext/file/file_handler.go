package file

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

// NewStdFileHandler creates a new file handler.
func NewStdFileHandler(l *slog.Logger, path string) (io.WriteCloser, error) {
	dir := filepath.Dir(path)
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, errors.WithStack(err)
		}
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return f, nil
}
