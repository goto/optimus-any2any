package file

import (
	"context"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"

	"github.com/goto/optimus-any2any/internal/fs"
	"github.com/pkg/errors"
)

type fileHandler struct {
	*fs.CommonWriteHandler
}

var _ fs.WriteHandler = (*fileHandler)(nil)

func NewFileHandler(ctx context.Context, logger *slog.Logger, opts ...fs.WriteOption) (*fileHandler, error) {
	writerFunc := func(destinationURI string) (io.Writer, error) {
		u, _ := url.Parse(destinationURI)
		// ensure the directory exists
		dir := filepath.Dir(u.Path)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return nil, errors.WithStack(err)
		}

		// open the file for writing, creating it if it doesn't exist
		return os.OpenFile(u.Path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	}

	// set appropriate schema and writer function
	w, err := fs.NewCommonWriteHandler(ctx, logger, append(opts,
		fs.WithWriteSchema("file"), fs.WithWriteNewWriterFunc(writerFunc))...,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create file handler
	return &fileHandler{w}, nil
}
