package io

import (
	"bufio"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type WriteFlusher interface {
	io.Writer
	Flush() error
}

type WriteFlushCloser interface {
	WriteFlusher
	io.Closer
}

type BufferedWriter struct {
	*bufio.Writer
}

// NewBufferedWriter creates a new buffered writer.
func NewBufferedWriter(w io.Writer) *BufferedWriter {
	return &BufferedWriter{
		Writer: bufio.NewWriterSize(w, 32*1024),
	}
}

// NewWriteHandler creates a new write handler.
// Close must be called after use to flush remaining data.
func NewWriteHandler(l *slog.Logger, path string) (WriteFlusher, error) {
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
	return NewBufferedWriter(f), nil
}
