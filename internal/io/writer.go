package io

import (
	"bufio"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

type BufferedWriter struct {
	*bufio.Writer
}

// NewBufferedWriter creates a new buffered writer.
func NewBufferedWriter(w io.Writer) *BufferedWriter {
	return &BufferedWriter{
		Writer: bufio.NewWriterSize(w, 32*1024),
	}
}

// Close flushes remaining data and closes the writer.
func (b *BufferedWriter) Close() error {
	return b.Writer.Flush()
}

// NewWriteHandler creates a new write handler.
func NewWriteHandler(l *slog.Logger, path string) (io.WriteCloser, error) {
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
