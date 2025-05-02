package io

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/internal/helper"
)

type chunkWriter struct {
	l          *slog.Logger
	chunkSize  int
	size       int
	extension  string
	skipHeader bool // used only when extension is csv, tsv, or xlsx
	delimiter  rune // used only when extension is csv, or tsv
	writer     io.Writer

	// internal use
	writerTmp io.ReadWriteSeeker // temporary writer to write the data until chunk is full
	cleanTmp  func()
	written   int64
}

var _ io.WriteCloser = (*chunkWriter)(nil)
var _ WriteFlusher = (*chunkWriter)(nil)
var _ WriteFlushCloser = (*chunkWriter)(nil)

// NewChunkWriter creates a new chunk writer with the specified chunk size and extension
func NewChunkWriter(l *slog.Logger, writer io.Writer, opts ...Option) *chunkWriter {
	w := &chunkWriter{
		l:         l,
		chunkSize: (1 << 10) * 1024, // default chunk size is 1MB
		writer:    writer,
	}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Write writes data to the writer in chunks
func (w *chunkWriter) Write(p []byte) (n int, err error) {
	if w.writerTmp == nil {
		// create a temporary writer
		cleanFunc, err := w.initTmpWriter()
		if err != nil {
			return 0, err
		}
		// register the cleanup function
		w.cleanTmp = cleanFunc
	}

	// write the data to the temporary writer
	n, err = w.writerTmp.Write(p)
	if err != nil {
		return 0, err
	}
	w.size += n

	// if the size exceeds the chunk size, write to the actual writer
	if w.size >= w.chunkSize {
		if err := w.Flush(); err != nil {
			return n, err
		}
	}
	return n, nil
}

// Flush flushes the data to the actual writer
func (w *chunkWriter) Flush() error {
	// perform flush on the temporary writer
	// if it implement Flusher
	if f, ok := w.writerTmp.(WriteFlusher); ok {
		if err := f.Flush(); err != nil {
			return err
		}
	}

	// get the reader converter based on the extension
	reader, cleanFunc, err := w.getConvertedReader()
	if err != nil {
		return err
	}
	defer cleanFunc()

	// write the converted data to the actual writer
	n, err := io.Copy(w.writer, reader)
	if err != nil {
		return err
	}
	w.written += n

	// reset
	w.size = 0
	w.writerTmp = nil
	w.cleanTmp()

	return nil
}

// Close closes the writer and cleans up the temporary file
func (w *chunkWriter) Close() error {
	w.cleanTmp()
	return nil
}

// initTmpWriter initializes the temporary writer
func (w *chunkWriter) initTmpWriter() (func(), error) {
	// create a temporary file with the specified extension
	f, err := os.CreateTemp(os.TempDir(), fmt.Sprintf("*.%s", w.extension))
	if err != nil {
		return nil, err
	}
	w.writerTmp = f

	return func() {
		f.Close()
		os.Remove(f.Name())
	}, nil
}

// getConvertedReader returns the reader converter based on the extension
func (w *chunkWriter) getConvertedReader() (io.Reader, func() error, error) {
	if _, err := w.writerTmp.Seek(0, io.SeekStart); err != nil {
		return nil, nil, err
	}

	switch w.extension {
	case ".json":
		return w.writerTmp, func() error { return nil }, nil
	case ".csv":
		if w.delimiter == 0 {
			w.delimiter = ','
		}
		fallthrough
	case ".tsv":
		if w.delimiter == 0 {
			w.delimiter = '\t'
		}
		skipHeader := w.written > 0 || w.skipHeader
		return helper.FromJSONToCSV(w.l, w.writerTmp, skipHeader, w.delimiter)
	case ".xlsx":
		skipHeader := w.written > 0 || w.skipHeader
		return helper.FromJSONToXLSX(w.l, w.writerTmp, skipHeader)
	default:
		w.l.Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", w.extension))
		return w.writerTmp, func() error { return nil }, nil
	}
}
