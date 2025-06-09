package io

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/goto/optimus-any2any/internal/fileconverter"
)

type chunkWriter struct {
	l          *slog.Logger
	noChunk    bool // if true, no chunking is done, data is written in one go
	chunkSize  int
	size       int
	extension  string
	skipHeader bool // used only when extension is csv, tsv, or xlsx
	delimiter  rune // used only when extension is csv, or tsv
	writer     io.Writer

	// internal use
	fileTmp *os.File // temporary file to write the data until chunk is full
	written int64
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
	if w.extension == ".xlsx" {
		l.Warn("xlsx format does not support chunking, file will be written in one go")
	}
	return w
}

// Write writes data to the writer in chunks
func (w *chunkWriter) Write(p []byte) (n int, err error) {
	if w.fileTmp == nil {
		// create a temporary writer
		if err := w.initFileTmp(); err != nil {
			return 0, err
		}
	}

	// write the data to the temporary writer
	n, err = w.fileTmp.Write(p)
	if err != nil {
		return 0, err
	}
	w.size += n
	w.l.Debug(fmt.Sprintf("write %d bytes to temporary writer", n))

	// if the size exceeds the chunk size, write to the actual writer
	if !w.noChunk && w.size >= w.chunkSize {
		w.l.Debug(fmt.Sprintf("chunk size reached: %d bytes. Flushing to destination writer", w.size))
		if err := w.Flush(); err != nil {
			return n, err
		}
	}
	return n, nil
}

// Flush flushes the data to the actual writer
func (w *chunkWriter) Flush() error {
	// there might be case where Flush() is called immediately without Write() before it
	// and in every Flush, temp writer is set to null. So need this check to avoid nil pointer dereference
	if w.fileTmp == nil {
		w.l.Debug("no data to flush")
		return nil
	}

	// ensure all data is written to the temporary file
	w.fileTmp.Sync()

	// convert the temporary file to the desired format
	convertedFileTmp, err := w.getConvertedFileTmp()
	if err != nil {
		return err
	}
	defer func() {
		// remove converted file
		convertedFileTmp.Close()
		os.Remove(convertedFileTmp.Name())
	}()

	// write the converted data to the actual writer
	n, err := io.Copy(w.writer, convertedFileTmp)
	if err != nil {
		return err
	}
	w.written += n
	w.l.Debug(fmt.Sprintf("flushed %d bytes to destination writer", n))

	// reset tmp file
	w.size = 0
	w.fileTmp.Close()
	os.Remove(w.fileTmp.Name())
	w.fileTmp = nil

	return nil
}

// Close closes the writer and cleans up the temporary file
func (w *chunkWriter) Close() error {
	if w.fileTmp != nil {
		w.fileTmp.Close()
		os.Remove(w.fileTmp.Name())
	}
	return nil
}

// initFileTmp initializes the temporary writer
func (w *chunkWriter) initFileTmp() error {
	// create a temporary file
	f, err := os.CreateTemp(os.TempDir(), "*.json")
	if err != nil {
		return nil
	}
	w.fileTmp = f
	return nil
}

func (w *chunkWriter) getConvertedFileTmp() (*os.File, error) {
	if _, err := w.fileTmp.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	switch strings.ToLower(w.extension) {
	case ".json":
		return w.fileTmp, nil
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
		return fileconverter.JSON2CSV(w.l, w.fileTmp, skipHeader, w.delimiter)
	case ".xlsx":
		skipHeader := w.written > 0 || w.skipHeader
		return fileconverter.JSON2XLSX(w.l, w.fileTmp, skipHeader)
	default:
		w.l.Warn(fmt.Sprintf("unsupported file format: %s, use default (json)", w.extension))
		return w.fileTmp, nil
	}
}
