package file

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/pkg/errors"
)

type stdFileHandler struct {
	l      *slog.Logger
	f      *os.File
	buffer [][]byte
}

// NewStdFileHandler creates a new file handler.
func NewStdFileHandler(l *slog.Logger, path string) (extcommon.FileHandler, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &stdFileHandler{
		l:      l,
		f:      f,
		buffer: make([][]byte, 0, 64),
	}, nil
}

// Write writes the given data to the file.
// n is the number of bytes written in the buffer.
// err is the error if any.
func (fh *stdFileHandler) Write(p []byte) (n int, err error) {
	if len(fh.buffer) >= cap(fh.buffer) {
		fh.l.Debug("file handler: buffer is full, flushing")
		if err := fh.Flush(); err != nil {
			return 0, err
		}
	}
	fh.l.Debug(fmt.Sprintf("file handler(%s): writing %d bytes", fh.f.Name(), len(p)))
	fh.l.Debug(fmt.Sprintf("file handler(%s): data: %s", fh.f.Name(), string(p)))
	fh.buffer = append(fh.buffer, p)
	return len(p), nil
}

// Close closes the file.
func (fh *stdFileHandler) Close() error {
	// better to flush before close
	if err := fh.Flush(); err != nil {
		return err
	}
	return fh.f.Close()
}

// Flush flushes the file.
func (fh *stdFileHandler) Flush() error {
	if len(fh.buffer) == 0 {
		return nil
	}
	fh.l.Debug(fmt.Sprintf("file handler(%s): persist data: %s", fh.f.Name(), string(bytes.Join(fh.buffer, []byte("")))))
	if _, err := fh.f.Write(bytes.Join(fh.buffer, []byte(""))); err != nil {
		return errors.WithStack(err)
	}
	fh.buffer = fh.buffer[:0]
	return nil
}
