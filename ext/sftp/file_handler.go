package sftp

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/pkg/errors"
	"github.com/pkg/sftp"
)

type sftpFileHandler struct {
	ctx    context.Context
	l      *slog.Logger
	buffer [][]byte

	w io.WriteCloser
}

// NewSFTPFileHandler creates a new SFTP file handler.
func NewSFTPFileHandler(ctx context.Context, l *slog.Logger, client *sftp.Client, path string) (extcommon.FileHandler, error) {
	f, err := client.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &sftpFileHandler{
		ctx:    ctx,
		l:      l,
		buffer: make([][]byte, 0, 64),
		w:      f,
	}, nil
}

// Write writes the given data to the file.
// n is the number of bytes written in the buffer.
// err is the error if any.
func (fh *sftpFileHandler) Write(p []byte) (n int, err error) {
	if len(fh.buffer) >= cap(fh.buffer) {
		fh.l.Debug("sftp file handler: buffer is full, flushing")
		if err := fh.Flush(); err != nil {
			return 0, nil
		}
	}
	fh.buffer = append(fh.buffer, p)
	return len(p), nil
}

// Close closes the file.
func (fh *sftpFileHandler) Close() error {
	// better to flush before close
	if err := fh.Flush(); err != nil {
		return err
	}
	return fh.w.Close()
}

// Flush flushes the file.
func (fh *sftpFileHandler) Flush() error {
	if len(fh.buffer) == 0 {
		return nil
	}
	data := bytes.Join(fh.buffer, []byte(""))
	if _, err := fh.w.Write(data); err != nil {
		return errors.WithStack(err)
	}
	fh.buffer = fh.buffer[:0]
	return nil
}
