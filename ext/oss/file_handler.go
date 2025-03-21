package oss

import (
	"bytes"
	"context"
	"io"
	"log/slog"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/pkg/errors"
)

type ossFileHandler struct {
	ctx    context.Context
	l      *slog.Logger
	buffer [][]byte

	w      io.WriteCloser
	bucket string
	path   string
}

// NewOSSFileHandler creates a new OSS file handler.
func NewOSSFileHandler(ctx context.Context, l *slog.Logger, client oss.AppendFileAPIClient, bucket, path string) (extcommon.FileHandler, error) {
	writeCloser, err := oss.NewAppendFile(ctx, client, bucket, path)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &ossFileHandler{
		ctx:    ctx,
		l:      l,
		buffer: make([][]byte, 0, 64),
		w:      writeCloser,
		bucket: bucket,
		path:   path,
	}, nil
}

// Write writes the given data to the file.
// n is the number of bytes written in the buffer.
// err is the error if any.
func (fh *ossFileHandler) Write(p []byte) (n int, err error) {
	if len(fh.buffer) >= cap(fh.buffer) {
		fh.l.Debug("oss file handler: buffer is full, flushing")
		if err := fh.flush(); err != nil {
			return 0, nil
		}
	}
	fh.buffer = append(fh.buffer, p)
	return len(p), nil
}

// Close closes the file.
func (fh *ossFileHandler) Close() error {
	// better to flush before close
	if err := fh.flush(); err != nil {
		return err
	}
	return fh.w.Close()
}

// Flush flushes the file.
func (fh *ossFileHandler) flush() error {
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
