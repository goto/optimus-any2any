package oss

import (
	"context"
	"io"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/pkg/errors"
)

type multipartWriter struct {
	pw   *io.PipeWriter
	done chan error
}

// NewMultipartWriter creates a new multipart writer for the given bucket and key
func NewMultipartWriter(ctx context.Context, client *oss.Client, bucket, key string) (*multipartWriter, error) {
	pr, pw := io.Pipe()
	w := &multipartWriter{
		pw:   pw,
		done: make(chan error, 1),
	}

	uploader := client.NewUploader()
	go func() {
		_, err := uploader.UploadFrom(ctx, &oss.PutObjectRequest{
			Bucket: oss.Ptr(bucket),
			Key:    oss.Ptr(key),
		}, pr)
		if err != nil {
			pr.CloseWithError(err)
		}
		w.done <- err
	}()

	return w, nil
}

// Write writes data to the multipart writer
func (w *multipartWriter) Write(p []byte) (int, error) {
	return w.pw.Write(p)
}

// Close closes the multipart writer and waits for the upload to complete
func (w *multipartWriter) Close() error {
	if err := w.pw.Close(); err != nil {
		return errors.WithStack(err)
	}
	if err := <-w.done; err != nil {
		return errors.WithStack(err)
	}
	return nil
}
