package s3

import (
	"context"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/djherbis/buffer"
	"github.com/djherbis/nio/v3"
	"github.com/pkg/errors"
)

// S3Writer implements io.Writer and io.ReaderFrom for streaming data to S3
type S3Writer struct {
	ctx        context.Context
	uploader   *manager.Uploader
	bucketName string
	key        string
	pipeReader *nio.PipeReader
	pipeWriter *nio.PipeWriter
	errChan    chan error
	closed     bool
	mu         sync.Mutex
}

// NewS3Writer creates a new writer that streams data to S3
func NewS3Writer(ctx context.Context, uploader *manager.Uploader, bucketName, key string) *S3Writer {
	buf := buffer.New(32 * 1024)
	pr, pw := nio.Pipe(buf)
	errChan := make(chan error, 1)

	w := &S3Writer{
		ctx:        ctx,
		uploader:   uploader,
		bucketName: bucketName,
		key:        key,
		pipeReader: pr,
		pipeWriter: pw,
		errChan:    errChan,
	}

	// start the upload process in a goroutine
	go func() {
		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
			Body:   pr,
		})
		errChan <- err
		close(errChan)
	}()

	return w
}

// Write implements io.Writer
func (w *S3Writer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		err := errors.New("writer is closed")
		return 0, errors.WithStack(err)
	}

	return w.pipeWriter.Write(p)
}

// ReadFrom implements io.ReaderFrom for more efficient copies
func (w *S3Writer) ReadFrom(r io.Reader) (int64, error) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		err := errors.New("writer is closed")
		return 0, errors.WithStack(err)
	}
	w.mu.Unlock()

	// copy directly from reader to the pipe
	n, err := io.Copy(w.pipeWriter, r)
	if err != nil {
		return n, errors.WithStack(err)
	}

	return n, nil
}

// Close closes the writer and waits for the upload to complete
func (w *S3Writer) Close() error {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return nil
	}
	w.closed = true
	w.mu.Unlock()

	// close the pipe writer to signal EOF to the pipe reader
	if err := w.pipeWriter.Close(); err != nil {
		return errors.WithStack(err)
	}

	// wait for the upload to complete and return any error
	return <-w.errChan
}
