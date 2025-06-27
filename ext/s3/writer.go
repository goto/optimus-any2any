package s3

import (
	"context"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

// S3Writer implements io.Writer and io.ReaderFrom for streaming data to S3
type S3Writer struct {
	ctx        context.Context
	client     *s3.Client
	bucketName string
	key        string
	// internal use
	chunkSize int      // size of the chunk to write to S3, default is 64MB
	fileTmp   *os.File // temporary file to write the data until flush
	size      int      // size of the data written to the temporary file
}

// NewS3Writer creates a new writer that streams data to S3
func NewS3Writer(ctx context.Context, client *s3.Client, bucketName, key string) (*S3Writer, error) {
	w := &S3Writer{
		ctx:        ctx,
		client:     client,
		bucketName: bucketName,
		key:        key,
		chunkSize:  (1 << 20) * 64, // default chunk size is 64MB
		size:       0,
	}

	return w, nil
}

// Write implements io.Writer
func (w *S3Writer) Write(p []byte) (int, error) {
	if w.fileTmp == nil {
		// create a temporary writer
		if err := w.initFileTmp(); err != nil {
			return 0, err
		}
	}

	// write the data to the temporary writer
	n, err := w.fileTmp.Write(p)
	if err != nil {
		return n, errors.WithStack(err)
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

// flush uploads the buffer content to S3
func (w *S3Writer) Flush() error {
	// if no data has been written, do nothing
	if w.fileTmp == nil {
		return nil
	}

	// ensure all data is written to the file
	w.fileTmp.Sync()

	// seek back to the beginning before reading for upload
	if _, err := w.fileTmp.Seek(0, io.SeekStart); err != nil {
		return errors.WithStack(err)
	}

	// upload the file to S3
	_, err := manager.NewUploader(w.client).Upload(w.ctx, &s3.PutObjectInput{
		Bucket: &w.bucketName,
		Key:    &w.key,
		Body:   w.fileTmp,
	})
	if err != nil {
		return errors.WithStack(err)
	}

	// reset the temporary file for the next write
	w.size = 0
	w.fileTmp.Close()
	os.Remove(w.fileTmp.Name())
	w.fileTmp = nil

	return nil
}

// ReadFrom implements io.ReaderFrom for more efficient copies
func (w *S3Writer) ReadFrom(r io.Reader) (int64, error) {
	if w.fileTmp == nil {
		// create a temporary writer
		if err := w.initFileTmp(); err != nil {
			return 0, err
		}
	}

	// copy the data from the reader to the temporary file
	n, err := io.Copy(w.fileTmp, r)
	if err != nil {
		return n, errors.WithStack(err)
	}
	w.size += int(n)

	return n, errors.WithStack(w.Flush())
}

// Close closes the writer and deletes the temporary file
func (w *S3Writer) Close() error {
	if w.fileTmp == nil {
		return nil // nothing to close
	}
	if err := w.fileTmp.Close(); err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(os.Remove(w.fileTmp.Name()))
}

func (w *S3Writer) initFileTmp() error {
	f, err := os.CreateTemp("", "s3-writer-*.tmp")
	if err != nil {
		return errors.WithStack(err)
	}
	w.fileTmp = f
	return nil
}
