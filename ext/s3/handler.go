package s3

import (
	"context"
	"io"
	"log/slog"

	"github.com/goto/optimus-any2any/internal/fs"
	"github.com/pkg/errors"
)

type s3Handler struct {
	*fs.CommonWriteHandler
}

var _ fs.WriteHandler = (*s3Handler)(nil)

func NewS3Handler(ctx context.Context, logger *slog.Logger, client *S3Client, enableOverwrite bool, opts ...fs.WriteOption) (*s3Handler, error) {
	writeFunc := func(destinationURI string) (io.Writer, error) {
		// remove object if it exists and overwrite is enabled
		if enableOverwrite {
			if err := client.DeleteObject(ctx, destinationURI); err != nil {
				return nil, errors.WithStack(err)
			}
		}
		// create a new writer for the S3 upload
		return client.GetUploadWriter(ctx, destinationURI)
	}

	// set appropriate schema and writer function
	w, err := fs.NewCommonWriteHandler(ctx, logger, append(opts,
		fs.WithWriteSchema("s3"), fs.WithWriteNewWriterFunc(writeFunc))...,
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create S3 handler
	return &s3Handler{w}, nil
}
