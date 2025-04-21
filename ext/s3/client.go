package s3

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

// S3ClientUploader handles streaming data to S3
type S3ClientUploader struct {
	client     *s3.Client
	uploader   *manager.Uploader
	bucketName string
}

// NewS3ClientUploader creates a new uploader with the provided authentication
func NewS3ClientUploader(ctx context.Context, bucketName, region string, credProvider aws.CredentialsProvider) (*S3ClientUploader, error) {
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithCredentialsProvider(credProvider),
		config.WithRegion(region),
	)
	if err != nil {
		err = fmt.Errorf("failed to load AWS configuration: %w", err)
		return nil, errors.WithStack(err)
	}

	client := s3.NewFromConfig(cfg)
	uploader := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024
		u.Concurrency = 3
	})

	return &S3ClientUploader{
		client:     client,
		uploader:   uploader,
		bucketName: bucketName,
	}, nil
}

// GetUploadWriter returns a writer for streaming data to S3
func (s *S3ClientUploader) GetUploadWriter(ctx context.Context, key string) *S3Writer {
	return NewS3Writer(ctx, s.uploader, s.bucketName, key)
}
