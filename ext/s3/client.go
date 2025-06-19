package s3

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"
)

// S3Client handles streaming data to S3
type S3Client struct {
	client   *s3.Client
	uploader *manager.Uploader
}

// NewS3Client creates a new uploader with the provided authentication
func NewS3Client(ctx context.Context, region string, credProvider aws.CredentialsProvider) (*S3Client, error) {
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

	return &S3Client{
		client:   client,
		uploader: uploader,
	}, nil
}

// GetUploadWriter returns a writer for streaming data to S3
func (s *S3Client) GetUploadWriter(ctx context.Context, destinationURI string) (*S3Writer, error) {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return NewS3Writer(ctx, s.uploader, u.Host, strings.TrimLeft(u.Path, "/")), nil
}

// DeleteObject deletes an object from S3
func (s *S3Client) DeleteObject(ctx context.Context, destinationURI string) error {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}

	bucketName := u.Host
	key := strings.TrimLeft(u.Path, "/")
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucketName,
		Key:    &key,
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
