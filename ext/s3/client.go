package s3

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/pkg/errors"

	xio "github.com/goto/optimus-any2any/internal/io"
)

// S3Client handles streaming data to S3
type S3Client struct {
	ctx    context.Context
	client *s3.Client
}

// NewS3Client creates a new uploader with the provided authentication
func NewS3Client(ctx context.Context, region string, credProvider aws.CredentialsProvider, endpointUrl string) (*S3Client, error) {
	configOpts := []func(*config.LoadOptions) error{
		config.WithCredentialsProvider(credProvider),
		config.WithRegion(region),
	}

	if endpointUrl != "" {
		configOpts = append(configOpts, config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           endpointUrl,
					SigningRegion: region,
				}, nil
			})))
	}

	cfg, err := config.LoadDefaultConfig(ctx, configOpts...)

	if err != nil {
		err = fmt.Errorf("failed to load AWS configuration: %w", err)
		return nil, errors.WithStack(err)
	}

	client := s3.NewFromConfig(cfg)

	return &S3Client{
		ctx:    ctx,
		client: client,
	}, nil
}

// NewWriter returns a writer for S3
func (s *S3Client) NewWriter(destinationURI string) (xio.WriteFlushCloser, error) {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return NewS3Writer(s.ctx, s.client, u.Host, strings.TrimLeft(u.Path, "/"))
}

// Remove deletes an object from S3
func (s *S3Client) Remove(destinationURI string) error {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}

	_, err = s.client.DeleteObject(s.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(u.Host),
		Key:    aws.String(strings.TrimLeft(u.Path, "/")),
	})
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
