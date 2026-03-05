package gcs

import (
	"context"
	"io"

	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type Client struct {
	ctx    context.Context
	client *storage.Client
}

func NewGCSClient(ctx context.Context, svcAccJSON string) (*Client, error) {
	creds, err := credentials.DetectDefault(&credentials.DetectOptions{
		CredentialsJSON: []byte(svcAccJSON),
		Scopes:          []string{"https://www.googleapis.com/auth/devstorage.read_only"},
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	client, err := storage.NewClient(ctx, option.WithAuthCredentials(creds))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &Client{ctx: ctx, client: client}, nil
}

func (c *Client) ListObjects(ctx context.Context, bucket, prefix string) ([]string, error) {
	query := &storage.Query{Prefix: prefix}
	it := c.client.Bucket(bucket).Objects(ctx, query)

	var keys []string
	for {
		attrs, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if attrs.Name == "" || attrs.Name[len(attrs.Name)-1] == '/' {
			continue
		}
		keys = append(keys, attrs.Name)
	}
	return keys, nil
}

func (c *Client) OpenFile(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	reader, err := c.client.Bucket(bucket).Object(key).NewReader(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return reader, nil
}

func (c *Client) Close() error {
	if c == nil || c.client == nil {
		return nil
	}
	return c.client.Close()
}
