package oss

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/pkg/errors"
)

var (
	defaultConnectTimeoutSeconds   = 0 * time.Second // no timeout
	defaultReadWriteTimeoutSeconds = 0 * time.Second // no timeout
)

const (
	ossInternalEndpointSuffix = "-internal.aliyuncs.com"
	ossPublicEndpointSuffix   = ".aliyuncs.com"
)

type OSSClientConfig struct {
	ConnectionTimeoutSeconds int
	ReadWriteTimeoutSeconds  int
}

type Client struct {
	*oss.Client
	ctx context.Context
}

type ossCredentials struct {
	AccessID      string `json:"access_key_id"`
	AccessKey     string `json:"access_key_secret"`
	Endpoint      string `json:"endpoint"`
	Region        string `json:"region"`
	SecurityToken string `json:"security_token"`
}

func parseOSSCredentials(data []byte) (*ossCredentials, error) {
	cred := new(ossCredentials)
	if err := json.Unmarshal(data, cred); err != nil {
		return nil, errors.WithStack(err)
	}

	return cred, nil
}

func NewOSSClient(ctx context.Context, rawCreds string, clientCfg OSSClientConfig) (*Client, error) {
	cred, err := parseOSSCredentials([]byte(rawCreds))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	credProvider := credentials.NewStaticCredentialsProvider(cred.AccessID, cred.AccessKey, cred.SecurityToken)
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credProvider).
		WithEndpoint(cred.Endpoint).
		WithRegion(cred.Region)

	// will be deprecated in the future
	timeoutCfg := defaultConnectTimeoutSeconds
	readWriteTimeoutCfg := defaultReadWriteTimeoutSeconds

	if clientCfg.ConnectionTimeoutSeconds > 0 {
		timeoutCfg = time.Duration(clientCfg.ConnectionTimeoutSeconds) * time.Second
	}
	if clientCfg.ReadWriteTimeoutSeconds > 0 {
		readWriteTimeoutCfg = time.Duration(clientCfg.ReadWriteTimeoutSeconds) * time.Second
	}
	cfg = cfg.WithConnectTimeout(timeoutCfg).WithReadWriteTimeout(readWriteTimeoutCfg)

	client := oss.NewClient(cfg)

	return &Client{Client: client, ctx: ctx}, nil
}

func (c *Client) NewWriter(destinationURI string) (io.WriteCloser, error) {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// create a new append file writer
	return oss.NewAppendFile(c.ctx, c.Client, u.Host, strings.TrimLeft(u.Path, "/"))
}

func (c *Client) Remove(destinationURI string) error {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}
	// remove object
	if response, err := c.Client.DeleteObject(c.ctx, &oss.DeleteObjectRequest{
		Bucket: oss.Ptr(u.Host),
		Key:    oss.Ptr(strings.TrimLeft(u.Path, "/")),
	}); err != nil {
		return errors.WithStack(err)
	} else if response.StatusCode >= 400 {
		err := errors.New(fmt.Sprintf("failed to delete object: %d; status %s", response.StatusCode, response.Status))
		return errors.WithStack(err)
	}
	return nil
}

func (c *Client) Copy(sourceURI, destinationURI string) error {
	uSrc, err := url.Parse(sourceURI)
	if err != nil {
		return errors.WithStack(err)
	}
	uDst, err := url.Parse(destinationURI)
	if err != nil {
		return errors.WithStack(err)
	}
	// copy object from source to destination
	if response, err := c.Client.CopyObject(c.ctx, &oss.CopyObjectRequest{
		SourceBucket: oss.Ptr(uSrc.Host),
		SourceKey:    oss.Ptr(strings.TrimLeft(uSrc.Path, "/")),
		Bucket:       oss.Ptr(uDst.Host),
		Key:          oss.Ptr(strings.TrimLeft(uDst.Path, "/")),
	}); err != nil {
		return errors.WithStack(err)
	} else if response.StatusCode >= 400 {
		err := errors.New(fmt.Sprintf("failed to copy object: %d; status: %s", response.StatusCode, response.Status))
		return errors.WithStack(err)
	}
	return nil
}

func (c *Client) GeneratePresignURL(destinationURI string, expirationInSeconds int) (string, error) {
	u, err := url.Parse(destinationURI)
	if err != nil {
		return "", errors.WithStack(err)
	}
	// get object request
	req := &oss.GetObjectRequest{
		Bucket: oss.Ptr(u.Host),
		Key:    oss.Ptr(strings.TrimLeft(u.Path, "/")),
	}
	// set expiration time
	expireAt := time.Now().Add(time.Duration(expirationInSeconds) * time.Second)
	// generate presigned URL
	presignResponse, err := c.Client.Presign(c.ctx, req, oss.PresignExpiration(expireAt))
	if err != nil {
		return "", errors.WithStack(err)
	}

	presignedURL := presignResponse.URL
	// remove internal endpoint suffix. Since presigned URL will be exposed to public,
	// usage of internal endpoint in the resulting URL will make it inaccessible.
	parsedURL, err := url.Parse(presignedURL)
	if err != nil {
		return "", errors.WithStack(err)
	}
	parsedURL.Host = strings.Replace(parsedURL.Host, ossInternalEndpointSuffix, ossPublicEndpointSuffix, 1)
	presignedURL = parsedURL.String()

	return presignedURL, nil
}
