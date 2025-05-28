package oss

import (
	"time"

	"github.com/goccy/go-json"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/pkg/errors"
)

var (
	defaultConnectTimeoutSeconds   = 30 * time.Second
	defaultReadWriteTimeoutSeconds = 60 * time.Second
)

type OSSClientConfig struct {
	ConnectionTimeoutSeconds int
	ReadWriteTimeoutSeconds  int
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
		return nil, err
	}

	return cred, nil
}

func NewOSSClient(rawCreds string, clientCfg OSSClientConfig) (*oss.Client, error) {
	cred, err := parseOSSCredentials([]byte(rawCreds))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	credProvider := credentials.NewStaticCredentialsProvider(cred.AccessID, cred.AccessKey, cred.SecurityToken)
	cfg := oss.LoadDefaultConfig().
		WithCredentialsProvider(credProvider).
		WithEndpoint(cred.Endpoint).
		WithRegion(cred.Region)

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

	return client, nil
}
