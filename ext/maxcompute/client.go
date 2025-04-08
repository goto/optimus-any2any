package maxcompute

import (
	"fmt"

	"github.com/goccy/go-json"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/pkg/errors"
)

// Client maxcompute for reading records
type Client struct {
	*odps.Odps
	QueryReader  func(query string) (RecordReaderCloser, error)
	StreamWriter func(tableID string) (*mcStreamRecordSender, error)
	BatchWriter  func(tableID string) (*mcBatchRecordSender, error)
}

type maxComputeCredentials struct {
	AccessID    string `json:"access_id"`
	AccessKey   string `json:"access_key"`
	Endpoint    string `json:"endpoint"`
	ProjectName string `json:"project_name"`
}

func collectMaxComputeCredential(jsonData []byte) (*maxComputeCredentials, error) {
	var creds maxComputeCredentials
	if err := json.Unmarshal(jsonData, &creds); err != nil {
		return nil, errors.WithStack(err)
	}

	return &creds, nil
}

// NewClient creates a new MaxCompute client
func NewClient(rawCreds string) (*Client, error) {
	cred, err := collectMaxComputeCredential([]byte(rawCreds))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cfg := odps.NewConfig()
	cfg.AccessId = cred.AccessID
	cfg.AccessKey = cred.AccessKey
	cfg.Endpoint = cred.Endpoint
	cfg.ProjectName = cred.ProjectName

	client := &Client{
		Odps: cfg.GenOdps(),
		QueryReader: func(query string) (RecordReaderCloser, error) {
			return nil, fmt.Errorf("query reader needs to be initialized")
		},
		StreamWriter: func(tableID string) (*mcStreamRecordSender, error) {
			return nil, fmt.Errorf("stream writer needs to be initialized")
		},
		BatchWriter: func(tableID string) (*mcBatchRecordSender, error) {
			return nil, fmt.Errorf("batch writer needs to be initialized")
		},
	}

	return client, nil
}
