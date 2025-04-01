package maxcompute

import (
	"github.com/goccy/go-json"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/pkg/errors"
)

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
func NewClient(rawCreds string) (*odps.Odps, error) {
	cred, err := collectMaxComputeCredential([]byte(rawCreds))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	cfg := odps.NewConfig()
	cfg.AccessId = cred.AccessID
	cfg.AccessKey = cred.AccessKey
	cfg.Endpoint = cred.Endpoint
	cfg.ProjectName = cred.ProjectName

	return cfg.GenOdps(), nil
}
