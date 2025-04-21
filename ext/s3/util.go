package s3

import (
	"github.com/goccy/go-json"
	"github.com/pkg/errors"
)

type AWSCredentials struct {
	AWSAccessKeyID     string `json:"aws_access_key_id"`
	AWSSecretAccessKey string `json:"aws_secret_access_key"`
	AWSSessionToken    string `json:"aws_session_token,omitempty"` // optional
}

func parseCredentials(creds string) (*AWSCredentials, error) {
	parsedCreds := &AWSCredentials{}
	if err := json.Unmarshal([]byte(creds), parsedCreds); err != nil {
		return nil, errors.WithStack(err)
	}
	if parsedCreds.AWSAccessKeyID == "" || parsedCreds.AWSSecretAccessKey == "" {
		err := errors.New("missing AWS access key ID or secret access key")
		return nil, errors.WithStack(err)
	}
	return parsedCreds, nil
}
