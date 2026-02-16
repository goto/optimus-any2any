package aws

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/goccy/go-json"
	"github.com/pkg/errors"
)

// TikTokConfig stores the client key and secret for TikTok API
type TikTokConfig struct {
	ClientKey    string
	ClientSecret string
}

// TikTokProvider is a custom AWS credentials provider that uses the TikTok
// API to obtain temporary credentials. It implements the aws.CredentialsProvider
// interface and can be used with the AWS SDK for Go v2.
type TikTokProvider struct {
	HTTPClient *http.Client
	Config     TikTokConfig
	Resource   ResourceType
}

var _ aws.CredentialsProvider = (*TikTokProvider)(nil)

// NewTikTokProvider creates a new TikTokProvider with the given client key,
// client secret, and resource type. The resource type determines the type of
// credentials to be retrieved. The provider uses the default HTTP client
// for making requests to the TikTok API.
func NewTikTokProvider(key, secret string, resourceType ResourceType) *TikTokProvider {
	return &TikTokProvider{
		HTTPClient: http.DefaultClient,
		Config: TikTokConfig{
			ClientKey:    key,
			ClientSecret: secret,
		},
		Resource: resourceType,
	}
}

func (p *TikTokProvider) Retrieve(ctx context.Context) (aws.Credentials, error) {
	token, err := p.getClientToken()
	if err != nil {
		return aws.Credentials{}, errors.WithStack(err)
	}

	switch p.Resource {
	case NoneResourceType:
		return aws.Credentials{
			AccessKeyID:     p.Config.ClientKey,
			SecretAccessKey: p.Config.ClientSecret,
			SessionToken:    token,
			Source:          "tiktok",
		}, nil
	case S3ResourceType:
		time.Sleep(time.Duration(rand.Intn(5)+5) * time.Second) // wait for 5-10 seconds between requests
		return p.getTemporaryUploadCredentials(token)
	default:
		return aws.Credentials{}, errors.WithStack(fmt.Errorf("unsupported resource type: %s", p.Resource))
	}
}

func (p *TikTokProvider) getClientToken() (string, error) {
	formData := url.Values{}
	formData.Set("client_key", p.Config.ClientKey)
	formData.Set("client_secret", p.Config.ClientSecret)
	formData.Set("grant_type", "client_credentials")

	req, err := http.NewRequest(http.MethodPost,
		"https://open.tiktokapis.com/v2/oauth/token/",
		strings.NewReader(formData.Encode()))
	if err != nil {
		return "", errors.WithStack(err)
	}

	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Cache-Control", "no-cache")

	resp, err := p.HTTPClient.Do(req)
	if err != nil {
		return "", errors.WithStack(err)
	}
	defer resp.Body.Close()

	var responseBody map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&responseBody); err != nil {
		return "", errors.WithStack(err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("%d response when getting client token: %v",
			resp.StatusCode, responseBody)
	}

	accessToken, ok := responseBody["access_token"].(string)
	if !ok {
		return "", errors.New("access_token not found in response")
	}

	tokenType, ok := responseBody["token_type"].(string)
	if !ok {
		return "", errors.New("token_type not found in response")
	}

	return tokenType + " " + accessToken, nil
}

func (p *TikTokProvider) getTemporaryUploadCredentials(token string) (aws.Credentials, error) {
	data := map[string]string{
		"username": p.Config.ClientKey,
		"password": p.Config.ClientSecret,
	}

	raw, err := json.Marshal(data)
	if err != nil {
		return aws.Credentials{}, errors.WithStack(err)
	}

	req, err := http.NewRequest(http.MethodPost,
		"https://open.tiktokapis.com/v2/feed/upload/get_credentials/",
		bytes.NewBuffer(raw))
	if err != nil {
		return aws.Credentials{}, errors.WithStack(err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-tt-target-idc", "useast1a")
	req.Header.Set("Authorization", token)

	resp, err := p.HTTPClient.Do(req)
	if err != nil {
		return aws.Credentials{}, errors.WithStack(err)
	}
	defer resp.Body.Close()

	var responseBody map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&responseBody); err != nil {
		return aws.Credentials{}, errors.WithStack(err)
	}

	if resp.StatusCode != http.StatusOK {
		errBody, _ := responseBody["error"].(map[string]interface{})
		return aws.Credentials{}, errors.Errorf("%d response when getting temporary creds: %v",
			resp.StatusCode, errBody)
	}

	responseBodyData, ok := responseBody["data"].(map[string]interface{})
	if !ok {
		return aws.Credentials{}, errors.New("data not found in response")
	}

	aki, ok := responseBodyData["access_key_id"].(string)
	if !ok {
		return aws.Credentials{}, errors.New("access_key_id not found in response")
	}

	sk, ok := responseBodyData["secret_key"].(string)
	if !ok {
		return aws.Credentials{}, errors.New("secret_key not found in response")
	}

	st, ok := responseBodyData["session_token"].(string)
	if !ok {
		return aws.Credentials{}, errors.New("session_token not found in response")
	}

	return aws.Credentials{
		AccessKeyID:     aki,
		SecretAccessKey: sk,
		SessionToken:    st,
		Source:          "tiktok",
	}, nil
}
