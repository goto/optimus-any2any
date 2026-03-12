package clientcredentials

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// ProviderA is a clientcredentials.Config specific for OAuth2 client credentials flow for Provider A.
type ProviderA struct {
	clientcredentials.Config
	l           *slog.Logger
	token       *oauth2.Token      // Cached token for reuse
	tokenSource oauth2.TokenSource // Token source for OAuth2
}

// Ensure ProviderA implements the oauth2.TokenSource interface.
var _ oauth2.TokenSource = (*ProviderA)(nil)

// ProviderAResponse is the expected response structure from Provider A's token endpoint.
type ProviderATokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   string `json:"expires_in"`
	Scope       string `json:"scope,omitempty"`
}

func NewProviderA(l *slog.Logger, clientID, clientSecret, tokenURL string) *ProviderA {
	c := &ProviderA{
		Config: clientcredentials.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			TokenURL:     tokenURL,
		},
		l:     l,
		token: nil,
	}
	c.tokenSource = oauth2.ReuseTokenSource(c.token, c)
	return c
}

// Token retrieves an OAuth2 token using the client credentials flow.
func (c *ProviderA) Client(ctx context.Context) *http.Client {
	return oauth2.NewClient(ctx, c.tokenSource)
}

// Token retrieves an OAuth2 token using the client credentials flow.
func (c *ProviderA) Token() (*oauth2.Token, error) {
	c.l.Info("requesting OAuth2 token using client credentials flow")

	auth := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("%s:%s", c.ClientID, c.ClientSecret)))
	values := url.Values{}
	values.Set("grant_type", "client_credentials")
	tokenURL := c.TokenURL + "?" + values.Encode()

	req, err := http.NewRequest("POST", tokenURL, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// set content type to json and authorization header
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Basic "+auth)

	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, errors.WithStack(fmt.Errorf("failed to get token: %s, response: %s", resp.Status, body))
	}

	var token ProviderATokenResponse
	if err := json.Unmarshal(body, &token); err != nil {
		return nil, errors.WithStack(fmt.Errorf("failed to decode token response: %w", err))
	}

	expiresIn, err := time.ParseDuration(token.ExpiresIn + "s")
	if err != nil {
		return nil, errors.WithStack(fmt.Errorf("failed to parse expires_in: %w", err))
	}

	return &oauth2.Token{
		AccessToken: token.AccessToken,
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(expiresIn),
		ExpiresIn:   int64(expiresIn.Seconds()),
	}, nil
}
