package clientcredentials

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// BCAProvider is a clientcredentials.Config specific for BCA OAuth2 client credentials flow.
type BCAProvider struct {
	clientcredentials.Config
	expiresAt   time.Time // Time when the token expires
	accessToken string    // Cached access token
}

// Ensure BCAConfig implements the oauth2.TokenSource interface.
var _ oauth2.TokenSource = (*BCAProvider)(nil)

// BCATokenResponse represents the response structure for BCA OAuth2 token endpoint.
type BCATokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   string `json:"expires_in"`
	Scope       string `json:"scope,omitempty"`
}

// NewBCAProvider creates a new Config for satisfy BCA OAuth2 client credentials flow.
func NewBCAProvider(clientID, clientSecret, tokenURL string) *BCAProvider {
	return &BCAProvider{
		Config: clientcredentials.Config{
			ClientID:     clientID,
			ClientSecret: clientSecret,
			TokenURL:     tokenURL,
		},
		expiresAt:   time.Time{}, // Initialize to zero time
		accessToken: "",          // Initialize to empty string
	}
}

// Client returns an HTTP client using the provided token.
// The token will auto-refresh as necessary.
func (c *BCAProvider) Client(ctx context.Context) *http.Client {
	return oauth2.NewClient(ctx, c)
}

// Token retrieves an OAuth2 token using the client credentials flow.
func (c *BCAProvider) Token() (*oauth2.Token, error) {
	if c.accessToken != "" && time.Now().Before(c.expiresAt) {
		// Return cached token if it is still valid
		return &oauth2.Token{
			AccessToken: c.accessToken,
			TokenType:   "Bearer",
			Expiry:      c.expiresAt,
		}, nil
	}

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
		return nil, fmt.Errorf("failed to get token: %s, response: %s", resp.Status, body)
	}

	var token BCATokenResponse
	if err := json.Unmarshal(body, &token); err != nil {
		return nil, fmt.Errorf("failed to decode token response: %w", err)
	}

	c.accessToken = token.AccessToken
	expiresIn, err := time.ParseDuration(token.ExpiresIn + "s")
	if err != nil {
		return nil, fmt.Errorf("failed to parse expires_in: %w", err)
	}
	c.expiresAt = time.Now().Add(expiresIn)

	return &oauth2.Token{
		AccessToken: c.accessToken,
		TokenType:   "Bearer",
		Expiry:      c.expiresAt,
	}, nil
}
