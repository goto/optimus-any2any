package clientcredentials

import (
	"context"
	"net/http"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// BaseProvider is a clientcredentials.Config specific for OAuth2 client credentials flow.
type BaseProvider struct {
	clientcredentials.Config
	tokenType   string    // Type of the token, e.g., "Bearer"
	expiresAt   time.Time // Time when the token expires
	accessToken string    // Cached access token
}

// Client returns an HTTP client using the provided token.
// The token will auto-refresh as necessary.
func (c *BaseProvider) Client(ctx context.Context) *http.Client {
	return oauth2.NewClient(ctx, c)
}

// Token retrieves an OAuth2 token using the client credentials flow.
func (c *BaseProvider) Token() (*oauth2.Token, error) {
	return &oauth2.Token{
		AccessToken: c.accessToken,
		TokenType:   c.tokenType,
		Expiry:      c.expiresAt,
	}, nil
}
