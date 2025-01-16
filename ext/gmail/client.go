package gmail

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
)

// TokenJSON is a struct that represents the JSON data of a token.
type TokenJSON struct {
	Token          string    `json:"token"`
	RefreshToken   string    `json:"refresh_token"`
	TokenURI       string    `json:"token_uri"`
	ClientID       string    `json:"client_id"`
	ClientSecret   string    `json:"client_secret"`
	Scopes         []string  `json:"scopes"`
	Expiry         time.Time `json:"expiry"`
	Account        string    `json:"account"`
	UniverseDomain string    `json:"universe_domain"`
}

func NewServiceFromToken(ctx context.Context, tokenJSON []byte) (*gmail.Service, error) {
	// Create an HTTP client
	client, err := NewHTTPClient(ctx, tokenJSON)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Create a Gmail service
	srv, err := gmail.NewService(ctx, option.WithHTTPClient(client))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return srv, nil
}

func NewHTTPClient(ctx context.Context, tokenJSON []byte) (*http.Client, error) {
	// generate token source
	tokenSource, err := generateTokenSource(ctx, tokenJSON)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Create an HTTP client
	return oauth2.NewClient(ctx, tokenSource), nil
}

func generateTokenSource(ctx context.Context, tokenJSON []byte) (oauth2.TokenSource, error) {
	// Decode the JSON
	var tokenData TokenJSON
	if err := json.Unmarshal([]byte(tokenJSON), &tokenData); err != nil {
		return nil, errors.WithStack(err)
	}

	// Create an oauth2.Token from the JSON data
	token := &oauth2.Token{
		AccessToken:  tokenData.Token,
		RefreshToken: tokenData.RefreshToken,
		Expiry:       tokenData.Expiry,
		TokenType:    "Bearer", // Default to Bearer
	}

	// Create an oauth2.Config from the JSON data
	config := &oauth2.Config{
		ClientID:     tokenData.ClientID,
		ClientSecret: tokenData.ClientSecret,
		Endpoint: oauth2.Endpoint{
			TokenURL: tokenData.TokenURI,
		},
		Scopes: tokenData.Scopes,
	}

	// Generate a token source
	return config.TokenSource(ctx, token), nil
}
