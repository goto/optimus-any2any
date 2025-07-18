package http

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	xclientcredentials "github.com/goto/optimus-any2any/internal/auth/clientcredentials"
	"github.com/pkg/errors"
)

func isUsingOAuth2(clientCredentialsProvider, clientCredentialsClientID, clientCredentialsClientSecret, clientCredentialsTokenURL string) bool {
	return clientCredentialsProvider != "" && clientCredentialsClientID != "" && clientCredentialsClientSecret != "" && clientCredentialsTokenURL != ""
}

func newClientWithOAuth2(ctx context.Context, l *slog.Logger, clientCredentialsProvider, clientCredentialsClientID, clientCredentialsClientSecret, clientCredentialsTokenURL string) (*http.Client, error) {
	switch strings.ToLower(clientCredentialsProvider) {
	case xclientcredentials.CustomProviderA:
		ccProvider := xclientcredentials.NewProviderA(l, clientCredentialsClientID, clientCredentialsClientSecret, clientCredentialsTokenURL)
		return ccProvider.Client(ctx), nil
	default:
		return nil, errors.New(fmt.Sprintf("unsupported client credentials provider: %s", clientCredentialsProvider))
	}
}
