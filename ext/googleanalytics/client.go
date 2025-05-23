package googleanalytics

import (
	"context"
	"crypto/tls"

	"github.com/goto/optimus-any2any/internal/auth"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	analyticsdata "google.golang.org/genproto/googleapis/analytics/data/v1beta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/oauth"
)

const (
	defaultHost = "analyticsdata.googleapis.com:443"
)

// GAClient is a client for Google Analytics Data API v1beta.
type GAClient struct {
	analyticsdata.BetaAnalyticsDataClient
	conn *grpc.ClientConn
}

// NewClient creates a new GAClient.
func NewClient(ctx context.Context, svcAcc string, tlsCert, tlsCACert, tlsKey string, projectID string) (*GAClient, error) {
	// get transport credentials
	c, err := google.CredentialsFromJSON(context.Background(), []byte(svcAcc),
		"https://www.googleapis.com/auth/cloud-platform",
		"https://www.googleapis.com/auth/analytics.readonly",
	)
	if err != nil {
		return nil, err
	}

	// create a new TLS config
	tlsConfig := &tls.Config{}
	if tlsCert != "" && tlsKey != "" && tlsCACert != "" {
		tlsConfig, err = auth.NewTLSConfig(tlsCert, tlsKey, tlsCACert)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}

	// create a new gRPC connection
	conn, err := grpc.NewClient(defaultHost,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithPerRPCCredentials(oauth.TokenSource{TokenSource: c.TokenSource}),
	)
	if err != nil {
		return nil, err
	}

	client := analyticsdata.NewBetaAnalyticsDataClient(conn)
	return &GAClient{
		BetaAnalyticsDataClient: client,
		conn:                    conn,
	}, nil
}

// Close closes the gRPC connection.
func (c *GAClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}
