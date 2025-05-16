package salesforce

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/pkg/errors"
	"github.com/simpleforce/simpleforce"
)

type Client struct {
	*simpleforce.Client
	sfBaseURL    string
	sfAPIVersion string
}

// NewClient creates a new Salesforce client.
func NewClient(sfURL, sfUser, sfPassword, sfToken, sfAPIVersion string) (*Client, error) {
	if sfAPIVersion == "" {
		sfAPIVersion = simpleforce.DefaultAPIVersion
	}
	if strings.HasSuffix(sfURL, "/") {
		sfURL = sfURL[:len(sfURL)-1]
	}

	client := simpleforce.NewClient(sfURL, simpleforce.DefaultClientID, sfAPIVersion)
	if client == nil {
		return nil, fmt.Errorf("source: failed to create a new Salesforce client")
	}

	err := client.LoginPassword(sfUser, sfPassword, sfToken)
	if err != nil {
		return nil, fmt.Errorf("source: failed to login with the provided credentials")
	}

	return &Client{
		Client:       client,
		sfBaseURL:    sfURL,
		sfAPIVersion: sfAPIVersion,
	}, nil
}

func (c *Client) Query(includeDeleted bool, soqlQuery string) (*simpleforce.QueryResult, error) {
	formatString := "services/data/v%s/query?q=%s"
	if includeDeleted {
		formatString = "services/data/v%s/queryAll?q=%s"
	}
	u := fmt.Sprintf(formatString, c.sfAPIVersion, url.QueryEscape(soqlQuery))

	if strings.HasPrefix(soqlQuery, "/services/data") {
		u = soqlQuery
	}

	data, err := c.ApexREST(http.MethodGet, u, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Ref: https://github.com/simpleforce/simpleforce/blob/acf4ac67ef68eee62febf8e1afac93b55b0e6512/force.go#L93
	var result simpleforce.QueryResult
	err = json.Unmarshal(data, &result)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	for idx := range result.Records {
		result.Records[idx].Set("__client__", c.Client)
	}

	return &result, nil
}
