package salesforce

import (
	"fmt"

	"github.com/simpleforce/simpleforce"
)

// createClient creates a new Salesforce client.
func createClient(sfURL, sfUser, sfPassword, sfToken string) (*simpleforce.Client, error) {
	// for now we use default api version
	// TODO: parameterize api version if needed
	client := simpleforce.NewClient(sfURL, simpleforce.DefaultClientID, simpleforce.DefaultAPIVersion)
	if client == nil {
		return nil, fmt.Errorf("source: failed to create a new Salesforce client")
	}

	err := client.LoginPassword(sfUser, sfPassword, sfToken)
	if err != nil {
		return nil, fmt.Errorf("source: failed to login with the provided credentials")
	}

	return client, nil
}
