package salesforce

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/simpleforce/simpleforce"
)

// SalesforceSource is a source that reads data from Salesforce.
type SalesforceSource struct {
	*common.Source
	client    *simpleforce.Client
	soqlQuery string
}

var _ flow.Source = (*SalesforceSource)(nil)

// NewSource creates a new SalesforceSource
// sfURL, sfUser, sfPassword, sfToken are the Salesforce credentials
// soqlFilePath is the path to the SOQL query file
// columnMapFilePath is the path to the column map file
func NewSource(l *slog.Logger,
	sfURL, sfUser, sfPassword, sfToken string,
	soqlFilePath string, opts ...common.Option) (*SalesforceSource, error) {
	// create commonSource
	commonSource := common.NewSource(l, opts...)
	// create salesforce client
	client, err := createClient(sfURL, sfUser, sfPassword, sfToken)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// read soql query
	soqlQueryRaw, err := os.ReadFile(soqlFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// create source
	sf := &SalesforceSource{
		Source:    commonSource,
		client:    client,
		soqlQuery: string(soqlQueryRaw),
		// columnMap: columnMap,
	}

	// add clean func
	commonSource.AddCleanFunc(func() {
		commonSource.Logger.Debug("source(sf): close salesforce client")
	})
	commonSource.RegisterProcess(sf.process)

	return sf, nil
}

// process reads data from Salesforce and sends it to the channel.
func (sf *SalesforceSource) process() {
	// initiate record result
	result := &simpleforce.QueryResult{
		Done:           false,
		NextRecordsURL: sf.soqlQuery, // next records url can be soql query or url
	}
	sf.Logger.Info(fmt.Sprintf("source(sf): fetching records from:\n%s", sf.soqlQuery))
	// fetch records until done
	for !result.Done {
		sf.Logger.Debug(fmt.Sprintf("source(sf): fetching more records from: %s", result.NextRecordsURL))
		currentResult, err := sf.client.Query(result.NextRecordsURL)
		if err != nil {
			sf.Logger.Error(fmt.Sprintf("source(sf): failed to query more salesforce: %s", err.Error()))
			sf.SetError(errors.WithStack(err))
			return
		}
		sf.Logger.Info(fmt.Sprintf("source(sf): fetched %d records", len(currentResult.Records)))
		for _, v := range currentResult.Records {
			record := map[string]interface{}(v)
			raw, err := json.Marshal(record)
			if err != nil {
				sf.Logger.Error(fmt.Sprintf("source(sf): failed to marshal record: %s", err.Error()))
				sf.SetError(errors.WithStack(err))
				continue
			}
			sf.Send(raw)
		}
		result = currentResult
	}
}
