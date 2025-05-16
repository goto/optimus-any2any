package salesforce

import (
	"fmt"
	"os"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/simpleforce/simpleforce"
)

// SalesforceSource is a source that reads data from Salesforce.
type SalesforceSource struct {
	common.Source
	client         *Client
	soqlQuery      string
	includeDeleted bool
}

var _ flow.Source = (*SalesforceSource)(nil)

// NewSource creates a new SalesforceSource
// sfURL, sfUser, sfPassword, sfToken are the Salesforce credentials
// soqlFilePath is the path to the SOQL query file
// columnMapFilePath is the path to the column map file
func NewSource(commonSource common.Source,
	sfURL, sfUser, sfPassword, sfToken string,
	sfAPIVersion string, includeDeleted bool,
	soqlFilePath string, opts ...common.Option) (*SalesforceSource, error) {

	// create salesforce client
	client, err := NewClient(sfURL, sfUser, sfPassword, sfToken, sfAPIVersion)
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
		Source:         commonSource,
		client:         client,
		soqlQuery:      string(soqlQueryRaw),
		includeDeleted: includeDeleted,
	}

	// add clean func
	commonSource.AddCleanFunc(func() error {
		sf.Logger().Debug(fmt.Sprintf("close salesforce client"))
		return nil
	})
	commonSource.RegisterProcess(sf.process)

	return sf, nil
}

// process reads data from Salesforce and sends it to the channel.
func (sf *SalesforceSource) process() error {
	// initiate record result
	result := &simpleforce.QueryResult{
		Done:           false,
		NextRecordsURL: sf.soqlQuery, // next records url can be soql query or url
	}
	sf.Logger().Info(fmt.Sprintf("fetching records from:\n%s", sf.soqlQuery))

	// fetch records until done
	for !result.Done {
		sf.Logger().Debug(fmt.Sprintf("fetching more records from: %s", result.NextRecordsURL))

		// query salesforce
		err := sf.DryRunable(func() error {
			currentResult, err := sf.client.Query(sf.includeDeleted, result.NextRecordsURL)
			if err != nil {
				sf.Logger().Error(fmt.Sprintf("failed to query more salesforce: %s", err.Error()))
				return errors.WithStack(err)
			}
			result = currentResult
			sf.Logger().Info(fmt.Sprintf("fetched %d records", len(result.Records)))
			return nil
		}, func() error {
			// if dry run, set the result.Done to true
			result.Done = true
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}

		// send records to the channel
		for _, v := range result.Records {
			record := model.NewRecordFromMap(map[string]interface{}(v))
			if err := sf.SendRecord(record); err != nil {
				sf.Logger().Error(fmt.Sprintf("failed to send record: %s", err.Error()))
				return errors.WithStack(err)
			}
		}
	}
	return nil
}
