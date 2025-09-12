package salesforce

import (
	"fmt"
	"os"
	"strconv"
	"strings"

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
	// fetch initial records
	sf.Logger().Info(fmt.Sprintf("fetching records from:\n%s", sf.soqlQuery))
	result, err := sf.client.Query(sf.includeDeleted, sf.soqlQuery)
	if err != nil {
		sf.Logger().Error(fmt.Sprintf("failed to query salesforce: %s", err.Error()))
		return errors.WithStack(err)
	}

	// prepare to handle pagination
	totalRecords := result.TotalSize

	var splittedURL []string
	var urlTemplate string
	batchSize := totalRecords // no pagination, use total records as batch size

	if result.NextRecordsURL != "" && totalRecords > 0 {
		sf.Logger().Info(fmt.Sprintf("pagination detected, next records URL: %s", result.NextRecordsURL))
		splittedURL = strings.Split(result.NextRecordsURL, "-")
		batchSize, err = strconv.Atoi(splittedURL[len(splittedURL)-1])
		if err != nil {
			sf.Logger().Error(fmt.Sprintf("failed to parse batch size from next records URL: %s", err.Error()))
			return errors.WithStack(err)
		}
		urlTemplate = strings.Join(splittedURL[:len(splittedURL)-1], "-") + "-%d"
		sf.Logger().Info(fmt.Sprintf("total records: %d, batch size: %d, url template: %s", totalRecords, batchSize, urlTemplate))
	}

	// fetch records in batches
	for i := 0; i < result.TotalSize; i += batchSize {
		url := sf.soqlQuery // use the initial SOQL query for the first batch
		if i > 0 {
			url = fmt.Sprintf(urlTemplate, i)
		}

		// fetch records from the next records URL concurrently
		err := sf.ConcurrentQueue(func() error {
			var result *simpleforce.QueryResult
			err := sf.DryRunable(func() error {
				r, err := sf.client.Query(sf.includeDeleted, url)
				if err != nil {
					sf.Logger().Error(fmt.Sprintf("failed to query salesforce: %s", err.Error()))
					return errors.WithStack(err)
				}
				result = r
				if url != sf.soqlQuery {
					sf.Logger().Info(fmt.Sprintf("fetched %d records from %s", len(result.Records), url))
				}
				return nil
			}, func() error {
				// In dry-run mode, simulate an empty result
				r := &simpleforce.QueryResult{
					Done:    true,
					Records: []simpleforce.SObject{},
				}
				result = r
				return nil
			})
			if err != nil {
				sf.Logger().Error(fmt.Sprintf("failed to query salesforce: %s", err.Error()))
				return errors.WithStack(err)
			}

			// send records to the channel
			for _, v := range result.Records {
				value := v
				record := model.NewRecordFromMap(map[string]interface{}(value))
				if err := sf.SendRecord(record); err != nil {
					sf.Logger().Error(fmt.Sprintf("failed to send record: %s", err.Error()))
					return errors.WithStack(err)
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return errors.WithStack(sf.ConcurrentQueueWait()) // wait for all queued functions to finish
}
