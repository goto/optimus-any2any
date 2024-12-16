package salesforce

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/source"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/simpleforce/simpleforce"
)

type SalesforceSource struct {
	*source.CommonSource
	client    *simpleforce.Client
	soqlQuery string
}

var _ flow.Source = (*SalesforceSource)(nil)

func NewSource(l *slog.Logger,
	sfURL, sfUser, sfPassword, sfToken string,
	soqlFilePath string, opts ...option.Option) (*SalesforceSource, error) {
	// create commonSource
	commonSource := source.NewCommonSource(l, opts...)
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
		CommonSource: commonSource,
		client:       client,
		soqlQuery:    string(soqlQueryRaw),
	}

	// add clean func
	commonSource.AddCleanFunc(func() {
		commonSource.Logger.Debug("source: close salesforce client")
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
	// fetch records until done
	for !result.Done {
		currentResult, err := sf.client.Query(result.NextRecordsURL)
		if err != nil {
			sf.Logger.Error(fmt.Sprintf("source: failed to query more salesforce: %s", err.Error()))
			return
		}
		result = currentResult
		for _, record := range result.Records {
			sf.Send(record)
		}
	}
}
