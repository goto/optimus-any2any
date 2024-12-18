package salesforce

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/source"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"github.com/simpleforce/simpleforce"
)

// SalesforceSource is a source that reads data from Salesforce.
type SalesforceSource struct {
	*source.CommonSource
	client    *simpleforce.Client
	soqlQuery string
	columnMap map[string]string
}

var _ flow.Source = (*SalesforceSource)(nil)

// NewSource creates a new SalesforceSource
// sfURL, sfUser, sfPassword, sfToken are the Salesforce credentials
// soqlFilePath is the path to the SOQL query file
// columnMapFilePath is the path to the column map file
func NewSource(l *slog.Logger,
	sfURL, sfUser, sfPassword, sfToken string,
	soqlFilePath, columnMappingFilePath string, opts ...option.Option) (*SalesforceSource, error) {
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
	// read column map
	columnMap, err := getColumnMap(columnMappingFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// create source
	sf := &SalesforceSource{
		CommonSource: commonSource,
		client:       client,
		soqlQuery:    string(soqlQueryRaw),
		columnMap:    columnMap,
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
		sf.Logger.Info(fmt.Sprintf("source: fetched %d records", len(currentResult.Records)))
		for _, v := range currentResult.Records {
			record := map[string]interface{}(v)
			mappedRecord := sf.mapping(record)
			raw, err := json.Marshal(mappedRecord)
			if err != nil {
				sf.Logger.Error(fmt.Sprintf("source: failed to marshal record: %s", err.Error()))
				continue
			}
			sf.Send(raw)
		}
		result = currentResult
	}
}

// mapping maps the column name from Salesforce to the column name in the column map.
func (sf *SalesforceSource) mapping(value map[string]interface{}) map[string]interface{} {
	sf.Logger.Debug(fmt.Sprintf("source: record before map: %v", value))
	mappedValue := make(map[string]interface{})
	for key, val := range value {
		if mappedKey, ok := sf.columnMap[key]; ok {
			mappedValue[mappedKey] = val
		} else {
			mappedValue[key] = val
		}
	}
	sf.Logger.Debug(fmt.Sprintf("source: record after map: %v", mappedValue))
	return mappedValue
}

// getColumnMap reads the column map from the file.
func getColumnMap(columnMapFilePath string) (map[string]string, error) {
	columnMapRaw, err := os.ReadFile(columnMapFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	columnMap := make(map[string]string)
	if err = json.Unmarshal(columnMapRaw, &columnMap); err != nil {
		return nil, errors.WithStack(err)
	}
	return columnMap, nil
}
