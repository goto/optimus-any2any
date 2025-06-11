package maxcompute

import (
	errs "errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/template"

	"maps"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// MaxcomputeSource is the source component for MaxCompute.
type MaxcomputeSource struct {
	common.Source

	Client         *Client
	PreQuery       string
	QueryTemplates map[string]*template.Template

	filenameColumn string
	closers        []io.Closer
}

var _ flow.Source = (*MaxcomputeSource)(nil)

// NewSource creates a new MaxcomputeSource.
func NewSource(commonSource common.Source, creds string, queryFilePath string, prequeryFilePath string, filenameColumn string, executionProject string, additionalHints map[string]string, logViewRetentionInDays int, batchSize int) (*MaxcomputeSource, error) {
	// create client for maxcompute
	client, err := NewClient(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if executionProject != "" {
		client.SetDefaultProjectName(executionProject)
	}

	// read pre-query from file
	var rawPreQuery []byte
	if prequeryFilePath != "" {
		rawPreQuery, err = os.ReadFile(prequeryFilePath)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	// read query from file / folder
	rawQueries, err := getRawQueries(queryFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	queryTemplates := make(map[string]*template.Template, len(rawQueries))
	for filename, rawQuery := range rawQueries {
		queryTemplate, err := compiler.NewTemplate(fmt.Sprintf("source_mc_query_%s", filename), string(rawQuery))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		queryTemplates[filename] = queryTemplate
	}

	// create tunnel
	var t *tunnel.Tunnel
	if err := commonSource.Retry(func() (err error) {
		t, err = tunnel.NewTunnelFromProject(client.DefaultProject())
		return
	}); err != nil {
		return nil, errors.WithStack(err)
	}

	// add additional hints
	hints := make(map[string]string)
	maps.Copy(hints, additionalHints)

	// query reader
	readerId := 0
	client.QueryReader = func(query string) (RecordReaderCloser, error) {
		readerIdName := fmt.Sprintf("reader-%d", readerId)
		if readerId == 0 {
			readerIdName = "prereader"
		}
		mcRecordReader := &mcRecordReader{
			l:                      commonSource.Logger(),
			client:                 client.Odps,
			readerId:               readerIdName,
			tunnel:                 t,
			query:                  query,
			additionalHints:        hints,
			logViewRetentionInDays: logViewRetentionInDays,
			instance:               nil,
			retryFunc:              commonSource.Retry,
			batchSize:              batchSize,
		}
		readerId++
		return mcRecordReader, nil
	}

	mc := &MaxcomputeSource{
		Source:         commonSource,
		Client:         client,
		QueryTemplates: queryTemplates,
		PreQuery:       string(rawPreQuery),
		filenameColumn: filenameColumn,
		closers:        []io.Closer{},
	}

	// add clean function
	commonSource.AddCleanFunc(func() error {
		mc.Logger().Debug(fmt.Sprintf("cleaning up"))
		var e error
		for _, closer := range mc.closers {
			if err := mc.DryRunable(closer.Close); err != nil {
				mc.Logger().Error(fmt.Sprintf("failed to close closer: %s", err.Error()))
				e = errs.Join(e, errors.WithStack(err))
			}
		}
		return e
	})

	commonSource.RegisterProcess(mc.Process)

	return mc, nil
}

// process is the process function for MaxcomputeSource.
func (mc *MaxcomputeSource) Process() error {
	// create pre-record reader
	var preRecordReader RecordReaderCloser
	var err error

	err = mc.DryRunable(func() error {
		preRecordReader, err = mc.Client.QueryReader(mc.PreQuery)
		if err != nil {
			mc.Logger().Error(fmt.Sprintf("failed to get pre-record reader"))
			return errors.WithStack(err)
		}
		mc.closers = append(mc.closers, preRecordReader)
		return nil
	}, func() error {
		// use empty query for dry run
		if mc.PreQuery != "" {
			mc.Logger().Info(fmt.Sprintf("dry run will not run the query, generated query:\n%s", mc.PreQuery))
		}
		preRecordReader, _ = mc.Client.QueryReader("")
		return nil
	}, func() error {
		// explain pre-query
		return errors.WithStack(mc.executeQueryExplain(mc.PreQuery))
	})
	if err != nil {
		return errors.WithStack(err)
	}

	// record reader tasks
	recordReaderTasks := []func() error{}
	for preRecord, err := range preRecordReader.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}

		// add prefix for every key
		preRecordWithPrefix := mc.RecordWithMetadata(preRecord)
		mc.Logger().Debug(fmt.Sprintf("pre-record: %v", preRecordWithPrefix))

		// iterate over all available queries
		for filename, queryTemplate := range mc.QueryTemplates {
			// compile query
			query, err := compiler.Compile(queryTemplate, model.ToMap(preRecordWithPrefix))
			if err != nil {
				mc.Logger().Error(fmt.Sprintf("failed to compile query"))
				return errors.WithStack(err)
			}

			// create record reader
			var recordReader RecordReaderCloser
			err = mc.DryRunable(func() error {
				recordReader, err = mc.Client.QueryReader(query)
				if err != nil {
					mc.Logger().Error(fmt.Sprintf("failed to get record reader"))
					return errors.WithStack(err)
				}
				mc.closers = append(mc.closers, recordReader)
				return nil
			}, func() error {
				// use empty query for dry run
				if query != "" {
					mc.Logger().Info(fmt.Sprintf("dry run will not run the query, generated query:\n%s", query))
				}
				recordReader, _ = mc.Client.QueryReader("")
				return nil
			}, func() error {
				// explain query
				return errors.WithStack(mc.executeQueryExplain(query))
			})
			if err != nil {
				return errors.WithStack(err)
			}

			preRecordWithPrefixCopy := preRecordWithPrefix.Copy()
			filenameCopy := filename
			recordReaderTasks = append(recordReaderTasks, func() error {
				for record, err := range recordReader.ReadRecord() {
					if err != nil {
						return err
					}

					// merge with pre-record
					for k := range preRecordWithPrefixCopy.AllFromFront() {
						if _, ok := record.Get(k); !ok {
							record.Set(k, preRecordWithPrefixCopy.GetOrDefault(k, nil))
						}
					}
					// add filename column
					record.Set(mc.filenameColumn, filenameCopy)

					if err := mc.SendRecord(record); err != nil {
						mc.Logger().Error(fmt.Sprintf("failed to send record: %s", err.Error()))
						return err
					}
				}
				return nil
			})
		}
	}
	return mc.ConcurrentTasks(recordReaderTasks)
}

func (mc *MaxcomputeSource) executeQueryExplain(query string) error {
	if query == "" {
		return nil
	}
	// get query explain
	queryExplain := getQueryExplain(query)
	mc.Logger().Debug(fmt.Sprintf("query explain:\n%s", queryExplain))

	hints := map[string]string{}
	if strings.Contains(queryExplain, ";") {
		hints["odps.sql.submit.mode"] = "script"
	}
	ins, err := mc.Client.ExecSQl(queryExplain, hints)
	if err != nil {
		return errors.WithStack(err)
	}
	// wait for success
	if err := ins.WaitForSuccess(); err != nil {
		return errors.WithStack(err)
	}
	mc.Logger().Info("query explain verified")
	return nil
}

func getQueryExplain(query string) string {
	// separate headers, variables and udfs from the query
	hr, query := SeparateHeadersAndQuery(query)
	varsAndUDFs, query := SeparateVariablesUDFsAndQuery(query)
	drops, query := SeparateDropsAndQuery(query)

	// construct final query with headers, drops, variables and udfs
	if hr != "" {
		hr += "\n"
	}
	if drops != "" {
		drops += "\n"
	}
	if varsAndUDFs != "" {
		varsAndUDFs += "\n"
	}
	// construct query explain
	return fmt.Sprintf("%s%s\nEXPLAIN\n%s\n;", hr, varsAndUDFs, query)
}

func getRawQueries(queryFilePath string) (map[string][]byte, error) {
	stat, err := os.Stat(queryFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if !stat.IsDir() {
		rawQuery, err := os.ReadFile(queryFilePath)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return map[string][]byte{
			filepath.Base(stat.Name()): rawQuery,
		}, nil
	}
	// read all files in the directory
	ff, err := file.ReadFiles(queryFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// read all files in the directory
	rawQueries := make(map[string][]byte, len(ff))
	for _, f := range ff {
		defer f.Close()
		rawQuery, err := io.ReadAll(f)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		rawQueries[filepath.Base(f.Name())] = rawQuery
	}
	return rawQueries, nil
}
