package maxcompute

import (
	errs "errors"
	"fmt"
	"io"
	"os"
	"text/template"

	"maps"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// MaxcomputeSource is the source component for MaxCompute.
type MaxcomputeSource struct {
	flow.Source
	component.Getter
	common.RecordSender
	common.RecordHelper
	common.ConcurrentLimiter

	Client        *Client
	PreQuery      string
	QueryTemplate *template.Template

	closers []io.Closer
}

var _ flow.Source = (*MaxcomputeSource)(nil)

// NewSource creates a new MaxcomputeSource.
func NewSource(commonSource common.Source, creds string, queryFilePath string, prequeryFilePath string, executionProject string, additionalHints map[string]string, logViewRetentionInDays int, batchSize int) (*MaxcomputeSource, error) {
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
	// read query from file
	rawQuery, err := os.ReadFile(queryFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	queryTemplate, err := compiler.NewTemplate("source_mc_query", string(rawQuery))
	if err != nil {
		return nil, errors.WithStack(err)
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
		Source:            commonSource,
		Getter:            commonSource,
		RecordSender:      commonSource,
		RecordHelper:      commonSource,
		ConcurrentLimiter: commonSource,
		Client:            client,
		QueryTemplate:     queryTemplate,
		PreQuery:          string(rawPreQuery),
		closers:           []io.Closer{},
	}

	// add clean function
	commonSource.AddCleanFunc(func() error {
		mc.Logger().Debug(fmt.Sprintf("cleaning up"))
		var e error
		for _, closer := range mc.closers {
			if err := closer.Close(); err != nil {
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
	preRecordReader, err := mc.Client.QueryReader(mc.PreQuery)
	if err != nil {
		mc.Logger().Error(fmt.Sprintf("failed to get pre-record reader"))
		return errors.WithStack(err)
	}
	mc.closers = append(mc.closers, preRecordReader)

	// record reader tasks
	recordReaderTasks := []func() error{}
	for preRecord, err := range preRecordReader.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}

		// add prefix for every key
		preRecordWithPrefix := mc.RecordWithMetadata(preRecord)
		mc.Logger().Debug(fmt.Sprintf("pre-record: %v", preRecordWithPrefix))

		// compile query
		query, err := compiler.Compile(mc.QueryTemplate, model.ToMap(preRecordWithPrefix))
		if err != nil {
			mc.Logger().Error(fmt.Sprintf("failed to compile query"))
			return errors.WithStack(err)
		}

		// create record reader
		recordReader, err := mc.Client.QueryReader(query)
		if err != nil {
			mc.Logger().Error(fmt.Sprintf("failed to get record reader"))
			return errors.WithStack(err)
		}
		mc.closers = append(mc.closers, recordReader)

		preRecordWithPrefixCopy := preRecordWithPrefix.Copy()
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

				if err := mc.SendRecord(record); err != nil {
					mc.Logger().Error(fmt.Sprintf("failed to send record: %s", err.Error()))
					return err
				}
			}
			return nil
		})
	}
	return mc.ConcurrentLimiter.ConcurrentTasks(mc.Context(), 4, recordReaderTasks)
}
