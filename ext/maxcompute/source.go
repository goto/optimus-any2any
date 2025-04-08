package maxcompute

import (
	"fmt"
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

	Client        *Client
	PreQuery      string
	QueryTemplate *template.Template
}

var _ flow.Source = (*MaxcomputeSource)(nil)

// NewSource creates a new MaxcomputeSource.
func NewSource(commonSource *common.CommonSource, creds string, queryFilePath string, prequeryFilePath string, executionProject string, additionalHints map[string]string, logViewRetentionInDays int) (*MaxcomputeSource, error) {
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
	t, err := tunnel.NewTunnelFromProject(client.DefaultProject())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// add additional hints
	hints := make(map[string]string)
	maps.Copy(hints, additionalHints)

	// query reader
	client.QueryReader = func(query string) (common.RecordReader, error) {
		mcRecordReader := &mcRecordReader{
			l:                      commonSource.Logger(),
			client:                 client.Odps,
			tunnel:                 t,
			query:                  query,
			additionalHints:        hints,
			logViewRetentionInDays: logViewRetentionInDays,
		}
		return mcRecordReader, nil
	}

	mc := &MaxcomputeSource{
		Source:        commonSource,
		Getter:        commonSource,
		RecordSender:  commonSource,
		RecordHelper:  commonSource,
		Client:        client,
		QueryTemplate: queryTemplate,
		PreQuery:      string(rawPreQuery),
	}

	// add clean function
	commonSource.AddCleanFunc(func() error {
		mc.Logger().Debug(fmt.Sprintf("cleaning up"))
		return nil
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

		for record, err := range recordReader.ReadRecord() {
			if err != nil {
				return errors.WithStack(err)
			}

			// merge with pre-record
			for k := range preRecordWithPrefix.AllFromFront() {
				if _, ok := record.Get(k); !ok {
					record.Set(k, preRecordWithPrefix.GetOrDefault(k, nil))
				}
			}

			if err := mc.SendRecord(record); err != nil {
				mc.Logger().Error(fmt.Sprintf("failed to send record: %s", err.Error()))
				return errors.WithStack(err)
			}
		}
	}
	return nil
}
