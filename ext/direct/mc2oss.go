package direct

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"text/template"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/ext/maxcompute"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/internal/helper"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type mc2oss struct {
	flow.NoFlow
	ctx                    context.Context
	l                      *slog.Logger
	client                 *maxcompute.Client
	preQuery               string
	queryTemplate          *template.Template
	destinationURITemplate *template.Template
	prefixMetadata         string
	logViewRetentionInDays int
	cleanFuncs             []func() error
}

func NewMC2OSS(ctx context.Context, l *slog.Logger, prefixMetadata string, cfgMC *config.SourceMCConfig, cfgOSS *config.SinkOSSConfig) (flow.NoFlow, error) {
	// create client for maxcompute
	client, err := maxcompute.NewClient(cfgMC.Credentials)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// read pre-query from file
	var rawPreQuery []byte
	if cfgMC.PreQueryFilePath != "" {
		rawPreQuery, err = os.ReadFile(cfgMC.PreQueryFilePath)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	// read query from file
	rawQuery, err := os.ReadFile(cfgMC.QueryFilePath)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	queryTemplate, err := compiler.NewTemplate("source_mc_query", string(rawQuery))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// parse destinationURI as template
	destinationURITemplate, err := compiler.NewTemplate("sink_oss_destination_uri", cfgOSS.DestinationURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse destination URI template: %w", err)
	}
	t, err := tunnel.NewTunnelFromProject(client.DefaultProject())
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// query reader
	client.QueryReader = func(query string) (maxcompute.RecordReaderCloser, error) {
		mcRecordReader, err := maxcompute.NewRecordReader(l, client.Odps, t, cfgMC.PreQueryFilePath)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		mcRecordReader.SetReaderId("prereader")
		mcRecordReader.SetLogViewRetentionInDays(cfgMC.LogViewRetentionInDays)
		mcRecordReader.SetBatchSize(cfgMC.BatchSize)
		return mcRecordReader, nil
	}
	mo := &mc2oss{
		ctx:                    ctx,
		l:                      l,
		client:                 client,
		preQuery:               string(rawPreQuery),
		queryTemplate:          queryTemplate,
		destinationURITemplate: destinationURITemplate,
		prefixMetadata:         prefixMetadata,
		logViewRetentionInDays: cfgMC.LogViewRetentionInDays,
	}

	return mo, nil
}

func (mo *mc2oss) Run() []error {
	// create pre-record reader
	preRecordReader, err := mo.client.QueryReader(mo.preQuery)
	if err != nil {
		return []error{errors.WithStack(err)}
	}
	mo.cleanFuncs = append(mo.cleanFuncs, func() error { return preRecordReader.Close() })

	// unload tasks
	unloadTasks := []func() error{}
	for preRecord, err := range preRecordReader.ReadRecord() {
		if err != nil {
			return []error{errors.WithStack(err)}
		}

		// add prefix for every key
		preRecordWithPrefix := common.RecordWithMetadata(preRecord, mo.prefixMetadata)

		// compile query
		query, err := compiler.Compile(mo.queryTemplate, model.ToMap(preRecordWithPrefix))
		if err != nil {
			mo.l.Error(fmt.Sprintf("failed to compile query"))
			return []error{errors.WithStack(err)}
		}

		preRecordWithPrefixCopy := preRecordWithPrefix.Copy()
		runnerId := atomic.Int32{}
		unloadTasks = append(unloadTasks, func() error {
			id := fmt.Sprintf("%d", runnerId.Add(1))
			// generate destination URI
			destinationURI, err := compiler.Compile(mo.destinationURITemplate, model.ToMap(preRecordWithPrefixCopy))
			if err != nil {
				mo.l.Error(fmt.Sprintf("failed to compile destination URI"))
				return errors.WithStack(err)
			}
			// build query unload
			query, err := mo.buildQuery(query, destinationURI)
			if err != nil {
				return errors.WithStack(err)
			}
			hints := map[string]string{}
			if strings.Contains(query, ";") {
				hints["odps.sql.submit.mode"] = "script"
			}
			// run query
			mo.l.Info(fmt.Sprintf("runner(%s): running query:\n%s", id, query))
			var instance *odps.Instance
			if err := mo.retryFunc(func() (err error) {
				instance, err = mo.client.ExecSQlWithHints(query, hints)
				return
			}); err != nil {
				return errors.WithStack(err)
			}
			// generate logview
			var url string
			if err := mo.retryFunc(func() (err error) {
				url, err = odps.NewLogView(mo.client.Odps).GenerateLogView(instance, mo.logViewRetentionInDays*24)
				return
			}); err != nil {
				return errors.WithStack(err)
			}
			mo.l.Info(fmt.Sprintf("runner(%s): log view url: %s", id, url))
			mo.cleanFuncs = append(mo.cleanFuncs, func() error {
				return mo.terminateInstance(id, instance)
			})
			// wait for instance to finish
			mo.l.Info(fmt.Sprintf("runner(%s): waiting for instance to finish...", id))
			if err := mo.retryFunc(instance.WaitForSuccess); err != nil {
				mo.l.Error(fmt.Sprintf("runner(%s): instance failed: logview detail: %s", id, url))
				return errors.WithStack(err)
			}
			mo.l.Info(fmt.Sprintf("runner(%s): instance finished: %s: success uploaded to %s", id, instance.Id(), destinationURI))
			return nil
		})
	}
	// run unload tasks concurrently
	if err := common.ConcurrentTask(mo.ctx, 4, unloadTasks); err != nil {
		mo.l.Error(fmt.Sprintf("failed to run unload tasks: %s", err.Error()))
		return []error{errors.WithStack(err)}
	}
	return []error{}
}

func (mo *mc2oss) Close() error {
	for _, cleanFunc := range mo.cleanFuncs {
		if err := cleanFunc(); err != nil {
			mo.l.Error(fmt.Sprintf("failed to clean up: %s", err.Error()))
			return errors.WithStack(err)
		}
	}
	return nil
}

func (mo *mc2oss) buildQuery(query string, destinationURI string) (string, error) {
	// separate headers, variables and udfs from the query
	hr, query := helper.SeparateHeadersAndQuery(query)
	varsAndUDFs, query := helper.SeparateVariablesUDFsAndQuery(query)
	drops, query := helper.SeparateDropsAndQuery(query)

	// determine storage handler
	var storageHandler string
	switch filepath.Ext(destinationURI) {
	case ".csv":
		storageHandler = "com.aliyun.odps.CsvStorageHandler"
	case ".tsv":
		storageHandler = "com.aliyun.odps.TsvStorageHandler"
	case ".json":
		storageHandler = "org.apache.hive.hcatalog.data.JsonSerDe"
	default:
		mo.l.Warn(fmt.Sprintf("unknown file type: %s, using default json storage handler", filepath.Ext(destinationURI)))
		storageHandler = "org.apache.hive.hcatalog.data.JsonSerDe"
	}

	// construct query
	query = fmt.Sprintf("UNLOAD FROM\n(\n%s\n)\nINTO LOCATION '%s'\nSTORED BY '%s'\n;", query, destinationURI, storageHandler)

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
	query = fmt.Sprintf("%s%s%s%s", hr, drops, varsAndUDFs, query)
	return query, nil
}

func (mo *mc2oss) retryFunc(f func() error) error {
	return common.Retry(mo.l, 3, 1000, f)
}

func (mo *mc2oss) terminateInstance(id string, instance *odps.Instance) error {
	if instance == nil {
		return nil
	}
	if err := mo.retryFunc(instance.Load); err != nil {
		mo.l.Error(fmt.Sprintf("failed to load instance %s: %s", instance.Id(), err.Error()))
		return errors.WithStack(err)
	}
	if instance.Status() == odps.InstanceTerminated { // instance is terminated, no need to terminate again
		mo.l.Info(fmt.Sprintf("runner(%s): success terminating instance %s", id, instance.Id()))
		return nil
	}
	mo.l.Info(fmt.Sprintf("runner(%s): trying to terminate instance %s", id, instance.Id()))
	if err := mo.retryFunc(instance.Terminate); err != nil {
		mo.l.Error(fmt.Sprintf("failed to terminate instance %s: %s", instance.Id(), err.Error()))
		return errors.WithStack(err)
	}
	mo.l.Info(fmt.Sprintf("runner(%s): success terminating instance %s", id, instance.Id()))
	return nil
}
