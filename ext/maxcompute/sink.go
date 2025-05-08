package maxcompute

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

const (
	LOAD_METHOD_APPEND  = "APPEND" // default
	LOAD_METHOD_REPLACE = "REPLACE"
	UPLOAD_MODE_STREAM  = "STREAM" // default
	UPLOAD_MODE_REGULAR = "REGULAR"
)

type MaxcomputeSink struct {
	common.Sink

	Client     *Client
	uploadMode string

	loadMethod         string
	tableIDTransition  string
	tableIDDestination string
}

var _ flow.Sink = (*MaxcomputeSink)(nil)

// NewSink creates a new MaxcomputeSink
func NewSink(commonSink common.Sink, creds string, executionProject string, tableID string, loadMethod string, uploadMode string, batchSizeInMB int, concurrency int, allowSchemaMismatch bool, opts ...common.Option) (*MaxcomputeSink, error) {
	// create client for maxcompute
	client, err := NewClient(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if executionProject != "" {
		client.SetDefaultProjectName(executionProject)
	}
	commonSink.Logger().Info(fmt.Sprintf("client created, execution project: %s", client.DefaultProject().Name()))

	tableIDDestination := tableID
	// stream to temporary table if load method is replace
	err = commonSink.DryRunable(func() error {
		if loadMethod == LOAD_METHOD_REPLACE {
			tableID = fmt.Sprintf("%s_temp_%d", strings.ReplaceAll(tableID, "`", ""), time.Now().Unix())
			commonSink.Logger().Info(fmt.Sprintf("load method is replace, creating temporary table: %s", tableID))
			if err := createTempTable(commonSink.Logger(), client.Odps, tableID, tableIDDestination, 1); err != nil {
				return errors.WithStack(err)
			}
			commonSink.Logger().Info(fmt.Sprintf("temporary table created: %s", tableID))
		}
		return nil
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t, err := tunnel.NewTunnelFromProject(client.DefaultProject())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	client.StreamWriter = func(tableID string) (*mcStreamRecordSender, error) {
		destination, err := getTable(commonSink.Logger(), client.Odps, tableID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		session, err := t.CreateStreamUploadSession(destination.ProjectName(), destination.Name(),
			tunnel.SessionCfg.WithSchemaName(destination.SchemaName()),
			tunnel.SessionCfg.WithAllowSchemaMismatch(allowSchemaMismatch),
		)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		recordSender, err := newStreamRecordSender(commonSink.Logger(), session, batchSizeInMB)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		recordSender.retryFunc = commonSink.Retry
		return recordSender, nil
	}

	client.BatchWriter = func(tableID string) (*mcBatchRecordSender, error) {
		destination, err := getTable(commonSink.Logger(), client.Odps, tableID)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		session, err := t.CreateUploadSession(destination.ProjectName(), destination.Name(),
			tunnel.SessionCfg.WithSchemaName(destination.SchemaName()),
			tunnel.SessionCfg.WithAllowSchemaMismatch(allowSchemaMismatch),
		)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newBatchRecordSender(commonSink.Logger(), session, batchSizeInMB, concurrency)
	}

	mc := &MaxcomputeSink{
		Sink:               commonSink,
		Client:             client,
		loadMethod:         loadMethod,
		tableIDTransition:  tableID,
		tableIDDestination: tableIDDestination,
		uploadMode:         uploadMode,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		mc.Logger().Info(fmt.Sprintf("drop temporary table"))
		// delete temporary table if load method is replace
		if mc.loadMethod == LOAD_METHOD_REPLACE {
			mc.Logger().Info(fmt.Sprintf("load method is replace, deleting temporary table: %s", mc.tableIDTransition))
			return mc.Retry(func() error {
				return mc.DryRunable(func() error {
					return dropTable(mc.Logger(), client.Odps, mc.tableIDTransition)
				})
			})
		}
		return nil
	})
	// register process, it will immediately start the process
	commonSink.RegisterProcess(mc.process)

	return mc, nil
}

func (mc *MaxcomputeSink) process() error {
	mc.Logger().Info(fmt.Sprintf("start writing records to table: %s", mc.tableIDTransition))
	logCheckpoint := 1000
	countRecord := 0

	// prepare record sender
	var sender common.RecordSender
	var closer io.Closer
	var err error
	if mc.uploadMode == UPLOAD_MODE_STREAM {
		streamWriter, e := mc.Client.StreamWriter(mc.tableIDTransition)
		sender = streamWriter
		closer = streamWriter
		err = e
	} else if mc.uploadMode == UPLOAD_MODE_REGULAR {
		batchWriter, e := mc.Client.BatchWriter(mc.tableIDTransition)
		sender = batchWriter
		closer = batchWriter
		err = e
	} else {
		err = fmt.Errorf("not supported upload mode %s", mc.uploadMode)
	}
	if err != nil {
		return errors.WithStack(err)
	}

	// read record and send via specific sender
	for record, err := range mc.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		record = mc.RecordWithoutMetadata(record)

		// send record to maxcompute
		err = mc.DryRunable(func() error {
			if err := mc.Retry(func() error {
				return sender.SendRecord(record)
			}); err != nil {
				return errors.WithStack(err)
			}
			countRecord++

			if countRecord%logCheckpoint == 0 {
				mc.Logger().Info(fmt.Sprintf("send %d records", countRecord))
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	if countRecord > 0 {
		mc.Logger().Info(fmt.Sprintf("write %d records", countRecord))
	}

	if err := mc.DryRunable(closer.Close); err != nil {
		return errors.WithStack(err)
	}

	err = mc.DryRunable(func() error {
		if mc.loadMethod == LOAD_METHOD_REPLACE {
			mc.Logger().Info(fmt.Sprintf("load method is replace, load data from temporary table to destination table: %s", mc.tableIDDestination))
			if err := mc.Retry(func() error {
				queryToExecute, err := insertOverwriteQuery(mc.Logger(), mc.Client.Odps, mc.tableIDDestination, mc.tableIDTransition)
				if err != nil {
					return errors.WithStack(err)
				}
				mc.Logger().Info(fmt.Sprintf("executing query: %s", queryToExecute))
				instance, err := mc.Client.Odps.ExecSQl(queryToExecute)
				if err != nil {
					return errors.WithStack(err)
				}
				// generate logview
				url, err := generateLogView(mc.Logger(), mc.Client.Odps, instance, 2) // 2 days
				if err != nil {
					return errors.WithStack(err)
				}
				mc.Logger().Info(fmt.Sprintf("log view url: %s", url))

				// register clean func to terminate instance
				mc.AddCleanFunc(func() error {
					if err := mc.Retry(instance.Load); err != nil {
						return errors.WithStack(err)
					}
					if instance.Status() == odps.InstanceTerminated { // instance is terminated, no need to terminate again
						return nil
					}
					if err := mc.Retry(instance.Terminate); err != nil {
						return errors.WithStack(err)
					}
					return nil
				})

				// wait for instance to finish
				mc.Logger().Info(fmt.Sprintf("waiting for instance %s to finish...", instance.Id()))
				return errors.WithStack(instance.WaitForSuccess())
			}); err != nil {
				mc.Logger().Error(fmt.Sprintf("insert overwrite error: %s", err.Error()))
				return errors.WithStack(err)
			}
			mc.Logger().Info(fmt.Sprintf("load method is replace, data successfully loaded to destination table: %s", mc.tableIDDestination))
		}
		return nil
	})
	return errors.WithStack(err)
}
