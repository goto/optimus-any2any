package maxcompute

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

const (
	LOAD_METHOD_APPEND  = "APPEND" // default
	LOAD_METHOD_REPLACE = "REPLACE"
)

type MaxcomputeSink struct {
	*common.CommonSink

	tableSchema tableschema.TableSchema
	uploadMode  string
	// for stream mode
	sessionStream *tunnel.StreamUploadSession
	packWriter    *tunnel.RecordPackStreamWriter
	// for regular mode
	sessionRegular *tunnel.UploadSession
	recordWriter   *tunnel.RecordProtocWriter

	client             *odps.Odps
	loadMethod         string
	tableIDTransition  string
	tableIDDestination string
}

var _ flow.Sink = (*MaxcomputeSink)(nil)

// NewSink creates a new MaxcomputeSink
func NewSink(l *slog.Logger, creds string, executionProject string, tableID string, loadMethod string, uploadMode string, allowSchemaMismatch bool, opts ...common.Option) (*MaxcomputeSink, error) {
	// create commonSink sink
	commonSink := common.NewCommonSink(l, "mc", opts...)

	// create client for maxcompute
	client, err := NewClient(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if executionProject != "" {
		client.SetDefaultProjectName(executionProject)
	}
	l.Info(fmt.Sprintf("client created, execution project: %s", client.DefaultProject().Name()))

	tableIDDestination := tableID
	// stream to temporary table if load method is replace
	if loadMethod == LOAD_METHOD_REPLACE {
		tableID = fmt.Sprintf("%s_temp_%d", strings.ReplaceAll(tableID, "`", ""), time.Now().Unix())
		l.Info(fmt.Sprintf("load method is replace, creating temporary table: %s", tableID))
		if err := createTempTable(l, client.Odps, tableID, tableIDDestination, 1); err != nil {
			return nil, errors.WithStack(err)
		}
		l.Info(fmt.Sprintf("temporary table created: %s", tableID))
	}

	destination, err := getTable(l, client.Odps, tableID)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t, err := tunnel.NewTunnelFromProject(client.DefaultProject())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// create session based on upload mode
	var (
		sessionStream  *tunnel.StreamUploadSession
		sessionRegular *tunnel.UploadSession
		packWriter     *tunnel.RecordPackStreamWriter
		recordWriter   *tunnel.RecordProtocWriter
	)
	if uploadMode == "STREAM" {
		session, err := t.CreateStreamUploadSession(destination.ProjectName(), destination.Name(),
			tunnel.SessionCfg.WithSchemaName(destination.SchemaName()),
			tunnel.SessionCfg.WithAllowSchemaMismatch(allowSchemaMismatch),
		)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		sessionStream = session
		packWriter = session.OpenRecordPackWriter()
	} else if uploadMode == "REGULAR" {
		session, err := t.CreateUploadSession(destination.ProjectName(), destination.Name(),
			tunnel.SessionCfg.WithSchemaName(destination.SchemaName()),
			tunnel.SessionCfg.WithAllowSchemaMismatch(allowSchemaMismatch),
		)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		sessionRegular = session
		rw, err := session.OpenRecordWriter(0) // TODO: use proper block id
		if err != nil {
			return nil, errors.WithStack(err)
		}
		recordWriter = rw
	} else {
		return nil, errors.New(fmt.Sprintf("invalid upload mode: %s", uploadMode))
	}

	mc := &MaxcomputeSink{
		CommonSink:         commonSink,
		tableSchema:        destination.Schema(),
		client:             client.Odps,
		loadMethod:         loadMethod,
		tableIDTransition:  tableID,
		tableIDDestination: tableIDDestination,
		uploadMode:         uploadMode,
		// for stream mode
		sessionStream: sessionStream,
		packWriter:    packWriter,
		// for regular mode
		sessionRegular: sessionRegular,
		recordWriter:   recordWriter,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		mc.Logger().Info(fmt.Sprintf("drop temporary table"))
		// delete temporary table if load method is replace
		if mc.loadMethod == LOAD_METHOD_REPLACE {
			mc.Logger().Info(fmt.Sprintf("load method is replace, deleting temporary table: %s", mc.tableIDTransition))
			return mc.Retry(func() error {
				return dropTable(mc.Logger(), client.Odps, mc.tableIDTransition)
			})
		}
		return nil
	})
	// register process, it will immediately start the process
	commonSink.RegisterProcess(mc.process)

	return mc, nil
}

func (mc *MaxcomputeSink) process() error {
	mc.Logger().Info(fmt.Sprintf("start writing records to table: %s", mc.tableSchema.TableName))
	mc.Logger().Debug(fmt.Sprintf("record column: %+v", mc.tableSchema.Columns))
	countRecord := 0
	for record, err := range mc.ReadRecord() {
		if err != nil {
			return errors.WithStack(err)
		}
		record = mc.RecordWithoutMetadata(record)

		mcRecord, err := createRecord(mc.Logger(), record, mc.tableSchema)
		if err != nil {
			mc.Logger().Error(fmt.Sprintf("record creation error: %s", err.Error()))
			return errors.WithStack(err)
		}
		mc.Logger().Debug(fmt.Sprintf("record: %v", mcRecord))

		countRecord++
		if mc.uploadMode == "STREAM" {
			if err := mc.packWriter.Append(mcRecord); err != nil {
				mc.Logger().Error(fmt.Sprintf("record write error: %s", err.Error()))
				return errors.WithStack(err)
			}
			if mc.packWriter.DataSize() > 524288 { // flush every ~512KB
				mc.Logger().Info(fmt.Sprintf("write %d records", countRecord))
				if err := mc.Retry(mc.flush); err != nil {
					mc.Logger().Error(fmt.Sprintf("record flush error: %s", err.Error()))
					return errors.WithStack(err)
				}
			}
		} else if mc.uploadMode == "REGULAR" {
			if err := mc.recordWriter.Write(mcRecord); err != nil {
				mc.Logger().Error(fmt.Sprintf("record write error: %s", err.Error()))
				return errors.WithStack(err)
			}
			if countRecord%100 == 0 {
				mc.Logger().Info(fmt.Sprintf("write %d records", countRecord))
			}
		}
	}

	if countRecord > 0 {
		mc.Logger().Info(fmt.Sprintf("write %d records", countRecord))
	}

	if mc.uploadMode == "STREAM" {
		// flush remaining records
		if err := mc.Retry(mc.flush); err != nil {
			mc.Logger().Error(fmt.Sprintf("record flush error: %s", err.Error()))
			return errors.WithStack(err)
		}
	} else if mc.uploadMode == "REGULAR" {
		if err := mc.recordWriter.Close(); err != nil {
			mc.Logger().Error(fmt.Sprintf("record writer close error: %s", err.Error()))
			return errors.WithStack(err)
		}
		err := mc.Retry(func() error {
			return mc.sessionRegular.Commit([]int{0})
		})
		if err != nil {
			mc.Logger().Error(fmt.Sprintf("session commit error: %s", err.Error()))
			return errors.WithStack(err)
		}
	}

	if mc.loadMethod == LOAD_METHOD_REPLACE {
		mc.Logger().Info(fmt.Sprintf("load method is replace, load data from temporary table to destination table: %s", mc.tableIDDestination))
		if err := insertOverwrite(mc.Logger(), mc.client, mc.tableIDDestination, mc.tableIDTransition); err != nil {
			mc.Logger().Error(fmt.Sprintf("insert overwrite error: %s", err.Error()))
			return errors.WithStack(err)
		}
		mc.Logger().Info(fmt.Sprintf("load method is replace, data successfully loaded to destination table: %s", mc.tableIDDestination))
	}
	return nil
}

func (mc *MaxcomputeSink) flush() error {
	traceId, recordCount, bytesSend, err := mc.packWriter.Flush()
	if err != nil {
		return errors.WithStack(err)
	}
	mc.Logger().Debug(fmt.Sprintf("flush trace id: %s, record count: %d, bytes send: %d", traceId, recordCount, bytesSend))
	return nil
}
