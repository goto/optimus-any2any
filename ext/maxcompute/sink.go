package maxcompute

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/ext/common/model"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

const (
	LOAD_METHOD_APPEND  = "APPEND" // default
	LOAD_METHOD_REPLACE = "REPLACE"
)

type MaxcomputeSink struct {
	*common.Sink

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
func NewSink(l *slog.Logger, metadataPrefix string, creds string, executionProject string, tableID string, loadMethod string, uploadMode string, allowSchemaMismatch bool, opts ...common.Option) (*MaxcomputeSink, error) {
	// create commonSink sink
	commonSink := common.NewSink(l, metadataPrefix, opts...)

	// create client for maxcompute
	client, err := NewClient(creds)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if executionProject != "" {
		client.SetDefaultProjectName(executionProject)
	}
	l.Info(fmt.Sprintf("sink(mc): client created, execution project: %s", client.DefaultProject().Name()))

	tableIDDestination := tableID
	// stream to temporary table if load method is replace
	if loadMethod == LOAD_METHOD_REPLACE {
		tableID = fmt.Sprintf("%s_temp_%d", strings.ReplaceAll(tableID, "`", ""), time.Now().Unix())
		l.Info(fmt.Sprintf("sink(mc): load method is replace, creating temporary table: %s", tableID))
		if err := createTempTable(l, client, tableID, tableIDDestination, 1); err != nil {
			return nil, errors.WithStack(err)
		}
		l.Info(fmt.Sprintf("sink(mc): temporary table created: %s", tableID))
	}

	destination, err := getTable(l, client, tableID)
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
		Sink:               commonSink,
		tableSchema:        destination.Schema(),
		client:             client,
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
	commonSink.AddCleanFunc(func() {
		l.Debug("sink(mc): drop temporary table")
		// delete temporary table if load method is replace
		if mc.loadMethod == LOAD_METHOD_REPLACE {
			l.Info(fmt.Sprintf("sink(mc): load method is replace, deleting temporary table: %s", mc.tableIDTransition))
			if err := dropTable(l, client, mc.tableIDTransition); err != nil {
				l.Error(fmt.Sprintf("sink(mc): delete temporary table error: %s", err.Error()))
				mc.SetError(errors.WithStack(err))
			}
		}
	})
	// register process, it will immediately start the process
	commonSink.RegisterProcess(mc.process)

	return mc, nil
}

func (mc *MaxcomputeSink) process() {
	mc.Logger.Info(fmt.Sprintf("sink(mc): start writing records to table: %s", mc.tableSchema.TableName))
	mc.Logger.Debug(fmt.Sprintf("sink(mc): record column: %+v", mc.tableSchema.Columns))
	countRecord := 0
	for msg := range mc.Read() {
		if mc.Err() != nil {
			mc.Logger.Debug("sink(mc): got an error, skip processing")
			continue
		}
		b, ok := msg.([]byte)
		if !ok {
			err := errors.New(fmt.Sprintf("message type assertion error: %T", msg))
			mc.Logger.Error(fmt.Sprintf("sink(mc): message type assertion error: %s", err.Error()))
			mc.SetError(errors.WithStack(err))
			continue
		}

		var record model.Record
		if err := json.Unmarshal(b, &record); err != nil {
			mc.Logger.Error(fmt.Sprintf("sink(mc): message unmarshal error: %s", err.Error()))
			mc.SetError(errors.WithStack(err))
			continue
		}
		record = extcommon.RecordWithoutMetadata(record, mc.MetadataPrefix)

		mc.Logger.Debug(fmt.Sprintf("sink(mc): message: %s", string(b)))
		mcRecord, err := createRecord(mc.Logger, record, mc.tableSchema)
		if err != nil {
			mc.Logger.Error(fmt.Sprintf("record creation error: %s", err.Error()))
			mc.SetError(errors.WithStack(err))
			continue
		}
		mc.Logger.Debug(fmt.Sprintf("sink(mc): record: %v", mcRecord))

		countRecord++
		if mc.uploadMode == "STREAM" {
			if err := mc.packWriter.Append(mcRecord); err != nil {
				mc.Logger.Error(fmt.Sprintf("sink(mc): record write error: %s", err.Error()))
				mc.SetError(errors.WithStack(err))
				continue
			}
			if mc.packWriter.DataSize() > 524288 { // flush every ~512KB
				mc.Logger.Info(fmt.Sprintf("sink(mc): write %d records", countRecord))
				if err := mc.Retry(mc.flush); err != nil {
					mc.Logger.Error(fmt.Sprintf("sink(mc): record flush error: %s", err.Error()))
					mc.SetError(errors.WithStack(err))
					continue
				}
			}
		} else if mc.uploadMode == "REGULAR" {
			if err := mc.recordWriter.Write(mcRecord); err != nil {
				mc.Logger.Error(fmt.Sprintf("sink(mc): record write error: %s", err.Error()))
				mc.SetError(errors.WithStack(err))
				continue
			}
			if countRecord%100 == 0 {
				mc.Logger.Info(fmt.Sprintf("sink(mc): write %d records", countRecord))
			}
		}
	}
	if mc.Err() != nil {
		// don't commit if there's an error
		return
	}

	if countRecord > 0 {
		mc.Logger.Info(fmt.Sprintf("sink(mc): write %d records", countRecord))
	}

	if mc.uploadMode == "STREAM" {
		// flush remaining records
		if err := mc.Retry(mc.flush); err != nil {
			mc.Logger.Error(fmt.Sprintf("sink(mc): record flush error: %s", err.Error()))
			mc.SetError(errors.WithStack(err))
			return
		}
	} else if mc.uploadMode == "REGULAR" {
		if err := mc.recordWriter.Close(); err != nil {
			mc.Logger.Error(fmt.Sprintf("sink(mc): record writer close error: %s", err.Error()))
			mc.SetError(errors.WithStack(err))
			return
		}
		err := mc.Retry(func() error {
			return mc.sessionRegular.Commit([]int{0})
		})
		if err != nil {
			mc.Logger.Error(fmt.Sprintf("sink(mc): session commit error: %s", err.Error()))
			mc.SetError(errors.WithStack(err))
			return
		}
	}

	if mc.loadMethod == LOAD_METHOD_REPLACE {
		mc.Logger.Info(fmt.Sprintf("sink(mc): load method is replace, load data from temporary table to destination table: %s", mc.tableIDDestination))
		if err := insertOverwrite(mc.Logger, mc.client, mc.tableIDDestination, mc.tableIDTransition); err != nil {
			mc.Logger.Error(fmt.Sprintf("sink(mc): insert overwrite error: %s", err.Error()))
			mc.SetError(errors.WithStack(err))
		}
		mc.Logger.Info(fmt.Sprintf("sink(mc): load method is replace, data successfully loaded to destination table: %s", mc.tableIDDestination))
	}
}

func (mc *MaxcomputeSink) flush() error {
	traceId, recordCount, bytesSend, err := mc.packWriter.Flush()
	if err != nil {
		return err
	}
	mc.Logger.Debug(fmt.Sprintf("sink(mc): flush trace id: %s, record count: %d, bytes send: %d", traceId, recordCount, bytesSend))
	return nil
}
