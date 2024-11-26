package maxcompute

import (
	"fmt"
	"log/slog"

	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/goto/optimus-any2any/pkg/sink"
	"github.com/pkg/errors"
)

type MaxcomputeSink struct {
	*sink.Common
	logger       *slog.Logger
	session      *tunnel.UploadSession
	recordWriter *tunnel.RecordProtocWriter
	tableSchema  tableschema.TableSchema
}

var _ flow.Sink = (*MaxcomputeSink)(nil)

// NewSink creates a new MaxcomputeSink
// svcAcc is the service account json string refer to maxComputeCredentials
// tableID is the table ID to write to, it must be in the format of project_name.schema_name.table_name
func NewSink(l *slog.Logger, svcAcc string, tableID string, opts ...flow.Option) (*MaxcomputeSink, error) {
	// create common sink
	common := sink.NewCommon(l, opts...)

	// create client for maxcompute
	client, err := NewClient(svcAcc)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	destination, err := getTable(client, tableID)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	t, err := tunnel.NewTunnelFromProject(client.DefaultProject())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	session, err := t.CreateUploadSession(destination.ProjectName(), destination.Name())
	if err != nil {
		return nil, errors.WithStack(err)
	}

	recordWriter, err := session.OpenRecordWriter(0) // TODO: use proper block id
	if err != nil {
		return nil, errors.WithStack(err)
	}

	mc := &MaxcomputeSink{
		Common:       common,
		logger:       l,
		session:      session,
		recordWriter: recordWriter,
		tableSchema:  session.Schema(),
	}

	// add clean func
	common.AddCleanFunc(func() {
		l.Debug("sink: close record writer")
		_ = recordWriter.Close()
	})
	// register process, it will immediately start the process
	common.RegisterProcess(mc.process)

	return mc, nil
}

func (mc *MaxcomputeSink) process() {
	for msg := range mc.Read() {
		b, ok := msg.([]byte)
		if !ok {
			mc.logger.Error(fmt.Sprintf("message type assertion error: %T", msg))
			continue
		}
		mc.logger.Debug(fmt.Sprintf("sink: message: %s", string(b)))
		record, err := createRecord(b, mc.tableSchema)
		if err != nil {
			mc.logger.Error(fmt.Sprintf("record creation error: %s", err.Error()))
			continue
		}
		mc.logger.Debug(fmt.Sprintf("sink: record: %s", record.String()))
		if err := mc.recordWriter.Write(record); err != nil {
			mc.logger.Error(fmt.Sprintf("record write error: %s", err.Error()))
		}
	}
	if err := mc.recordWriter.Close(); err != nil {
		mc.logger.Error(fmt.Sprintf("record writer close error: %s", err.Error()))
	}
	if err := mc.session.Commit([]int{0}); err != nil {
		mc.logger.Error(fmt.Sprintf("session commit error: %s", err.Error()))
	}
}
