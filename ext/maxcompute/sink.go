package maxcompute

import (
	"context"
	"fmt"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tableschema"
	"github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type MaxcomputeSink struct {
	ctx         context.Context
	client      *odps.Odps
	destination *odps.Table
	done        chan uint8
	c           chan any
}

var _ flow.Sink = (*MaxcomputeSink)(nil)

// NewSink creates a new MaxcomputeSink
// svcAcc is the service account json string refer to maxComputeCredentials
// tableID is the table ID to write to, it must be in the format of project_name.schema_name.table_name
func NewSink(ctx context.Context, svcAcc string, tableID string, opts ...flow.Option) (*MaxcomputeSink, error) {
	client, err := NewClient(svcAcc)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	destination, err := getTable(client, tableID)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	mc := &MaxcomputeSink{
		ctx:         ctx,
		client:      client,
		destination: destination,
		done:        make(chan uint8),
		c:           make(chan any),
	}

	for _, opt := range opts {
		opt(mc)
	}

	if err = mc.init(); err != nil {
		return nil, errors.WithStack(err)
	}
	return mc, nil
}

func (mc *MaxcomputeSink) init() error {
	// Initialize tunnel
	t, err := tunnel.NewTunnelFromProject(mc.client.DefaultProject())
	if err != nil {
		return errors.WithStack(err)
	}

	// Create session for upload
	session, err := t.CreateUploadSession(mc.destination.ProjectName(), mc.destination.Name())
	if err != nil {
		return errors.WithStack(err)
	}

	// Get schema of the destination table
	schema := session.Schema()

	// Open protoc record writer
	recordWriter, err := session.OpenRecordWriter(0) // TODO: use proper block id
	if err != nil {
		return errors.WithStack(err)
	}

	// Ready to write records
	go func(w *tunnel.RecordProtocWriter, s tableschema.TableSchema) {
		defer func() {
			// TODO: properly handle writer close + session commit
			_ = w.Close()
			err = session.Commit([]int{0})
			if err != nil {
				// TODO: log error
				fmt.Println(err.Error())
			}
			mc.done <- 0
		}()
		// Write records individually
		for msg := range mc.c {
			b, ok := msg.(string)
			if !ok {
				fmt.Println("invalid message")
				// TODO: log error
				continue
			}
			record, err := createRecord([]byte(b), s)
			if err != nil {
				fmt.Println(err.Error())
				// TODO: log error
				continue
			}
			w.Write(record)
		}
	}(recordWriter, schema)

	return nil
}

func (mc *MaxcomputeSink) SetBufferSize(size int) {
	mc.c = make(chan any, size)
}

func (mc *MaxcomputeSink) In() chan<- any {
	return mc.c
}

func (mc *MaxcomputeSink) Wait() {
	select {
	case <-mc.ctx.Done():
	case <-mc.done:
	}
	close(mc.done)
}
