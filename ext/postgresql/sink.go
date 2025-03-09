package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"

	extcommon "github.com/goto/optimus-any2any/ext/common"
	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

type PGSink struct {
	*common.Sink
	ctx context.Context

	conn               *pgx.Conn
	destinationTableID string

	batchSize         int                      // internal use
	recordsBuffer     []map[string]interface{} // internal use
	fileRecordCounter int                      // internal use
}

var _ flow.Sink = (*PGSink)(nil)

// NewSink creates a new PGSink
func NewSink(ctx context.Context, l *slog.Logger, metadataPrefix string,
	connectionDSN, preSQLScript, destinationTableID string,
	opts ...common.Option) (*PGSink, error) {

	// create common sink
	commonSink := common.NewSink(l, metadataPrefix, opts...)

	// create pg connection
	conn, err := pgx.Connect(ctx, connectionDSN)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	pgSink := &PGSink{
		Sink:               commonSink,
		ctx:                ctx,
		conn:               conn,
		destinationTableID: destinationTableID,
		batchSize:          512,
		recordsBuffer:      make([]map[string]interface{}, 0, 512),
		fileRecordCounter:  0,
	}

	// execute preSQLScript
	if _, err := conn.Exec(ctx, preSQLScript); err != nil {
		return nil, errors.WithStack(err)
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Info("sink(pg): close pg connection")
		_ = pgSink.conn.Close(ctx)
	})

	// register sink process
	commonSink.RegisterProcess(pgSink.process)

	return pgSink, nil
}

func (p *PGSink) process() {
	for msg := range p.Read() {
		if p.Err() != nil {
			continue
		}
		b, ok := msg.([]byte)
		if !ok {
			p.Logger.Error("sink(pg): invalid message type")
			p.SetError(errors.New(fmt.Sprintf("sink(pg): invalid message type: %T", msg)))
			continue
		}
		p.Logger.Debug(fmt.Sprintf("sink(pg): received message: %s", string(b)))

		var record map[string]interface{}
		if err := json.Unmarshal(b, &record); err != nil {
			p.Logger.Error(fmt.Sprintf("sink(pg): failed to unmarshal message: %s", string(b)))
			p.SetError(errors.WithStack(errors.WithMessage(err, fmt.Sprintf("sink(pg): failed to unmarshal message: %s", string(b)))))
			continue
		}

		p.recordsBuffer = append(p.recordsBuffer, record)
		p.fileRecordCounter++

		if len(p.recordsBuffer) < p.batchSize {
			continue
		}

		// flush records buffer to file
		if err := p.flush(); err != nil {
			p.Logger.Error("sink(pg): failed to flush records")
			p.SetError(errors.WithStack(err))
			return
		}
	}

	if p.Err() != nil {
		return
	}

	// flush remaining records
	if len(p.recordsBuffer) > 0 {
		if err := p.flush(); err != nil {
			p.Logger.Error("sink(pg): failed to flush remaining records")
			p.SetError(errors.WithStack(err))
		}
	}
}

// flush writes records buffer to file
func (p *PGSink) flush() error {
	var wg sync.WaitGroup
	pipeReader, pipeWriter := io.Pipe()
	defer func() {
		p.Logger.Debug("sink(pg): clear records buffer")
		p.recordsBuffer = p.recordsBuffer[:0]
	}()

	// converting to csv
	errChan := make(chan error)
	wg.Add(1)
	go func(errChan chan error) {
		defer func() {
			p.Logger.Debug("sink(pg): close pipe writer")
			pipeWriter.Close()
			wg.Done()
		}()
		if err := extcommon.ToCSV(p.Logger, pipeWriter, p.recordsBuffer); err != nil {
			errChan <- errors.WithStack(err)
			return
		}
	}(errChan)

	// piping the records to pg
	query := fmt.Sprintf(`COPY %s FROM STDIN DELIMITER ',' CSV HEADER;`, p.destinationTableID)
	p.Logger.Info(fmt.Sprintf("sink(pg): start writing %d records to pg", len(p.recordsBuffer)))
	p.Logger.Debug(fmt.Sprintf("sink(pg): query: %s", query))
	t, err := p.conn.PgConn().CopyFrom(p.ctx, pipeReader, query)
	if err != nil {
		return errors.WithStack(err)
	}
	p.Logger.Info(fmt.Sprintf("sink(pg): done writing %d records to pg", t.RowsAffected()))
	wg.Wait()

	// check if there is an error
	select {
	case err := <-errChan:
		return errors.WithStack(err)
	default:
	}

	return nil
}
