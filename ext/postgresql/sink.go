package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/gocarina/gocsv"
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
	destinationPath   string                   // internal use
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
		destinationPath:    "/tmp/records.csv",
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
			p.Logger.Error("sink(pg): failed to unmarshal message")
			p.SetError(errors.WithStack(err))
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
	// prepare tmp file
	p.Logger.Debug(fmt.Sprintf("sink(pg): create tmp file %s", p.destinationPath))
	f, err := os.OpenFile(p.destinationPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if err != nil {
		return errors.WithStack(err)
	}
	defer func() {
		p.Logger.Debug(fmt.Sprintf("sink(pg): remove tmp file %s", p.destinationPath))
		_ = os.Remove(p.destinationPath)
	}()
	defer f.Close()

	p.Logger.Info(fmt.Sprintf("sink(pg): writing %d records to %s", len(p.recordsBuffer), p.destinationPath))
	// converting to csv
	if err := gocsv.MarshalFile(&p.recordsBuffer, f); err != nil {
		return errors.WithStack(err)
	}
	// clear records buffer
	p.recordsBuffer = p.recordsBuffer[:0]

	// copy records from file to pg
	p.Logger.Info(fmt.Sprintf("sink(pg): writing records from %s to pg", p.destinationPath))
	query := fmt.Sprintf(`COPY %s FROM '%s' DELIMITER ',' CSV HEADER;`, p.destinationPath, p.destinationPath)
	if _, err := p.conn.Exec(p.ctx, query); err != nil {
		return errors.WithStack(err)
	}

	return nil
}
