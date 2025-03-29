package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/helper"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

type PGSink struct {
	*common.CommonSink
	ctx context.Context

	conn               *pgx.Conn
	destinationTableID string

	batchSize         int            // internal use
	records           []model.Record // internal use
	fileRecordCounter int            // internal use
}

var _ flow.Sink = (*PGSink)(nil)

// NewSink creates a new PGSink
func NewSink(ctx context.Context, l *slog.Logger, metadataPrefix string,
	connectionDSN, preSQLScript, destinationTableID string,
	batchSize int, opts ...common.Option) (*PGSink, error) {

	// create common sink
	commonSink := common.NewCommonSink(l, "pg", metadataPrefix, opts...)

	// create pg connection
	conn, err := pgx.Connect(ctx, connectionDSN)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	p := &PGSink{
		CommonSink:         commonSink,
		ctx:                ctx,
		conn:               conn,
		destinationTableID: destinationTableID,
		batchSize:          512,
		records:            make([]model.Record, 0, batchSize),
		fileRecordCounter:  0,
	}

	// execute preSQLScript
	l.Info(fmt.Sprintf("execute preSQLScript: %s", preSQLScript))
	if _, err := conn.Exec(ctx, preSQLScript); err != nil {
		return nil, errors.WithStack(err)
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		p.Logger().Info(fmt.Sprintf("close pg connection"))
		return p.conn.Close(ctx)
	})

	// register sink process
	commonSink.RegisterProcess(p.process)

	return p, nil
}

func (p *PGSink) process() error {
	for msg := range p.Read() {
		b, ok := msg.([]byte)
		if !ok {
			p.Logger().Error(fmt.Sprintf("invalid message type"))
			return errors.WithStack(errors.New(fmt.Sprintf("invalid message type: %T", msg)))
		}
		p.Logger().Debug(fmt.Sprintf("received message: %s", string(b)))

		var record model.Record
		if err := json.Unmarshal(b, &record); err != nil {
			p.Logger().Error(fmt.Sprintf("failed to unmarshal message: %s", string(b)))
			return errors.WithStack(err)
		}

		p.records = append(p.records, record)
		p.fileRecordCounter++

		if len(p.records) < p.batchSize {
			continue
		}

		// flush records buffer to file
		if err := p.Retry(p.flush); err != nil {
			p.Logger().Error(fmt.Sprintf("failed to flush records"))
			return errors.WithStack(err)
		}
	}

	// flush remaining records
	if len(p.records) > 0 {
		if err := p.Retry(p.flush); err != nil {
			p.Logger().Error(fmt.Sprintf("failed to flush remaining records"))
			return errors.WithStack(err)
		}
	}
	return nil
}

// flush writes records buffer to file
func (p *PGSink) flush() error {
	var wg sync.WaitGroup
	r, w := io.Pipe()
	defer func() {
		p.Logger().Debug(fmt.Sprintf("clear records buffer"))
		p.records = p.records[:0]
	}()

	// converting to csv
	errChan := make(chan error)
	wg.Add(1)
	go func(w io.WriteCloser, errChan chan error) {
		defer func() {
			p.Logger().Debug(fmt.Sprintf("close pipe writer"))
			w.Close()
			wg.Done()
		}()
		if err := helper.ToCSV(p.Logger(), w, p.records, false); err != nil {
			errChan <- errors.WithStack(err)
			return
		}
	}(w, errChan)

	// piping the records to pg
	query := fmt.Sprintf(`COPY %s FROM STDIN DELIMITER ',' CSV HEADER;`, p.destinationTableID)
	p.Logger().Info(fmt.Sprintf("start writing %d records to pg", len(p.records)))
	p.Logger().Debug(fmt.Sprintf("query: %s", query))
	t, err := p.conn.PgConn().CopyFrom(p.ctx, r, query)
	if err != nil {
		return errors.WithStack(err)
	}
	p.Logger().Info(fmt.Sprintf("done writing %d records to pg", t.RowsAffected()))
	wg.Wait()

	// check if there is an error
	select {
	case err := <-errChan:
		return errors.WithStack(err)
	default:
	}

	return nil
}
