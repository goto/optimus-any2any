package postgresql

import (
	"fmt"
	"io"
	"os"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/internal/helper"
	xio "github.com/goto/optimus-any2any/internal/io"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"
)

const (
	tmpFile = "/tmp/pg_sink.tmp.json"
)

type PGSink struct {
	common.Sink

	conn               *pgx.Conn
	destinationTableID string

	batchSize         int            // internal use
	writerTmpHandler  io.WriteCloser // internal use
	fileRecordCounter int            // internal use
}

var _ flow.Sink = (*PGSink)(nil)

// NewSink creates a new PGSink
func NewSink(commonSink common.Sink,
	connectionDSN, preSQLScript, destinationTableID string,
	batchSize int, opts ...common.Option) (*PGSink, error) {

	// create pg connection
	conn, err := pgx.Connect(commonSink.Context(), connectionDSN)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	p := &PGSink{
		Sink:               commonSink,
		conn:               conn,
		destinationTableID: destinationTableID,
		batchSize:          512,
		writerTmpHandler:   nil,
		fileRecordCounter:  0,
	}

	// execute preSQLScript
	p.Logger().Info(fmt.Sprintf("execute preSQLScript: %s", preSQLScript))
	if _, err := conn.Exec(p.Context(), preSQLScript); err != nil {
		return nil, errors.WithStack(err)
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		p.Logger().Info(fmt.Sprintf("close pg connection"))
		return p.conn.Close(p.Context())
	})

	// register sink process
	commonSink.RegisterProcess(p.process)

	return p, nil
}

func (p *PGSink) process() error {
	for v := range p.Read() {
		if p.writerTmpHandler == nil {
			// create tmp file handler
			f, err := xio.NewWriteHandler(p.Logger(), tmpFile)
			if err != nil {
				return errors.WithStack(err)
			}
			p.writerTmpHandler = f
		}
		_, err := p.writerTmpHandler.Write(append(v, '\n'))
		if err != nil {
			return errors.WithStack(err)
		}

		p.fileRecordCounter++
		if p.fileRecordCounter < p.batchSize {
			continue
		}

		// flush records buffer to file
		if err := p.Retry(p.flush); err != nil {
			p.Logger().Error(fmt.Sprintf("failed to flush records"))
			return errors.WithStack(err)
		}
	}

	// flush remaining records
	if p.fileRecordCounter > 0 {
		if err := p.Retry(p.flush); err != nil {
			p.Logger().Error(fmt.Sprintf("failed to flush remaining records"))
			return errors.WithStack(err)
		}
	}
	return nil
}

// flush writes records buffer to file
func (p *PGSink) flush() error {
	// close the writer first
	if err := p.writerTmpHandler.Close(); err != nil {
		p.Logger().Error(fmt.Sprintf("failed to close writer"))
		return errors.WithStack(err)
	}
	defer func() {
		p.Logger().Debug(fmt.Sprintf("clear records buffer"))
		p.writerTmpHandler = nil
		p.fileRecordCounter = 0
		if err := os.Remove(tmpFile); err != nil {
			p.Logger().Error(fmt.Sprintf("failed to remove tmp file"))
			return
		}
	}()

	// read from the tmp file
	f, err := os.OpenFile(tmpFile, os.O_RDONLY, 0644)
	if err != nil {
		p.Logger().Error(fmt.Sprintf("failed to open tmp file"))
		return errors.WithStack(err)
	}

	r := helper.FromJSONToCSV(p.Logger(), f, false) // no skip header by default

	// piping the records to pg
	query := fmt.Sprintf(`COPY %s FROM STDIN DELIMITER ',' CSV HEADER;`, p.destinationTableID)
	p.Logger().Info(fmt.Sprintf("start writing %d records to pg", p.fileRecordCounter))
	p.Logger().Debug(fmt.Sprintf("query: %s", query))
	t, err := p.conn.PgConn().CopyFrom(p.Context(), r, query)
	if err != nil {
		return errors.WithStack(err)
	}
	p.Logger().Info(fmt.Sprintf("done writing %d records to pg", t.RowsAffected()))

	return nil
}
