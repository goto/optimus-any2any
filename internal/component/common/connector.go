package common

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"

	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type ConnectorExecFunc func(inputReader io.Reader) (io.Reader, error)

// Connector is a struct that extends the component.Connector
// to provide additional functionality for processing records in batches
// with some specialized data handling.
type Connector struct {
	*component.Connector
	metadataPrefix   string
	batchSize        int
	batchIndexColumn string
	exec             ConnectorExecFunc
}

func NewConnector(ctx context.Context, cancelFn context.CancelCauseFunc, logger *slog.Logger, metadataPrefix string, batchSize int, batchIndexColumn string, name string, execFunc ConnectorExecFunc) (*Connector, error) {
	c := &Connector{
		metadataPrefix:   metadataPrefix,
		batchSize:        batchSize,
		batchIndexColumn: batchIndexColumn,
		exec:             execFunc,
	}
	c.Connector = component.NewConnector(ctx, cancelFn, logger, name, c.process)
	return c, nil
}

// func (c *Connector) Connect

func (c *Connector) process(outlet flow.Outlet, inlets ...flow.Inlet) error {
	// create a buffer to hold the batch of records
	var batchBuffer bytes.Buffer
	recordCount := 0

	// process records in batches
	for record, err := range ReadRecordFromOutlet(outlet) {
		if err != nil {
			return errors.WithStack(err)
		}

		// check if the record is a specialized metadata record
		if IsSpecializedMetadataRecord(record, c.metadataPrefix) {
			// marshal the record to JSON
			raw, err := json.Marshal(record)
			if err != nil {
				return errors.WithStack(err)
			}
			// send specialized metadata records directly to inlets
			flush(bytes.NewReader(raw), inlets...)
			continue
		}

		// add batch index as metadata
		record.Set(c.batchIndexColumn, int(recordCount/c.batchSize))
		// marshal the record to JSON
		raw, err := json.Marshal(record)
		if err != nil {
			return errors.WithStack(err)
		}

		// write the JSON byte array directly to the buffer
		batchBuffer.Write(append(raw, '\n'))
		recordCount++

		// when we reach batch size, process the batch
		if recordCount%c.batchSize == 0 {
			batchOutputReader, err := c.exec(&batchBuffer)
			if err != nil {
				return errors.WithStack(err)
			}
			if err := flush(batchOutputReader, inlets...); err != nil {
				return errors.WithStack(err)
			}
			// reset the buffer
			batchBuffer = bytes.Buffer{}
		}
	}

	// process any remaining records
	if recordCount%c.batchSize != 0 {
		batchOutputReader, err := c.exec(&batchBuffer)
		if err != nil {
			return errors.WithStack(err)
		}
		if err := flush(batchOutputReader, inlets...); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func flush(r io.Reader, inlets ...flow.Inlet) error {
	// split the input by newlines and send each record to all inlets
	reader := bufio.NewReader(r)
	for {
		raw, err := reader.ReadBytes('\n')
		if len(raw) > 0 {
			line := make([]byte, len(raw))
			copy(line, raw)
			for _, inlet := range inlets {
				inlet.In(bytes.TrimSpace(line))
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
