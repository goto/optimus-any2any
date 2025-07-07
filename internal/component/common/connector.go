package common

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"

	"github.com/goccy/go-json"
	cq "github.com/goto/optimus-any2any/internal/concurrentqueue"
	"github.com/goto/optimus-any2any/internal/otel"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/metric"
)

type ConnectorExecFunc func(inputReader io.Reader) (io.Reader, error)

// Connector is a struct that extends the component.Connector
// to provide additional functionality for processing records in batches
// with some specialized data handling.
type Connector struct {
	*component.Connector
	exec             ConnectorExecFunc
	concurrentQueue  cq.ConcurrentQueue
	metadataPrefix   string
	batchSize        int
	batchIndexColumn string

	// metrics related
	m                    metric.Meter
	connectorBytes       metric.Int64Counter
	connectorBytesBucket metric.Int64Histogram
}

func NewConnector(ctx context.Context, cancelFn context.CancelCauseFunc, logger *slog.Logger, concurrency int, metadataPrefix string, batchSize int, batchIndexColumn string, name string) (*Connector, error) {
	c := &Connector{
		Connector: component.NewConnector(ctx, cancelFn, logger, name),
		exec: func(inputReader io.Reader) (io.Reader, error) {
			return inputReader, nil
		},
		concurrentQueue:  cq.NewConcurrentQueueWithCancel(ctx, cancelFn, concurrency),
		metadataPrefix:   metadataPrefix,
		batchSize:        batchSize,
		batchIndexColumn: batchIndexColumn,
	}
	// initialize metrics related
	c.m = otel.GetMeter(c.Component(), c.Name())
	if err := c.initializeMetrics(); err != nil {
		return nil, errors.WithStack(err)
	}

	// set the connector function to process
	c.Connector.SetConnectorFunc(c.process)
	return c, nil
}

func (c *Connector) initializeMetrics() error {
	var err error
	if c.connectorBytes, err = c.m.Int64Counter(otel.ConnectorBytes, metric.WithDescription("The total number of bytes processed by the connector"), metric.WithUnit("bytes")); err != nil {
		return errors.WithStack(err)
	}
	if c.connectorBytesBucket, err = c.m.Int64Histogram(otel.ConnectorBytesBucket, metric.WithDescription("The total number of bytes processed by the connector in buckets"), metric.WithUnit("bytes")); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// SetExecFunc sets the execution function for the Connector.
func (c *Connector) SetExecFunc(execFunc ConnectorExecFunc) {
	c.exec = execFunc
}

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
			c.flush(bytes.NewReader(raw), inlets...)
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
			// copy the batch buffer to a new buffer to support concurrent processing
			var batchBufferCopy bytes.Buffer
			batchBufferCopy.Write(batchBuffer.Bytes())

			// submit the batch processing to the concurrent queue
			err := c.concurrentQueue.Submit(func() error {
				batchOutputReader, err := c.exec(&batchBufferCopy)
				if err != nil {
					return errors.WithStack(err)
				}
				return errors.WithStack(c.flush(batchOutputReader, inlets...))
			})
			if err != nil {
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
		if err := c.flush(batchOutputReader, inlets...); err != nil {
			return errors.WithStack(err)
		}
	}
	// wait for all queued functions to finish
	return errors.WithStack(c.concurrentQueue.Wait())
}

func (c *Connector) flush(r io.Reader, inlets ...flow.Inlet) error {
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

		// record the number of bytes processed by the connector
		c.connectorBytes.Add(c.Context(), int64(len(raw)))
		c.connectorBytesBucket.Record(c.Context(), int64(len(raw)))
	}
	return nil
}
