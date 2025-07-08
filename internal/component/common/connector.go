package common

import (
	"bufio"
	"bytes"
	"context"
	"io"
	"log/slog"
	"sync/atomic"

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
	concurrency      int
	concurrentQueue  cq.ConcurrentQueue
	metadataPrefix   string
	batchSize        int
	batchIndexColumn string

	// metrics related
	m                    metric.Meter
	connectorBytes       metric.Int64Counter
	connectorBytesBucket metric.Int64Histogram
	concurrentLimits     atomic.Int64
	concurrentCount      atomic.Int64
}

func NewConnector(ctx context.Context, cancelFn context.CancelCauseFunc, logger *slog.Logger, concurrency int, metadataPrefix string, batchSize int, batchIndexColumn string, name string) (*Connector, error) {
	c := &Connector{
		Connector: component.NewConnector(ctx, cancelFn, logger, name),
		exec: func(inputReader io.Reader) (io.Reader, error) {
			return inputReader, nil
		},
		concurrency:      concurrency,
		metadataPrefix:   metadataPrefix,
		batchSize:        batchSize,
		batchIndexColumn: batchIndexColumn,
	}
	// initialize metrics related
	c.m = otel.GetMeter(c.Component(), c.Name())
	if err := c.initializeMetrics(); err != nil {
		return nil, errors.WithStack(err)
	}

	// initialize concurrent queue
	c.concurrentQueue = cq.NewConcurrentQueueWithCancel(ctx, cancelFn, concurrency)
	c.concurrentLimits.Add(int64(concurrency))

	// set the connector function to process
	c.Connector.SetConnectorFunc(c.process)
	return c, nil
}

func (c *Connector) initializeMetrics() error {
	var err error

	// non-observable metrics
	if c.connectorBytes, err = c.m.Int64Counter(otel.ConnectorBytes, metric.WithDescription("The total number of bytes processed by the connector"), metric.WithUnit("bytes")); err != nil {
		return errors.WithStack(err)
	}
	if c.connectorBytesBucket, err = c.m.Int64Histogram(otel.ConnectorBytesBucket, metric.WithDescription("The total number of bytes processed by the connector in buckets"), metric.WithUnit("bytes")); err != nil {
		return errors.WithStack(err)
	}

	// observable metrics
	c.concurrentLimits = atomic.Int64{}
	c.concurrentCount = atomic.Int64{}
	processLimits, err := c.m.Int64ObservableGauge(otel.ConnectorProcessLimits, metric.WithDescription("The total number of concurrent processes allowed for the sink"))
	if err != nil {
		return errors.WithStack(err)
	}
	processCount, err := c.m.Int64ObservableGauge(otel.ConnectorProcessCount, metric.WithDescription("The total number of processes running for the sink"))
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = c.m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(processLimits, c.concurrentLimits.Load())
		o.ObserveInt64(processCount, c.concurrentCount.Load())
		return nil
	}, processLimits, processCount)
	return errors.WithStack(err)
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
			}, func() error {
				c.concurrentCount.Add(-1)
				return nil
			})
			if err != nil {
				return errors.WithStack(err)
			}
			// increment the concurrent count
			c.concurrentCount.Add(1)

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
	// wait for all queued functions to finish and release concurrent limits
	defer func() {
		c.concurrentLimits.Add(-int64(c.concurrency))
	}()
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
