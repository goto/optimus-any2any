package common

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/internal/otel"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/metric"
)

// Source is a complete interface that defines source component.
type Source interface {
	// fundamental
	flow.Source
	// must have
	component.Setter
	component.Getter
	component.Registrants
	// source specific
	Sender
	RecordSender
	// helpers
	RecordHelper
	Retrier
	DryRunabler
	ConcurrentLimiter
	InstrumentationGetter
}

// Sender is an interface that defines a method to send data to a source.
type Sender interface {
	Send(v []byte)
}

// RecordWriter is an interface that defines a method to write records.
type RecordSender interface {
	SendRecord(*model.Record) error
}

// CommonSource is a common source that implements the flow.Source interface.
type CommonSource struct {
	*component.CoreSource
	*Common
	recordCounter atomic.Int64

	// metrics related
	recordCount       metric.Int64Counter
	recordBytes       metric.Int64Counter
	recordBytesBucket metric.Int64Histogram
}

var _ Source = (*CommonSource)(nil)

// NewCommonSource creates a new CommonSource.
func NewCommonSource(ctx context.Context, cancelFn context.CancelCauseFunc, l *slog.Logger, name string, opts ...Option) (*CommonSource, error) {
	coreSource := component.NewCoreSource(ctx, cancelFn, l, name)
	common, err := NewCommon(coreSource.Core)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	c := &CommonSource{
		CoreSource:    coreSource,
		Common:        common,
		recordCounter: atomic.Int64{},
	}
	for _, opt := range opts {
		opt(c.Common)
	}
	if err := c.initializeMetrics(); err != nil {
		return nil, errors.WithStack(err)
	}
	return c, nil
}

func (c *CommonSource) initializeMetrics() error {
	err := c.Common.initializeMetrics()
	if err != nil {
		return errors.WithStack(err)
	}

	// non-observable metrics
	c.recordCount, err = c.Meter().Int64Counter(otel.SourceRecordCount, metric.WithDescription("The total number of data sent"))
	if err != nil {
		return errors.WithStack(err)
	}
	c.recordBytes, err = c.Meter().Int64Counter(otel.SourceRecordBytes, metric.WithDescription("The total number of bytes sent"), metric.WithUnit("bytes"))
	if err != nil {
		return errors.WithStack(err)
	}
	c.recordBytesBucket, err = c.Meter().Int64Histogram(otel.SourceRecordBytesBucket, metric.WithDescription("The total number of bytes sent in buckets"), metric.WithUnit("bytes"))
	if err != nil {
		return errors.WithStack(err)
	}

	// observable metrics
	processLimits, err := c.Meter().Int64ObservableGauge(otel.SourceProcessLimits, metric.WithDescription("The total number of concurrent processes allowed for the source"))
	if err != nil {
		return errors.WithStack(err)
	}
	processCount, err := c.Meter().Int64ObservableGauge(otel.SourceProcessCount, metric.WithDescription("The total number of processes running for the source"))
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = c.Meter().RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(processLimits, c.concurrentLimits.Load())
		o.ObserveInt64(processCount, c.concurrentCount.Load())
		return nil
	})
	return errors.WithStack(err)
}

// Send sends the given data to the source.
// This is a wrapper around the CoreSource's Send method.
func (c *CommonSource) Send(v []byte) {
	c.Common.Core.In(v)
}

// SendRecord sends a record to the source.
// It marshals the record into JSON format and then sends it using the Send method.
func (c *CommonSource) SendRecord(record *model.Record) error {
	if record == nil {
		return nil
	}

	// if record is not a specialized metadata record,
	// we set the record index as a metadata
	if !c.IsSpecializedMetadataRecord(record) {
		// set the record index as a metadata
		record.Set(c.metadataPrefix+"record_index", c.recordCounter.Load())
		c.recordCounter.Add(1)
	}

	// marshal the record to JSON
	raw, err := json.Marshal(record)
	if err != nil {
		return errors.WithStack(err)
	}

	// if record is not a specialized metadata record,
	// we increment the record related metrics
	if !c.IsSpecializedMetadataRecord(record) {
		c.recordCount.Add(c.Context(), 1)
		c.recordBytes.Add(c.Context(), int64(len(raw)))
		c.recordBytesBucket.Record(c.Context(), int64(len(raw)))
	}

	c.Send(raw)
	return nil
}
