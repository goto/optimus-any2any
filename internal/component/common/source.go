package common

import (
	"context"
	"log/slog"
	"sync/atomic"

	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
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
	return c, nil
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
		c.recordCount.Add(c.Context(), 1, c.attributesOpt)
		c.recordBytes.Add(c.Context(), int64(len(raw)), c.attributesOpt)
		c.recordBytesBucket.Record(c.Context(), int64(len(raw)), c.attributesOpt)
	}

	c.Send(raw)
	return nil
}
