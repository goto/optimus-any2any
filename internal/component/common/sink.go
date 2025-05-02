package common

import (
	"context"
	"fmt"
	"iter"
	"log/slog"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// Sink is a complete interface that defines sink component.
type Sink interface {
	// fundamental
	flow.Sink
	// must have
	component.Setter
	component.Getter
	component.Registrants
	// sink specific
	Reader
	RecordReader
	// helpers
	RecordHelper
	Retrier
	DryRunabler
	ConcurrentLimiter
}

// Reader is an interface that defines a method to read data inside a sink.
type Reader interface {
	Read() iter.Seq[[]byte]
}

// RecordReader is an interface that defines a method to read data and unmarshal it into a model.Record.
type RecordReader interface {
	ReadRecord() iter.Seq2[*model.Record, error]
}

// CommonSink is a common sink that implements the flow.Sink interface.
type commonSink struct {
	*component.CoreSink
	*Common
}

var _ Sink = (*commonSink)(nil)

// NewCommonSink creates a new CommonSink.
func NewCommonSink(ctx context.Context, cancelFn context.CancelCauseFunc, l *slog.Logger, name string, opts ...Option) *commonSink {
	coreSink := component.NewCoreSink(ctx, cancelFn, l, name)
	c := &commonSink{
		CoreSink: coreSink,
		Common:   NewCommon(coreSink.Core),
	}
	for _, opt := range opts {
		opt(c.Common)
	}
	return c
}

// Read reads the data from the sink.
func (c *commonSink) Read() iter.Seq[[]byte] {
	return c.Common.Core.Out()
}

// ReadRecord reads the data from the sink and unmarshals it into a model.Record.
func (c *commonSink) ReadRecord() iter.Seq2[*model.Record, error] {
	return func(yield func(*model.Record, error) bool) {
		for v := range c.Common.Core.Out() {
			var record model.Record
			if err := json.Unmarshal(v, &record); err != nil {
				c.Logger().Error(fmt.Sprintf("failed to unmarshal record: %s", err.Error()))
				yield(nil, errors.WithStack(err))
				break
			}
			if !yield(&record, nil) {
				break
			}
		}
	}
}
