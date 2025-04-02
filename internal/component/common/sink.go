package common

import (
	"fmt"
	"iter"
	"log/slog"

	"github.com/goccy/go-json"

	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// Reader is an interface that defines a method to read data inside a sink.
type Reader interface {
	Read() iter.Seq[[]byte]
}

// RecordReader is an interface that defines a method to read data and unmarshal it into a model.Record.
type RecordReader interface {
	ReadRecord() iter.Seq2[model.Record, error]
}

// CommonSink is a common sink that implements the flow.Sink interface.
type CommonSink struct {
	*component.CoreSink
	*Common
}

var _ flow.Sink = (*CommonSink)(nil)
var _ Reader = (*CommonSink)(nil)
var _ RecordReader = (*CommonSink)(nil)

// NewCommonSink creates a new CommonSink.
func NewCommonSink(l *slog.Logger, name string, opts ...Option) *CommonSink {
	coreSink := component.NewCoreSink(l, name)
	c := &CommonSink{
		CoreSink: coreSink,
		Common:   NewCommon(coreSink.Core),
	}
	for _, opt := range opts {
		opt(c.Common)
	}
	return c
}

// Read reads the data from the sink.
func (c *CommonSink) Read() iter.Seq[[]byte] {
	return c.Common.Core.Out()
}

// ReadRecord reads the data from the sink and unmarshals it into a model.Record.
func (c *CommonSink) ReadRecord() iter.Seq2[model.Record, error] {
	return func(yield func(model.Record, error) bool) {
		for v := range c.Read() {
			var record model.Record
			if err := json.Unmarshal(v, &record); err != nil {
				c.Logger().Error(fmt.Sprintf("failed to unmarshal record: %s", err.Error()))
				yield(model.Record{}, errors.WithStack(err))
				break
			}
			if !yield(record, nil) {
				break
			}
		}
	}
}
