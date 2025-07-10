package common

import (
	"context"
	"iter"
	"log/slog"

	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"
	"github.com/goccy/go-json"
	"go.opentelemetry.io/otel/metric"

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
	DataModifier
	Retrier
	DryRunabler
	ConcurrentLimiter
	InstrumentationGetter
}

// Reader is an interface that defines a method to read data inside a sink.
type Reader interface {
	Read() iter.Seq[[]byte]
}

// RecordReader is an interface that defines a method to read data and unmarshal it into a model.Record.
type RecordReader interface {
	ReadRecord() iter.Seq2[*model.Record, error]
}

// DataModifier is an interface that defines a method to modify the data
// just before sending it to the sink.
type DataModifier interface {
	JSONPathSelector(data []byte, path string) ([]byte, error)
}

// CommonSink is a common sink that implements the flow.Sink interface.
type commonSink struct {
	*component.CoreSink
	*Common
	pathToEvaluator map[string]gval.Evaluable

	// metrics related
	recordCount       metric.Int64Counter
	recordBytes       metric.Int64Counter
	recordBytesBucket metric.Int64Histogram
}

var _ Sink = (*commonSink)(nil)

// NewCommonSink creates a new CommonSink.
func NewCommonSink(ctx context.Context, cancelFn context.CancelCauseFunc, l *slog.Logger, name string, opts ...Option) (*commonSink, error) {
	coreSink := component.NewCoreSink(ctx, cancelFn, l, name)
	common, err := NewCommon(coreSink.Core)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	c := &commonSink{
		CoreSink:        coreSink,
		Common:          common,
		pathToEvaluator: make(map[string]gval.Evaluable),
	}
	for _, opt := range opts {
		opt(c.Common)
	}
	return c, nil
}

// Read reads the data from the sink.
func (c *commonSink) Read() iter.Seq[[]byte] {
	return c.Common.Core.Out()
}

// ReadRecord reads the data from the sink and unmarshals it into a model.Record.
func (c *commonSink) ReadRecord() iter.Seq2[*model.Record, error] {
	return func(yield func(*model.Record, error) bool) {
		for v := range c.Out() {
			var record model.Record
			if err := json.Unmarshal(v, &record); err != nil {
				yield(nil, errors.WithStack(err))
				break
			}

			// if record is not a specialized metadata record,
			// we increment the record related metrics
			if !c.IsSpecializedMetadataRecord(&record) {
				c.recordCount.Add(c.Context(), 1)
				c.recordBytes.Add(c.Context(), int64(len(v)))
				c.recordBytesBucket.Record(c.Context(), int64(len(v)))
			}

			if !yield(&record, nil) {
				break
			}
		}
	}
}

// JSONPathSelector selects a JSON path from the data and returns the value as a byte slice.
func (c *commonSink) JSONPathSelector(data []byte, path string) ([]byte, error) {
	if path == "" {
		return data, nil
	}

	if _, ok := c.pathToEvaluator[path]; !ok {
		// compile the json path selector
		builder := gval.Full(jsonpath.PlaceholderExtension())
		e, err := builder.NewEvaluable(path)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		c.pathToEvaluator[path] = e
	}

	// unmarshal the data to an interface{}
	var d interface{}
	if err := json.Unmarshal(data, &d); err != nil {
		return nil, errors.WithStack(err)
	}

	// evaluate the json path selector
	evaluator := c.pathToEvaluator[path]
	result, err := evaluator(c.Context(), d)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// marshal the result back to JSON
	raw, err := json.Marshal(result)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return raw, nil
}

// ReadRecordFromOutlet converts a flow.Outlet to an iter.Seq2 that yields model.Record.
// It reads the data from the outlet, unmarshals it into a model.Record, and yields it.
// If an error occurs during unmarshalling, it yields nil and the error.
// TODO: refactor this
func ReadRecordFromOutlet(outlet flow.Outlet) iter.Seq2[*model.Record, error] {
	return func(yield func(*model.Record, error) bool) {
		for v := range outlet.Out() {
			var record model.Record
			if err := json.Unmarshal(v, &record); err != nil {
				yield(nil, errors.WithStack(err))
				break
			}
			if !yield(&record, nil) {
				break
			}
		}
	}
}
