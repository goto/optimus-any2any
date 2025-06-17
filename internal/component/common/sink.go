package common

import (
	"context"
	"fmt"
	"iter"
	"log/slog"

	"github.com/PaesslerAG/gval"
	"github.com/PaesslerAG/jsonpath"
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
	DataModifier
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

// DataModifier is an interface that defines a method to modify the data
// just before sending it to the sink.
type DataModifier interface {
	JSONPathSelector(data []byte, path string) ([]byte, error)
	Compression(compressionType, compressionPassword string, filepaths []string) ([]string, error)
}

// CommonSink is a common sink that implements the flow.Sink interface.
type commonSink struct {
	*component.CoreSink
	*Common
	pathToEvaluator map[string]gval.Evaluable
}

var _ Sink = (*commonSink)(nil)

// NewCommonSink creates a new CommonSink.
func NewCommonSink(ctx context.Context, cancelFn context.CancelCauseFunc, l *slog.Logger, name string, opts ...Option) *commonSink {
	coreSink := component.NewCoreSink(ctx, cancelFn, l, name)
	c := &commonSink{
		CoreSink:        coreSink,
		Common:          NewCommon(coreSink.Core),
		pathToEvaluator: make(map[string]gval.Evaluable),
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

// Compression compresses the given filepaths using the specified compression type and password.
// It's useful for compressing files before sending them to the sink.
func (c *commonSink) Compression(compressionType, compressionPassword string, filepaths []string) ([]string, error) {
	if compressionType == "" || len(filepaths) == 0 {
		return filepaths, nil
	}
	return nil, errors.New("compression is not implemented yet")

	// // TODO: Implement the compressor based on the compression type
	// compressor, err := NewCompressor(compressionType, compressionPassword)
	// if err != nil {
	// 	return nil, errors.WithStack(err)
	// }

	// compressedFilepaths, err := compressor.Compress(filepaths)
	// if err != nil {
	// 	return nil, errors.WithStack(err)
	// }
	// return compressedFilepaths, nil
}
