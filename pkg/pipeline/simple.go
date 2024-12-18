package pipeline

import (
	"errors"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// SimplePipeline is a simple pipeline that connects a source to a sink.
type SimplePipeline struct {
	logger  *slog.Logger
	source  flow.Source
	connect flow.Connect
	sink    flow.Sink
}

// NewSimplePipeline creates a new simple pipeline.
func NewSimplePipeline(l *slog.Logger, source flow.Source, connect flow.Connect, sink flow.Sink) *SimplePipeline {
	p := &SimplePipeline{
		logger:  l,
		source:  source,
		connect: connect,
		sink:    sink,
	}
	return p
}

// Run runs the pipeline by connecting source to sink.
func (p *SimplePipeline) Run() <-chan uint8 {
	done := make(chan uint8)
	go func() {
		defer close(done)
		p.connect(p.source, p.sink)
		p.sink.Wait()
	}()
	return done
}

// Err returns the error from source or sink.
func (p *SimplePipeline) Err() error {
	var errs error
	return errors.Join(errs, p.source.Err(), p.sink.Err())
}

// Close closes source and sink gracefully.
func (p *SimplePipeline) Close() {
	p.source.Close()
	p.sink.Close()
}
