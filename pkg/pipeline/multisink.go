package pipeline

import (
	"errors"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// MultiSinkPipeline is a pipeline that connects a source to multiple sinks.
type MultiSinkPipeline struct {
	logger  *slog.Logger
	source  flow.Source
	connect flow.ConnectMultiSink
	sinks   []flow.Sink
}

// NewMultiSinkPipeline creates a new multi-sink pipeline.
func NewMultiSinkPipeline(l *slog.Logger, source flow.Source, connect flow.ConnectMultiSink, sinks ...flow.Sink) *MultiSinkPipeline {
	p := &MultiSinkPipeline{
		logger:  l,
		source:  source,
		connect: connect,
		sinks:   sinks,
	}
	return p
}

// Run runs the pipeline by connecting source to sink.
func (p *MultiSinkPipeline) Run() <-chan uint8 {
	done := make(chan uint8)
	// Convert []flow.Sink to []flow.Inlet
	sinks := make([]flow.Inlet, len(p.sinks))
	for i, sink := range p.sinks {
		sinks[i] = sink
	}
	go func() {
		defer close(done)
		p.connect(p.source, sinks...)
		for _, sink := range p.sinks {
			sink.Wait()
		}
	}()
	return done
}

// Err returns the error from source or sink.
func (p *MultiSinkPipeline) Err() error {
	var errs error
	errs = errors.Join(errs, p.source.Err())
	for _, sink := range p.sinks {
		errs = errors.Join(errs, sink.Err())
	}
	return errs
}

// Close closes source and sink gracefully.
func (p *MultiSinkPipeline) Close() {
	p.source.Close()
	for _, sink := range p.sinks {
		sink.Close()
	}
}
