package pipeline

import (
	"github.com/goto/optimus-any2any/pkg/connector"
	"github.com/goto/optimus-any2any/pkg/flow"
)

// SimplePipeline is a simple pipeline that connects a source to a sink.
type SimplePipeline struct {
	source flow.Source
	sink   flow.Sink
}

// NewSimplePipeline creates a new simple pipeline.
func NewSimplePipeline(source flow.Source, sink flow.Sink) *SimplePipeline {
	return &SimplePipeline{
		source: source,
		sink:   sink,
	}
}

// Run runs the pipeline by connecting source to sink.
func (p *SimplePipeline) Run() <-chan uint8 {
	done := make(chan uint8)
	go func() {
		defer close(done)
		connector.PassThrough(p.source, p.sink)
		p.sink.Wait()
	}()
	return done
}

// Close closes source and sink gracefully.
func (p *SimplePipeline) Close() {
	p.source.Close()
	p.sink.Close()
}
