package pipeline

import (
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/connector"
	"github.com/goto/optimus-any2any/pkg/flow"
)

// SimplePipeline is a simple pipeline that connects a source to a sink.
type SimplePipeline struct {
	logger    *slog.Logger
	source    flow.Source
	connect   flow.Connect
	processor flow.Processor
	sink      flow.Sink
}

// NewSimplePipeline creates a new simple pipeline.
func NewSimplePipeline(l *slog.Logger, source flow.Source, sink flow.Sink, processors ...flow.Processor) *SimplePipeline {
	p := &SimplePipeline{
		logger:  l,
		source:  source,
		connect: connector.PassThrough(l),
		sink:    sink,
	}
	if len(processors) > 0 && processors[0] != nil {
		p.processor = processors[0]
	}
	return p
}

// Run runs the pipeline by connecting source to sink.
func (p *SimplePipeline) Run() <-chan uint8 {
	done := make(chan uint8)
	go func() {
		defer close(done)
		if p.processor != nil {
			p.connect(p.source, p.processor)
			p.connect(p.processor, p.sink)
		} else {
			p.connect(p.source, p.sink)
		}
		p.sink.Wait()
	}()
	return done
}

// Close closes source and sink gracefully.
func (p *SimplePipeline) Close() {
	p.source.Close()
	p.sink.Close()
}
