package pipeline

import (
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

type SimplePipelineV2 struct {
	logger  *slog.Logger
	source  flow.SourceV2
	connect flow.ConnectV2
	sink    flow.SinkV2
}

func NewSimplePipelineV2(l *slog.Logger, source flow.SourceV2, connect flow.ConnectV2, sink flow.SinkV2) *SimplePipelineV2 {
	p := &SimplePipelineV2{
		logger:  l,
		source:  source,
		connect: connect,
		sink:    sink,
	}
	return p
}

func (p *SimplePipelineV2) Run() <-chan uint8 {
	done := make(chan uint8)
	go func() {
		defer close(done)
		p.connect(p.source, p.sink)
		p.sink.Wait()
	}()
	return done
}

func (p *SimplePipelineV2) Errs() []error {
	var errs []error
	if err := p.source.Err(); err != nil {
		errs = append(errs, err)
	}
	if err := p.sink.Err(); err != nil {
		errs = append(errs, err)
	}
	return errs
}

func (p *SimplePipelineV2) Close() error {
	p.source.Close()
	p.sink.Close()
	p.logger.Debug("pipeline closed")
	return nil
}
