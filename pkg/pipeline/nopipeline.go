package pipeline

import (
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// NoPipeline is a special pipeline that run the source and sink without data flow.
type NoPipeline struct {
	logger   *slog.Logger
	executor flow.NoFlow
	errs     []error
}

// NewNoPipeline creates a new no-pipeline.
func NewNoPipeline(l *slog.Logger, executor flow.NoFlow) *NoPipeline {
	return &NoPipeline{
		logger:   l,
		executor: executor,
		errs:     []error{},
	}
}

// Run runs the no-pipeline.
func (p *NoPipeline) Run() <-chan uint8 {
	done := make(chan uint8)
	go func() {
		defer close(done)
		p.errs = p.executor.Run()
	}()
	return done
}

// Errs returns the errors from the no-pipeline.
func (p *NoPipeline) Errs() []error {
	return p.errs
}

// Close closes the no-pipeline.
func (p *NoPipeline) Close() {
	p.executor.Close()
}
