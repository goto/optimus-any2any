package pipeline

import (
	errs "errors"
	"log/slog"
	"sync"

	"github.com/goto/optimus-any2any/pkg/flow"
)

type MultiSinkConnector interface {
	Err() error
	Connect() flow.ConnectMultiSink
}

// MultiSinkPipeline is a pipeline that connects a source to multiple sinks.
type MultiSinkPipeline struct {
	logger    *slog.Logger
	source    flow.Source
	connector MultiSinkConnector
	sinks     []flow.Sink
}

// NewMultiSinkPipeline creates a new multi-sink pipeline.
func NewMultiSinkPipeline(l *slog.Logger, source flow.Source, connector MultiSinkConnector, sinks ...flow.Sink) *MultiSinkPipeline {
	p := &MultiSinkPipeline{
		logger:    l,
		source:    source,
		connector: connector,
		sinks:     sinks,
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
	connect := p.connector.Connect()
	go func() {
		defer close(done)
		connect(p.source, sinks...)
		p.groupSinkWait(p.sinks...)
		p.logger.Info("all sinks are done")
	}()
	return done
}

// Err returns the error from source or sink.
func (p *MultiSinkPipeline) Errs() []error {
	var errs []error
	if err := p.source.Err(); err != nil {
		errs = append(errs, err)
	}
	if err := p.connector.Err(); err != nil {
		errs = append(errs, err)
	}
	for _, sink := range p.sinks {
		if err := sink.Err(); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// Close closes source and sink gracefully.
func (p *MultiSinkPipeline) Close() error {
	var e error

	err := p.source.Close()
	e = errs.Join(e, err)

	for _, sink := range p.sinks {
		err = sink.Close()
		e = errs.Join(e, err)
	}

	return e
}

// groupSinkWait waits until all sinks are done.
func (p *MultiSinkPipeline) groupSinkWait(sinks ...flow.Sink) {
	var wg sync.WaitGroup
	for _, sink := range sinks {
		wg.Add(1)
		go func(s flow.Sink) {
			defer wg.Done()
			s.Wait()
		}(sink)
	}
	wg.Wait()
}
