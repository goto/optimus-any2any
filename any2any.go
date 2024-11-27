package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/goto/optimus-any2any/internal/component"
	"github.com/goto/optimus-any2any/pkg/pipeline"
	"github.com/pkg/errors"
)

// any2any creates a pipeline from source to sink.
// TODO: load config in a single place, for now, log level not working on connector component (it defines in pipeline).
func any2any(l *slog.Logger, from, to string, envs []string) error {
	// graceful shutdown
	ctx, cancelFn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelFn()

	// create source
	source, err := component.GetSource(l, component.Type(from), envs...)
	if err != nil {
		return errors.WithStack(err)
	}
	// create sink
	sink, err := component.GetSink(l, component.Type(to), envs...)
	if err != nil {
		return errors.WithStack(err)
	}

	// initiate pipeline
	p := pipeline.NewSimplePipeline(l, source, sink)
	defer p.Close()

	select {
	// run pipeline until done
	case <-p.Run():
	// or until context is cancelled
	case <-ctx.Done():
	}

	return nil
}
