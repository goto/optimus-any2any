package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/goto/optimus-any2any/internal/component"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/goto/optimus-any2any/pkg/pipeline"
	"github.com/pkg/errors"
)

// any2any creates a pipeline from source to sink.
func any2any(from, to string, envs []string) error {
	// load config
	cfg, err := config.NewConfig(envs...)
	if err != nil {
		return errors.WithStack(err)
	}

	// set up logger
	l, err := logger.NewLogger(cfg.LogLevel)
	if err != nil {
		return errors.WithStack(err)
	}

	// graceful shutdown
	ctx, cancelFn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelFn()

	// create source
	source, err := component.GetSource(l, component.Type(from), cfg, envs...)
	if err != nil {
		return errors.WithStack(err)
	}
	// create sink
	sink, err := component.GetSink(l, component.Type(to), cfg, envs...)
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
