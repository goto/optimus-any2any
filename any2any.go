package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/goto/optimus-any2any/pkg/pipeline"
	"github.com/pkg/errors"
)

func any2any(from, to string) error {
	// create logger
	l, err := logger.NewLogger(slog.LevelDebug.String())
	if err != nil {
		return errors.WithStack(err)
	}

	// load config
	cfg := config.NewConfig()

	// graceful shutdown
	ctx, cancelFn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelFn()

	// create source
	source, err := getSource(l, cfg, Component(from))
	if err != nil {
		return errors.WithStack(err)
	}
	// create sink
	sink, err := getSink(l, cfg, Component(to))
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
