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
)

func any2any(from, to string) error {
	// create logger
	l, err := logger.NewLogger(slog.LevelDebug.String())
	if err != nil {
		return err
	}

	// load config
	_ = config.NewConfig()

	// graceful shutdown
	ctx, cancelFn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelFn()

	// create source
	source := getSource(l, from)
	// create sink
	sink := getSink(ctx, to)
	// initiate pipeline
	p := pipeline.NewSimplePipeline(source, sink)
	defer p.Close()

	select {
	// run pipeline until done
	case <-p.Run():
	// or until context is cancelled
	case <-ctx.Done():
	}

	return nil
}
