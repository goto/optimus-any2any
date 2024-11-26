package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/pkg/connector"
)

func any2any(from, to string) error {
	// load config
	_ = config.NewConfig()

	// graceful shutdown
	ctx, cancelFn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelFn()

	// initiate pipeline

	// create source
	source := getSource(ctx, from)
	// create sink
	sink := getSink(ctx, to)
	// connect
	connector.PassThrough(source, sink)
	// wait
	sink.Wait()

	return nil
}
