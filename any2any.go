package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/goto/optimus-any2any/internal/component"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/goto/optimus-any2any/pkg/connector"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/goto/optimus-any2any/pkg/pipeline"
	"github.com/pkg/errors"
)

// any2any creates a pipeline from source to sink.
func any2any(from string, to []string, envs []string) error {
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
	source, err := component.GetSource(ctx, l, component.Type(strings.ToUpper(from)), cfg, envs...)
	if err != nil {
		return errors.WithStack(err)
	}
	// create sinks (multiple)
	var sinks []flow.Sink
	for _, t := range to {
		sink, err := component.GetSink(ctx, l, component.Type(strings.ToUpper(t)), cfg, envs...)
		if err != nil {
			return errors.WithStack(err)
		}
		sinks = append(sinks, sink)
	}
	// get jq query for filtering / transforming data between source and sink
	jqQuery, err := component.GetJQQuery(l, envs...)
	if err != nil {
		return errors.WithStack(err)
	}

	// initiate pipeline
	var p interface {
		Run() <-chan uint8
		Err() error
		Close()
	}
	// create pipeline based on number of sinks
	if len(sinks) == 1 {
		p = pipeline.NewSimplePipeline(l, source, connector.PassThroughWithJQ(l, jqQuery), sinks[0])
	} else {
		p = pipeline.NewMultiSinkPipeline(l, source, connector.FanOutWithJQ(l, jqQuery), sinks...)
	}
	defer p.Close()

	select {
	// run pipeline until done
	case <-p.Run():
		if err := p.Err(); err != nil {
			return errors.WithStack(err)
		}
	// or until context is cancelled
	case <-ctx.Done():
	}

	return nil
}
