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
func any2any(from string, to []string, noPipeline bool, envs []string) []error {
	// load config
	cfg, err := config.NewConfig(envs...)
	if err != nil {
		return []error{errors.WithStack(err)}
	}

	// set up logger
	l, err := logger.NewLogger(cfg.LogLevel)
	if err != nil {
		return []error{errors.WithStack(err)}
	}

	// graceful shutdown
	ctx, cancelFn := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancelFn()

	var p interface {
		Run() <-chan uint8
		Errs() []error
		Close() error
	}

	if noPipeline {
		directSourceSink, err := component.GetDirectSourceSink(ctx, l, component.Type(strings.ToUpper(from)), component.Type(strings.ToUpper(to[0])), cfg, envs...)
		if err != nil {
			return []error{errors.WithStack(err)}
		}
		// run without pipeline
		p = pipeline.NewNoPipeline(l, directSourceSink)
	} else {
		// create source
		source, err := component.GetSource(ctx, l, component.Type(strings.ToUpper(from)), cfg, envs...)
		if err != nil {
			return []error{errors.WithStack(err)}
		}
		// create sinks (multiple)
		var sinks []flow.Sink
		for _, t := range to {
			sink, err := component.GetSink(ctx, l, component.Type(strings.ToUpper(t)), cfg, envs...)
			if err != nil {
				return []error{errors.WithStack(err)}
			}
			sinks = append(sinks, sink)
		}
		// get jq query for filtering / transforming data between source and sink
		jqQuery, err := component.GetJQQuery(l, envs...)
		if err != nil {
			return []error{errors.WithStack(err)}
		}
		// run with pipeline
		p = pipeline.NewMultiSinkPipeline(l, source, connector.FanOutWithJQ(l, jqQuery), sinks...)
	}
	defer p.Close()

	select {
	// run pipeline until done
	case <-p.Run():
		return p.Errs()
	// or until context is cancelled
	case <-ctx.Done():
	}

	return nil
}
