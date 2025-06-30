package main

import (
	"context"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/goto/optimus-any2any/internal/component"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/internal/logger"
	pkgcomponent "github.com/goto/optimus-any2any/pkg/component"
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

	if cfg.EnablePprof {
		go func() {
			l.Info("starting pprof server on :6060")
			if err := http.ListenAndServe(":6060", nil); err != nil {
				l.Error(fmt.Sprintf("failed to start pprof server: %s", err.Error()))
			}
		}()
	}

	// graceful shutdown
	ctx, cancelFn := signalAwareContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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
		ctx, cancelCauseFn := context.WithCancelCause(ctx)
		defer cancelCauseFn(context.Canceled)
		// create source
		source, err := component.GetSource(ctx, cancelCauseFn, l, component.Type(strings.ToUpper(from)), cfg, envs...)
		if err != nil {
			return []error{errors.WithStack(err)}
		}
		// create sinks (multiple)
		var sinks []flow.Sink
		for _, t := range to {
			sink, err := component.GetSink(ctx, cancelCauseFn, l, component.Type(strings.ToUpper(t)), cfg, envs...)
			if err != nil {
				return []error{errors.WithStack(err)}
			}
			sinks = append(sinks, sink)
		}
		// get jq query for filtering / transforming data between source and sink
		jqCfg, err := config.ProcessorJQ(envs...)
		if err != nil {
			return []error{errors.WithStack(err)}
		}
		jqQuery, err := component.GetJQQuery(l, jqCfg)
		if err != nil {
			return []error{errors.WithStack(err)}
		}
		// run with pipeline
		p = pipeline.NewMultiSinkPipeline(l, source, pkgcomponent.NewConnector(ctx, cancelCauseFn, l, jqQuery, cfg.MetadataPrefix, jqCfg.BatchSize, jqCfg.BatchIndexColumn), sinks...)
	}
	defer p.Close()

	select {
	// run pipeline until done
	case <-p.Run():
		// if run is completed, it's less likely to return an error
		// but it's better to return them anyway
		l.Info("pipeline completed")
		return p.Errs()
	// or until context is cancelled
	case <-ctx.Done():
		l.Info("pipeline cancelled")
		if err := context.Cause(ctx); err != nil {
			return append(p.Errs(), errors.WithStack(err))
		}
		return p.Errs()
	}
}

// signalAwareContext creates a context that is aware of signals.
func signalAwareContext(parent context.Context, signals ...os.Signal) (context.Context, context.CancelFunc) {
	ctx, cancelWithCause := context.WithCancelCause(parent)
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, signals...)

	// start a goroutine to handle signals
	go func() {
		select {
		case sig := <-sigCh:
			cancelWithCause(fmt.Errorf("signal: %v", sig))
			signal.Stop(sigCh)
		case <-ctx.Done():
			signal.Stop(sigCh)
		}
	}()

	// return a standard CancelFunc that preserves the original behavior
	return ctx, func() {
		cancelWithCause(context.Canceled)
	}
}
