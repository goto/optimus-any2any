package main

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/ext/io"
	"github.com/goto/optimus-any2any/ext/maxcompute"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/pkg/flow"
)

// TODO: restructure the code to make it more modular and testable.

type Component string

const (
	MC   Component = "mc"
	FILE Component = "file"
	IO   Component = "io"
)

// TODO: add more source components.
func getSource(l *slog.Logger, cfg *config.Config, source Component) (flow.Source, error) {
	// opts := []flow.Option{
	// 	component.SetLogger(cfg.LogLevel),
	// 	component.SetOtelSDK(cfg.OtelCollectorGRPCEndpoint, cfg.OtelAttributes),
	// 	flow.WithBufferSize(cfg.BufferSize),
	// }

	switch source {
	case MC:
		return nil, nil
	case FILE:
		// sourceCfg := config.SourceFile(cfg)
		// return file.NewSource(l, sourceCfg.Path, opts...)
		return file.NewSource(l, "in.txt")
	case IO:
	}
	return nil, fmt.Errorf("source: unknown source: %s", source)
}

// TODO: add more sink components.
func getSink(l *slog.Logger, cfg *config.Config, sink Component) (flow.Sink, error) {
	switch sink {
	case MC:
		return maxcompute.NewSink(l, "", "")
	case FILE:
	case IO:
		return io.NewSink(l), nil
	}
	return nil, fmt.Errorf("sink: unknown sink: %s", sink)
}
