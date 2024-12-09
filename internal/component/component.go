package component

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/ext/io"
	"github.com/goto/optimus-any2any/ext/maxcompute"
	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/pkg/connector"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type (
	Type          string
	ProcessorType string
)

const (
	MC   Type = "MC"
	FILE Type = "FILE"
	IO   Type = "IO"
)

// GetSource returns a source based on the given type.
// It will return an error if the source is unknown.
func GetSource(ctx context.Context, l *slog.Logger, source Type, cfg *config.Config, envs ...string) (flow.Source, error) {
	// set up options
	opts := getOpts(ctx, cfg)

	// create source based on type
	switch source {
	case MC:
	case FILE:
		sourceCfg, err := config.SourceFile(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return file.NewSource(l, sourceCfg.Path, opts...)
	case IO:
	}
	return nil, fmt.Errorf("source: unknown source: %s", source)
}

// GetSink returns a sink based on the given type.
// It will return an error if the sink is unknown.
func GetSink(ctx context.Context, l *slog.Logger, sink Type, cfg *config.Config, envs ...string) (flow.Sink, error) {
	// set up options
	opts := getOpts(ctx, cfg)

	// create sink based on type
	switch sink {
	case MC:
		sinkCfg, err := config.SinkMC(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return maxcompute.NewSink(l, sinkCfg.ServiceAccount, sinkCfg.DestinationTableID, opts...)
	case FILE:
	case IO:
		return io.NewSink(l), nil
	}
	return nil, fmt.Errorf("sink: unknown sink: %s", sink)
}

// GetConnector returns a connector to connect source to sink.
// For now it only supports PassThrough and JQ processor.
// TODO: refactor to support more processors.
func GetConnector(ctx context.Context, l *slog.Logger, cfg *config.Config, envs ...string) (flow.Connect, error) {
	processorCfg, err := config.ProcessorJQ(envs...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if processorCfg.Query == "" {
		return connector.PassThrough(l), nil
	}
	return connector.JQProcessor(l, processorCfg.Query), nil
}

// getOpts returns options based on the given config.
func getOpts(ctx context.Context, cfg *config.Config) []option.Option {
	return []option.Option{
		option.SetupLogger(cfg.LogLevel),
		option.SetupOtelSDK(ctx, cfg.OtelCollectorGRPCEndpoint, cfg.OtelAttributes),
		option.SetupBufferSize(cfg.BufferSize),
	}
}
