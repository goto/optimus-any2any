package component

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/ext/io"
	"github.com/goto/optimus-any2any/ext/maxcompute"
	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type Type string

const (
	MC   Type = "MC"
	FILE Type = "FILE"
	IO   Type = "IO"
)

// GetSource returns a source based on the given type.
// It will return an error if the source is unknown.
func GetSource(l *slog.Logger, source Type, cfg *config.Config, envs ...string) (flow.Source, error) {
	// set up options
	opts := getOpts(cfg)

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
func GetSink(l *slog.Logger, sink Type, cfg *config.Config, envs ...string) (flow.Sink, error) {
	// set up options
	opts := getOpts(cfg)

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

// getOpts returns options based on the given config.
func getOpts(cfg *config.Config) []option.Option {
	return []option.Option{
		option.SetupLogger(cfg.LogLevel),
		option.SetupOtelSDK(cfg.OtelCollectorGRPCEndpoint, cfg.OtelAttributes),
		option.SetupBufferSize(cfg.BufferSize),
	}
}
