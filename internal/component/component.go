package component

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/ext/gmail"
	"github.com/goto/optimus-any2any/ext/io"
	"github.com/goto/optimus-any2any/ext/maxcompute"
	"github.com/goto/optimus-any2any/ext/oss"
	"github.com/goto/optimus-any2any/ext/salesforce"
	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type (
	Type          string
	ProcessorType string
)

const (
	MC    Type = "MC"
	FILE  Type = "FILE"
	IO    Type = "IO"
	SF    Type = "SF"
	OSS   Type = "OSS"
	GMAIL Type = "GMAIL"
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
	case SF:
		sourceCfg, err := config.SourceSalesforce(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return salesforce.NewSource(l,
			sourceCfg.Host, sourceCfg.User, sourceCfg.Pass, sourceCfg.Token,
			sourceCfg.SOQLFilePath, sourceCfg.ColumnMappingFilePath, opts...)
	case GMAIL:
		sourceCfg, err := config.SourceGmail(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return gmail.NewSource(ctx, l, sourceCfg.Token, sourceCfg.Filter,
			sourceCfg.ExtractorSource, sourceCfg.ExtractorPattern, sourceCfg.ExtractorFileFormat,
			sourceCfg.FilenameColumn, sourceCfg.ColumnMappingFilePath, opts...)
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
		return maxcompute.NewSink(l, sinkCfg.ServiceAccount, sinkCfg.DestinationTableID, sinkCfg.LoadMethod, sinkCfg.UploadMode, opts...)
	case FILE:
	case IO:
		return io.NewSink(l), nil
	case OSS:
		sinkCfg, err := config.SinkOSS(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return oss.NewSink(ctx, l, sinkCfg.ServiceAccount,
			sinkCfg.DestinationBucketPath, sinkCfg.FilenamePattern,
			int64(sinkCfg.BatchSize), sinkCfg.EnableOverwrite, opts...)

	}
	return nil, fmt.Errorf("sink: unknown sink: %s", sink)
}

// GetJQQuery returns a jq query based on the given environment variables.
// jq query is used for JSON transformation, it acts as a filter and map for JSON data.
func GetJQQuery(l *slog.Logger, envs ...string) (string, error) {
	jqCfg, err := config.ProcessorJQ(envs...)
	if err != nil {
		return "", errors.WithStack(err)
	}
	return jqCfg.Query, nil
}

// getOpts returns options based on the given config.
func getOpts(ctx context.Context, cfg *config.Config) []option.Option {
	return []option.Option{
		option.SetupLogger(cfg.LogLevel),
		option.SetupOtelSDK(ctx, cfg.OtelCollectorGRPCEndpoint, cfg.OtelAttributes),
		option.SetupBufferSize(cfg.BufferSize),
	}
}
