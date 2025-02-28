package component

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/ext/direct"
	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/ext/gmail"
	"github.com/goto/optimus-any2any/ext/io"
	"github.com/goto/optimus-any2any/ext/kafka"
	"github.com/goto/optimus-any2any/ext/maxcompute"
	"github.com/goto/optimus-any2any/ext/oss"
	"github.com/goto/optimus-any2any/ext/salesforce"
	"github.com/goto/optimus-any2any/ext/sftp"
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
	KAFKA Type = "KAFKA"
	FILE  Type = "FILE"
	IO    Type = "IO"
	SF    Type = "SF"
	OSS   Type = "OSS"
	SFTP  Type = "SFTP"
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
		sourceCfg, err := config.SourceMC(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return maxcompute.NewSource(l, sourceCfg.Credentials, sourceCfg.QueryFilePath, sourceCfg.ExecutionProject, opts...)
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
	case OSS:
		sourceCfg, err := config.SourceOSS(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return oss.NewSource(ctx, l, sourceCfg.Credentials, sourceCfg.SourceURI, sourceCfg.FileFormat, sourceCfg.CSVDelimiter, sourceCfg.ColumnMappingFilePath, opts...)
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
		return maxcompute.NewSink(l, sinkCfg.Credentials, sinkCfg.DestinationTableID, sinkCfg.LoadMethod, sinkCfg.UploadMode, opts...)
	case FILE:
		sinkCfg, err := config.SinkFile(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return file.NewSink(l, sinkCfg.DestinationURI, opts...)
	case IO:
		return io.NewSink(l), nil
	case OSS:
		sinkCfg, err := config.SinkOSS(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return oss.NewSink(ctx, l, sinkCfg.Credentials,
			sinkCfg.DestinationURI,
			sinkCfg.GroupBy, sinkCfg.GroupBatchSize, sinkCfg.GroupColumnName,
			sinkCfg.FilenamePattern, sinkCfg.EnableOverwrite, opts...)
	case SFTP:
		sinkCfg, err := config.SinkSFTP(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return sftp.NewSink(ctx, l,
			sinkCfg.Address, sinkCfg.Username, sinkCfg.Password, sinkCfg.PrivateKey, sinkCfg.HostFingerprint,
			sinkCfg.DestinationPath,
			sinkCfg.GroupBy, sinkCfg.GroupBatchSize, sinkCfg.GroupColumnName,
			sinkCfg.ColumnMappingFilePath,
			sinkCfg.FilenamePattern, opts...)
	case KAFKA:
		sinkCfg, err := config.SinkKafka(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return kafka.NewSink(ctx, l, sinkCfg.BootstrapServers, sinkCfg.Topic, opts...)
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
	if jqCfg.Query != "" {
		return jqCfg.Query, nil
	}

	query, err := os.ReadFile(jqCfg.QueryFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", errors.WithStack(err)
	}
	return string(query), nil
}

func GetDirectSourceSink(ctx context.Context, l *slog.Logger, source Type, sink Type, cfg *config.Config, envs ...string) (flow.NoFlow, error) {
	// set up options
	opts := getOpts(ctx, cfg)

	if source == OSS && sink == MC {
		directCfg, err := config.OSS2MC(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return direct.NewOSS2MC(ctx, l, directCfg, opts...)
	}
	return nil, fmt.Errorf("direct: unknown source-sink: %s-%s", source, sink)
}

// getOpts returns options based on the given config.
func getOpts(ctx context.Context, cfg *config.Config) []option.Option {
	return []option.Option{
		option.SetupLogger(cfg.LogLevel),
		option.SetupOtelSDK(ctx, cfg.OtelCollectorGRPCEndpoint, cfg.OtelAttributes),
		option.SetupBufferSize(cfg.BufferSize),
	}
}
