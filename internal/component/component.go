package component

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"strings"

	"github.com/goto/optimus-any2any/ext/direct"
	"github.com/goto/optimus-any2any/ext/file"
	"github.com/goto/optimus-any2any/ext/gmail"
	"github.com/goto/optimus-any2any/ext/googleanalytics"
	"github.com/goto/optimus-any2any/ext/http"
	"github.com/goto/optimus-any2any/ext/io"
	"github.com/goto/optimus-any2any/ext/kafka"
	"github.com/goto/optimus-any2any/ext/maxcompute"
	"github.com/goto/optimus-any2any/ext/oss"
	"github.com/goto/optimus-any2any/ext/postgresql"
	"github.com/goto/optimus-any2any/ext/redis"
	"github.com/goto/optimus-any2any/ext/s3"
	"github.com/goto/optimus-any2any/ext/salesforce"
	"github.com/goto/optimus-any2any/ext/sftp"
	"github.com/goto/optimus-any2any/ext/smtp"
	"github.com/goto/optimus-any2any/internal/compiler"
	"github.com/goto/optimus-any2any/internal/component/common"
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
	S3    Type = "S3"
	SFTP  Type = "SFTP"
	SMTP  Type = "SMTP"
	PSQL  Type = "PSQL"
	GMAIL Type = "GMAIL"
	REDIS Type = "REDIS"
	HTTP  Type = "HTTP"
	GA    Type = "GA"
)

// GetSource returns a source based on the given type.
// It will return an error if the source is unknown.
func GetSource(ctx context.Context, cancelFn context.CancelCauseFunc, l *slog.Logger, source Type, cfg *config.Config, envs ...string) (flow.Source, error) {
	// set up options
	opts := getOpts(ctx, cfg)
	opts = append(opts, common.SetupConcurrency(cfg.SourceConcurrency))
	// create commonSource
	commonSource := common.NewCommonSource(ctx, cancelFn, l, strings.ToLower(string(source)), opts...)

	// create source based on type
	switch source {
	case MC:
		sourceCfg, err := config.SourceMC(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return maxcompute.NewSource(commonSource, sourceCfg.Credentials, sourceCfg.QueryFilePath, sourceCfg.PreQueryFilePath, sourceCfg.FilenameColumn, sourceCfg.ExecutionProject, sourceCfg.AdditionalHints, sourceCfg.LogViewRetentionInDays, sourceCfg.BatchSize)
	case FILE:
		sourceCfg, err := config.SourceFile(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return file.NewSource(commonSource, sourceCfg.SourceURI)
	case SF:
		sourceCfg, err := config.SourceSalesforce(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return salesforce.NewSource(commonSource,
			sourceCfg.Host, sourceCfg.User, sourceCfg.Pass, sourceCfg.Token,
			sourceCfg.APIVersion, sourceCfg.IncludeDeleted,
			sourceCfg.SOQLFilePath, opts...)
	case GMAIL:
		sourceCfg, err := config.SourceGmail(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return gmail.NewSource(commonSource, sourceCfg.Token, sourceCfg.Filter, sourceCfg.FilenameColumn, sourceCfg.CSVDelimiter, opts...)
	case OSS:
		sourceCfg, err := config.SourceOSS(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return oss.NewSource(commonSource, sourceCfg.Credentials, sourceCfg.SourceURI, sourceCfg.CSVDelimiter, sourceCfg.SkipHeader, sourceCfg.SkipRows, opts...)
	case GA:
		sourceCfg, err := config.SourceGA(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return googleanalytics.NewSource(commonSource,
			sourceCfg.ServiceAccount, sourceCfg.ConnectionTLSCert, sourceCfg.ConnectionTLSCACert, sourceCfg.ConnectionTLSKey,
			sourceCfg.PropertyID, sourceCfg.StartDate, sourceCfg.EndDate, sourceCfg.Dimensions, sourceCfg.Metrics, sourceCfg.BatchSize,
		)
	case IO:
	}
	return nil, fmt.Errorf("source: unknown source: %s", source)
}

// GetSink returns a sink based on the given type.
// It will return an error if the sink is unknown.
func GetSink(ctx context.Context, cancelFn context.CancelCauseFunc, l *slog.Logger, sink Type, cfg *config.Config, envs ...string) (flow.Sink, error) {
	// set up options
	opts := getOpts(ctx, cfg)
	opts = append(opts, common.SetupConcurrency(cfg.SinkConcurrency))
	// create commonSink
	commonSink := common.NewCommonSink(ctx, cancelFn, l, strings.ToLower(string(sink)), opts...)

	// create sink based on type
	switch sink {
	case MC:
		sinkCfg, err := config.SinkMC(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return maxcompute.NewSink(commonSink, sinkCfg.Credentials, sinkCfg.ExecutionProject, sinkCfg.DestinationTableID, sinkCfg.LoadMethod, sinkCfg.UploadMode, sinkCfg.BatchSizeInMB, sinkCfg.Concurrency, sinkCfg.AllowSchemaMismatch, opts...)
	case FILE:
		sinkCfg, err := config.SinkFile(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return file.NewSink(commonSink, sinkCfg.DestinationURI, sinkCfg.CompressionType, sinkCfg.CompressionPassword, sinkCfg.JSONPathSelector, opts...)
	case IO:
		return io.NewSink(commonSink), nil
	case OSS:
		sinkCfg, err := config.SinkOSS(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return oss.NewSink(commonSink, sinkCfg.Credentials,
			sinkCfg.DestinationURI,
			sinkCfg.BatchSize, sinkCfg.EnableOverwrite,
			sinkCfg.SkipHeader,
			sinkCfg.CompressionType, sinkCfg.CompressionPassword,
			sinkCfg.ConnectionTimeoutSeconds, sinkCfg.ReadWriteTimeoutSeconds,
			opts...)
	case S3:
		sinkCfg, err := config.SinkS3(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return s3.NewSink(commonSink, sinkCfg.Credentials, sinkCfg.Provider, sinkCfg.Region,
			sinkCfg.DestinationURI, sinkCfg.BatchSize, sinkCfg.EnableOverwrite,
			sinkCfg.SkipHeader, sinkCfg.MaxTempFileRecordNumber,
			sinkCfg.CompressionType, sinkCfg.CompressionPassword,
			sinkCfg.JSONPathSelector,
			opts...)
	case SFTP:
		sinkCfg, err := config.SinkSFTP(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return sftp.NewSink(commonSink,
			sinkCfg.PrivateKey, sinkCfg.HostFingerprint,
			sinkCfg.DestinationURI,
			sinkCfg.CompressionType, sinkCfg.CompressionPassword,
			sinkCfg.JSONPathSelector,
			opts...)
	case SMTP:
		sinkCfg, err := config.SinkSMTP(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}

		storageCfg := smtp.StorageConfig{
			Mode:           sinkCfg.StorageMode,
			DestinationDir: sinkCfg.StorageDestinationDir,
			Credentials:    sinkCfg.StorageCredentials,
			LinkExpiration: sinkCfg.StorageLinkExpiration,
		}

		return smtp.NewSink(commonSink,
			sinkCfg.ConnectionDSN, sinkCfg.From, sinkCfg.To, sinkCfg.Subject,
			sinkCfg.BodyFilePath, sinkCfg.BodyNoRecordFilePath, sinkCfg.AttachmentFilename, storageCfg,
			sinkCfg.CompressionType, sinkCfg.CompressionPassword,
			opts...)
	case PSQL:
		sinkCfg, err := config.SinkPG(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return postgresql.NewSink(commonSink, sinkCfg.ConnectionDSN, sinkCfg.PreSQLScript,
			sinkCfg.DestinationTableID, sinkCfg.BatchSize, opts...)
	case REDIS:
		sinkCfg, err := config.SinkRedis(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return redis.NewSink(commonSink,
			sinkCfg.ConnectionDSN, sinkCfg.ConnectionTLSCert, sinkCfg.ConnectionTLSCACert, sinkCfg.ConnectionTLSKey,
			sinkCfg.RecordKey, sinkCfg.RecordValue, sinkCfg.BatchSize, opts...)
	case HTTP:
		sinkCfg, err := config.SinkHTTP(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return http.NewSink(commonSink,
			sinkCfg.Method, sinkCfg.Endpoint, sinkCfg.Headers, sinkCfg.HeadersFile,
			sinkCfg.Body, sinkCfg.BodyFilePath, sinkCfg.BatchSize, opts...)
	case KAFKA:
		sinkCfg, err := config.SinkKafka(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return kafka.NewSink(commonSink, sinkCfg.BootstrapServers, sinkCfg.Topic, opts...)
	}
	return nil, fmt.Errorf("sink: unknown sink: %s", sink)
}

// GetJQQuery returns a jq query based on the given environment variables.
// jq query is used for JSON transformation, it acts as a filter and map for JSON data.
func GetJQQuery(l *slog.Logger, jqCfg *config.ProcessorJQConfig) (string, error) {
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
	// TODO: refactor the package extcommon, since it's also being used
	// in internal folder
	tmpl, err := compiler.NewTemplate("connector_jq", string(query))
	if err != nil {
		return "", errors.WithStack(err)
	}
	compiledQuery, err := compiler.Compile(tmpl, nil)
	if err != nil {
		return "", errors.WithStack(err)
	}
	if _, err := exec.LookPath("jq"); err != nil {
		l.Error("processor: jq not found")
		return "", errors.WithStack(err)
	}
	l.Info(fmt.Sprintf("processor: jq query: %s", compiledQuery))
	return compiledQuery, nil
}

func GetDirectSourceSink(ctx context.Context, l *slog.Logger, source Type, sink Type, cfg *config.Config, envs ...string) (flow.NoFlow, error) {
	if source == OSS && sink == MC {
		directCfg, err := config.OSS2MC(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return direct.NewOSS2MC(ctx, l, directCfg)
	}
	if source == MC && sink == OSS {
		sourceCfg, err := config.SourceMC(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		sinkCfg, err := config.SinkOSS(envs...)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return direct.NewMC2OSS(ctx, l, cfg.MetadataPrefix, sourceCfg, sinkCfg)
	}
	return nil, fmt.Errorf("direct: unknown source-sink: %s-%s", source, sink)
}

// getOpts returns options based on the given config.
func getOpts(ctx context.Context, cfg *config.Config) []common.Option {
	return []common.Option{
		common.SetupLogger(cfg.LogLevel),
		common.SetupBufferSize(cfg.BufferSize),
		common.SetupBackend(cfg.Backend),
		common.SetupOtelSDK(ctx, cfg.OtelCollectorGRPCEndpoint, cfg.OtelAttributes),
		common.SetupRetry(cfg.RetryMax, cfg.RetryBackoffMs),
		common.SetupDryRun(cfg.DryRun),
		common.SetupMetadataPrefix(cfg.MetadataPrefix),
	}
}
