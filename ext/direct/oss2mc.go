package direct

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/aliyun/aliyun-odps-go-sdk/odps"
	"github.com/goto/optimus-any2any/ext/maxcompute"
	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/config"
	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/goto/optimus-any2any/internal/otel"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

// OSS2MC is a direct execution from OSS to MaxCompute.
type OSS2MC struct {
	logger           *slog.Logger
	client           *odps.Odps
	query            string
	inst             *odps.Instance
	logViewRetention int
	cleanFuncs       []func()
}

// NewOSS2MC creates a new direct component to execute from OSS to MaxCompute.
func NewOSS2MC(ctx context.Context, l *slog.Logger, cfg *config.OSS2MCConfig, opts ...option.Option) (flow.NoFlow, error) {

	// create client for maxcompute
	client, err := maxcompute.NewClient(cfg.ServiceAccount)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// build query
	query, err := buildQuery(cfg)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	oss2mc := &OSS2MC{
		logger:           l,
		client:           client,
		query:            query,
		logViewRetention: cfg.LogViewRetentionInDays * 24,
		cleanFuncs:       []func(){},
	}

	for _, opt := range opts {
		opt(oss2mc)
	}

	// add clean function
	oss2mc.cleanFuncs = append(oss2mc.cleanFuncs, func() {
		// terminate instance if it is not nil and not terminated
		if oss2mc.inst != nil && oss2mc.inst.Status() != odps.InstanceTerminated {
			oss2mc.logger.Info("direct(oss2mc): terminating instance")
			_ = oss2mc.inst.Terminate()
		}
	})

	return oss2mc, nil
}

func (o *OSS2MC) Run() []error {
	// execute query
	o.logger.Info(fmt.Sprintf("direct(oss2mc): executing query: \n%s", o.query))
	inst, err := o.client.ExecSQl(o.query, nil)
	if err != nil {
		return []error{errors.WithStack(err)}
	}
	o.inst = inst

	// generate log view
	url, err := odps.NewLogView(o.client).GenerateLogView(inst, o.logViewRetention)
	if err != nil {
		return []error{errors.WithStack(err)}
	}
	o.logger.Info(fmt.Sprintf("direct(oss2mc): log view: %s", url))

	// wait for success
	if err := inst.WaitForSuccess(); err != nil {
		return []error{errors.WithStack(err)}
	}
	return nil
}

func (o *OSS2MC) Close() {
	o.logger.Debug("direct(oss2mc): close")
	for _, clean := range o.cleanFuncs {
		clean()
	}
}

func buildQuery(cfg *config.OSS2MCConfig) (string, error) {
	var (
		loadOp           string
		tableDestination string
		partition        string
		ossURI           string
		storageHandler   string
	)
	template := "load %s table `%s` %s from location '%s' %s;"

	// set load operation
	switch strings.ToLower(cfg.LoadMethod) {
	case "replace":
		loadOp = "overwrite"
	case "append":
		loadOp = "into"
	default:
		return "", errors.Errorf("direct(oss2mc): unknown load method: %s", cfg.LoadMethod)
	}
	// set table destination
	tableDestination = cfg.DestinationTableID
	// set partiton columns if it exists
	if len(cfg.PartitionValues) > 0 {
		partition = fmt.Sprintf("partition (%s)", strings.Join(cfg.PartitionValues, ","))
	}
	// set oss uri
	ossURI = fmt.Sprintf("oss://%s", cfg.SourceBucketPath)
	// set storage handler
	switch strings.ToLower(cfg.FileFormat) {
	case "json":
		storageHandler = "row format serde 'org.apache.hive.hcatalog.data.JsonSerDe' stored AS textfile"
	case "csv":
		storageHandler = "stored by 'com.aliyun.odps.CsvStorageHandler' with serdeproperties ('odps.text.option.header.lines.count'='1')"
	}

	return fmt.Sprintf(template, loadOp, tableDestination, partition, ossURI, storageHandler), nil
}

// SetBufferSize sets the buffer size.
func (o *OSS2MC) SetBufferSize(int) {
	// no implementation needed
}

// SetOtelSDK sets the OpenTelemetry SDK.
func (o *OSS2MC) SetOtelSDK(ctx context.Context, otelCollectorGRPCEndpoint string, otelAttributes map[string]string) {
	o.logger.Debug(fmt.Sprintf("direct(oss2mc): set otel sdk: %s", otelCollectorGRPCEndpoint))
	shutdownFunc, err := otel.SetupOTelSDK(ctx, otelCollectorGRPCEndpoint, otelAttributes)
	if err != nil {
		o.logger.Error(fmt.Sprintf("direct(oss2mc): set otel sdk error: %s", err.Error()))
	}
	o.cleanFuncs = append(o.cleanFuncs, func() {
		if err := shutdownFunc(); err != nil {
			o.logger.Error(fmt.Sprintf("direct(oss2mc): otel sdk shutdown error: %s", err.Error()))
		}
	})
}

// SetLogger sets the logger.
func (o *OSS2MC) SetLogger(logLevel string) {
	logger, err := logger.NewLogger(logLevel)
	if err != nil {
		o.logger.Error(fmt.Sprintf("direct(oss2mc): set logger error: %s", err.Error()))
		return
	}
	o.logger = logger
}
