package common

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/internal/otel"
	"github.com/goto/optimus-any2any/pkg/component"
	opentelemetry "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// RecordHelper is an interface that defines methods to manipulate records
// by adding or removing metadata prefixes.
// It is used to handle records in a consistent way across different components.
type RecordHelper interface {
	RecordWithoutMetadata(record *model.Record) *model.Record
	RecordWithMetadata(record *model.Record) *model.Record
}

// Retrier is an interface that defines a method to retry a function
// a given number of times with a backoff strategy.
// It is used to handle transient errors in a consistent way across different components.
type Retrier interface {
	Retry(func() error) error
}

// Common is an extension of the component.Core struct
type Common struct {
	Core           *component.Core
	m              metric.Meter
	retryMax       int
	retryBackoffMs int64
	metadataPrefix string
}

var _ Retrier = (*Common)(nil)
var _ RecordHelper = (*Common)(nil)

// NewCommon creates a new Common struct
func NewCommon(c *component.Core) *Common {
	return &Common{
		Core:           c,
		m:              opentelemetry.GetMeterProvider().Meter(c.Component()),
		retryMax:       1,              // default
		retryBackoffMs: 1000,           // default
		metadataPrefix: "__METADATA__", // default
	}

}

// SetOtelSDK sets up the OpenTelemetry SDK
func (c *Common) SetOtelSDK(ctx context.Context, otelCollectorGRPCEndpoint string, otelAttributes map[string]string) {
	c.Core.Logger().Debug(fmt.Sprintf("set otel sdk: %s", otelCollectorGRPCEndpoint))
	shutdownFunc, err := otel.SetupOTelSDK(ctx, otelCollectorGRPCEndpoint, otelAttributes)
	if err != nil {
		c.Core.Logger().Error(fmt.Sprintf("set otel sdk error: %s", err.Error()))
	}
	c.Core.AddCleanFunc(func() error {
		if err := shutdownFunc(); err != nil {
			c.Core.Logger().Error(fmt.Sprintf("otel sdk shutdown error: %s", err.Error()))
			return err
		}
		return nil
	})
}

// SetRetry sets the retry parameters
func (c *Common) SetRetry(retryMax int, retryBackoffMs int64) {
	c.retryMax = retryMax
	c.retryBackoffMs = retryBackoffMs
}

// SetMetadataPrefix sets the metadata prefix
func (c *Common) SetMetadataPrefix(metadataPrefix string) {
	c.metadataPrefix = metadataPrefix
}

// Retry retries the given function with the configured retry parameters
func (c *Common) Retry(f func() error) error {
	return retry(c.Core.Logger(), c.retryMax, c.retryBackoffMs, f)
}

// RecordWithMetadata returns a new record without metadata prefix
func (c *Common) RecordWithoutMetadata(record *model.Record) *model.Record {
	recordWithoutMetadata := model.NewRecord()
	for k, v := range record.AllFromFront() {
		if strings.HasPrefix(k, c.metadataPrefix) {
			continue
		}
		recordWithoutMetadata.Set(k, v)
	}
	return recordWithoutMetadata
}

// RecordWithMetadata returns a new record with metadata prefix
func (c *Common) RecordWithMetadata(record *model.Record) *model.Record {
	recordWithMetadata := model.NewRecord()
	for k, v := range record.AllFromFront() {
		if strings.HasPrefix(k, c.metadataPrefix) {
			continue
		}
		recordWithMetadata.Set(fmt.Sprintf("%s%s", c.metadataPrefix, k), v)
	}
	return recordWithMetadata
}

func retry(l *slog.Logger, retryMax int, retryBackoffMs int64, f func() error) error {
	var err error
	sleepTime := int64(1)

	for i := range retryMax {
		err = f()
		if err == nil {
			return nil
		}

		l.Warn(fmt.Sprintf("retry: %d, error: %v", i, err))
		sleepTime *= 1 << i
		time.Sleep(time.Duration(sleepTime*retryBackoffMs) * time.Second)
	}

	return err
}
