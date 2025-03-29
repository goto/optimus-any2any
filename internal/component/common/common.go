package common

import (
	"context"
	"fmt"

	"github.com/goto/optimus-any2any/internal/otel"
	"github.com/goto/optimus-any2any/pkg/component"
	opentelemetry "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Common is a common struct for all components
// Including source and sink
type Common struct {
	*component.Core
	m              metric.Meter
	retryMax       int
	retryBackoffMs int64
}

// NewCommon creates a new Common struct
func NewCommon(c *component.Core, component string) *Common {
	return &Common{
		Core:           c,
		m:              opentelemetry.GetMeterProvider().Meter(component),
		retryMax:       1,    // default
		retryBackoffMs: 1000, // default
	}

}

// SetOtelSDK sets up the OpenTelemetry SDK
func (c *Common) SetOtelSDK(ctx context.Context, otelCollectorGRPCEndpoint string, otelAttributes map[string]string) {
	c.Logger().Debug(fmt.Sprintf("set otel sdk: %s", otelCollectorGRPCEndpoint))
	shutdownFunc, err := otel.SetupOTelSDK(ctx, otelCollectorGRPCEndpoint, otelAttributes)
	if err != nil {
		c.Logger().Error(fmt.Sprintf("set otel sdk error: %s", err.Error()))
	}
	c.AddCleanFunc(func() error {
		if err := shutdownFunc(); err != nil {
			c.Logger().Error(fmt.Sprintf("otel sdk shutdown error: %s", err.Error()))
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

// Retry retries the given function with the configured retry parameters
func (c *Common) Retry(f func() error) error {
	return retry(c.Logger(), c.retryMax, c.retryBackoffMs, f)
}
