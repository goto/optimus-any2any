package otel

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	m "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// SetupOTelSDK sets up the OpenTelemetry SDK.
func SetupOTelSDK(ctx context.Context, collectorGRPCEndpoint string, attributes map[string]string) (shutdown func() error, err error) {
	metricExporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(collectorGRPCEndpoint),
		otlpmetricgrpc.WithInsecure(),
	)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	attr := []attribute.KeyValue{}
	for k, v := range attributes {
		attr = append(attr, attribute.String(k, v))
	}

	// for now, we only need metric provider
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(resource.NewWithAttributes(
			resource.Default().SchemaURL(),
			attr...,
		)),
		metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(5*time.Second))),
	)
	otel.SetMeterProvider(meterProvider)

	// start runtime metrics collection
	// this will collect metrics like memory usage, goroutines, etc.
	runtime.Start(runtime.WithMinimumReadMemStatsInterval(1 * time.Second))

	return func() error {
		return meterProvider.Shutdown(context.Background())
	}, nil
}

func GetMeter(component, name string) m.Meter {
	meterName := fmt.Sprintf("%s_%s", component, name)
	meter := otel.GetMeterProvider().Meter(meterName,
		m.WithInstrumentationVersion(InstrumentationVersion),
		m.WithInstrumentationAttributes(
			attribute.String("name", name),
		),
	)
	return meter
}
