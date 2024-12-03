package otel

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
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

	return func() error {
		return meterProvider.Shutdown(context.Background())
	}, nil
}
