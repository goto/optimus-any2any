package option

import (
	"context"
	"strings"
)

// SetupOptions is an interface that defines the options that can be set up
// for the component. It provides shared methods for setting up the component.
type SetupOptions interface {
	SetBufferSize(int)
	SetOtelSDK(context.Context, string, map[string]string)
	SetLogger(string)
}

// Option is a function that sets up the options for the component.
type Option func(SetupOptions)

// SetupBufferSize sets up the buffer size for the component.
func SetupBufferSize(bufferSize int) Option {
	return func(o SetupOptions) {
		if bufferSize > 0 {
			o.SetBufferSize(bufferSize)
		}
	}
}

// SetupOtelSDK sets up the OpenTelemetry SDK for the component.
func SetupOtelSDK(ctx context.Context, otelCollectorGRPCEndpoint string, otelAttributes string) Option {
	return func(o SetupOptions) {
		if otelCollectorGRPCEndpoint == "" {
			return
		}
		attrSlice := strings.Split(otelAttributes, ",")
		attr := make(map[string]string, len(attrSlice))
		for _, a := range attrSlice {
			kv := strings.Split(a, "=")
			if len(kv) == 2 {
				attr[kv[0]] = kv[1]
			}
		}
		o.SetOtelSDK(ctx, otelCollectorGRPCEndpoint, attr)
	}
}

// SetupLogger sets up the logger for the component.
func SetupLogger(logLevel string) Option {
	return func(o SetupOptions) {
		o.SetLogger(logLevel)
	}
}
