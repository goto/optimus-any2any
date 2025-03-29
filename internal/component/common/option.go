package common

import (
	"context"
	"strings"
)

type SetupOptions interface {
	SetBufferSize(int)
	SetOtelSDK(context.Context, string, map[string]string)
	SetRetry(int, int64)
}

type Option func(SetupOptions)

func SetupBufferSize(bufferSize int) Option {
	return func(o SetupOptions) {
		if bufferSize > 0 {
			o.SetBufferSize(bufferSize)
		}
	}
}

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

func SetupRetry(retryMax int, retryBackoffMs int64) Option {
	return func(o SetupOptions) {
		o.SetRetry(1, 1000)
		if retryMax > 0 {
			o.SetRetry(retryMax, retryBackoffMs)
		}
	}
}
