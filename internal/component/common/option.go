package common

import (
	"context"
	"fmt"
	"strings"

	"github.com/goto/optimus-any2any/internal/logger"
)

// Option is a function that takes a Common struct and modifies it
// It is used to set up the Common struct with different configurations
type Option func(*Common)

// SetupLogger sets up the logger for the Common struct
func SetupLogger(level string) Option {
	return func(c *Common) {
		l, err := logger.NewLogger(level)
		if err != nil {
			c.Core.Logger().Warn(fmt.Sprintf("failed to set logger %s, use default", err.Error()))
			l = logger.NewDefaultLogger()
		}
		c.Core.SetLogger(l)
	}
}

// SetupBufferSize sets up the buffer size for the Common struct
func SetupBufferSize(bufferSize int) Option {
	return func(c *Common) {
		if bufferSize > 0 {
			c.Core.SetBufferSize(bufferSize)
		}
	}
}

// SetupOtelSDK sets up the OpenTelemetry SDK for the Common struct
func SetupOtelSDK(ctx context.Context, otelCollectorGRPCEndpoint string, otelAttributes string) Option {
	return func(c *Common) {
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
		c.SetOtelSDK(ctx, otelCollectorGRPCEndpoint, attr)
	}
}

// SetupRetry sets up the retry parameters for the Common struct
func SetupRetry(retryMax int, retryBackoffMs int64) Option {
	return func(c *Common) {
		c.SetRetry(1, 1000)
		if retryMax > 0 {
			c.SetRetry(retryMax, retryBackoffMs)
		}
	}
}
