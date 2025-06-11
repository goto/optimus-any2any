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

// SetupMetadataPrefix sets up the metadata prefix for the Common struct
func SetupMetadataPrefix(metadataPrefix string) Option {
	return func(c *Common) {
		if metadataPrefix != "" {
			c.SetMetadataPrefix(metadataPrefix)
		}
	}
}

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

// SetupBackend sets up the backend for the Common struct
func SetupBackend(backend string) Option {
	return func(c *Common) {
		c.Core.SetBackend(strings.ToLower(backend))
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

// SetupDryRun sets up the dry run mode for the Common struct
func SetupDryRun(dryRun bool) Option {
	return func(c *Common) {
		c.SetDryRun(dryRun)
	}
}

// SetupConcurrency sets up the concurrency for the Common struct
func SetupConcurrency(concurrency int) Option {
	return func(c *Common) {
		c.SetConcurrency(1)
		if concurrency > 0 {
			c.SetConcurrency(concurrency)
		}
	}
}
