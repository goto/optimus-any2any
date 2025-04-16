package common

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
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

// ConcurrentLimiter is an interface that defines a method to limit the number of concurrent tasks.
type ConcurrentLimiter interface {
	ConcurrentTasks(context.Context, int, []func() error) error
}

// Common is an extension of the component.Core struct
type Common struct {
	Core           *component.Core
	m              metric.Meter
	retryMax       int
	retryBackoffMs int64
	metadataPrefix string
}

var _ ConcurrentLimiter = (*Common)(nil)
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
	return Retry(c.Core.Logger(), c.retryMax, c.retryBackoffMs, f)
}

// ConcurrentTasks runs the given functions concurrently with a limit
func (c *Common) ConcurrentTasks(ctx context.Context, concurrencyLimit int, funcs []func() error) error {
	return ConcurrentTask(ctx, concurrencyLimit, funcs)
}

// RecordWithMetadata returns a new record without metadata prefix
func (c *Common) RecordWithoutMetadata(record *model.Record) *model.Record {
	return RecordWithoutMetadata(record, c.metadataPrefix)
}

// RecordWithMetadata returns a new record with metadata prefix
func (c *Common) RecordWithMetadata(record *model.Record) *model.Record {
	return RecordWithMetadata(record, c.metadataPrefix)
}

// RecordWithoutMetadata returns a new record without metadata prefix
// TODO: refactor this function to use the proper package for metadata
func RecordWithMetadata(record *model.Record, metadataPrefix string) *model.Record {
	recordWithMetadata := model.NewRecord()
	for k, v := range record.AllFromFront() {
		if strings.HasPrefix(k, metadataPrefix) {
			continue
		}
		recordWithMetadata.Set(fmt.Sprintf("%s%s", metadataPrefix, k), v)
	}
	return recordWithMetadata
}

// RecordWithoutMetadata returns a new record without metadata prefix
// TODO: refactor this function to use the proper package for metadata
func RecordWithoutMetadata(record *model.Record, metadataPrefix string) *model.Record {
	recordWithoutMetadata := model.NewRecord()
	for k, v := range record.AllFromFront() {
		if strings.HasPrefix(k, metadataPrefix) {
			continue
		}
		recordWithoutMetadata.Set(k, v)
	}
	return recordWithoutMetadata
}

// Retry retries the given function with the specified maximum number of attempts
// TODO: refactor this function to use proper package for retry
func Retry(l *slog.Logger, retryMax int, retryBackoffMs int64, f func() error) error {
	var err error
	sleepTime := int64(1)

	for i := range retryMax {
		err = f()
		if err == nil {
			return nil
		}

		l.Warn(fmt.Sprintf("retry: %d, error: %v", i, err))
		sleepTime *= 1 << i
		time.Sleep(time.Duration(sleepTime*retryBackoffMs) * time.Millisecond)
	}

	return err
}

// ConcurrentTask runs N tasks concurrently with a limit, cancels on first error or context cancel.
// TODO: refactor this function to use proper package for concurrency
func ConcurrentTask(ctx context.Context, concurrencyLimit int, funcs []func() error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	sem := make(chan struct{}, concurrencyLimit)
	errCh := make(chan error, 1)
	var wg sync.WaitGroup

	for _, fn := range funcs {
		select {
		case <-ctx.Done():
			break
		default:
		}

		wg.Add(1)
		go func(f func() error) {
			defer wg.Done()

			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}

			// run the task
			done := make(chan struct{})
			go func() {
				defer close(done)
				if err := f(); err != nil {
					select {
					case errCh <- err:
						cancel() // cancel all other tasks
					default:
					}
				}
			}()

			select {
			case <-done:
			case <-ctx.Done():
				return
			}
		}(fn)
	}

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}
