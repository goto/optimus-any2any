package common

import (
	"context"
	errs "errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	cq "github.com/goto/optimus-any2any/internal/concurrentqueue"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/internal/otel"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// RecordHelper is an interface that defines methods to manipulate records
// by adding or removing metadata prefixes.
// It is used to handle records in a consistent way across different components.
type RecordHelper interface {
	RecordWithoutMetadata(record *model.Record) *model.Record
	RecordWithMetadata(record *model.Record) *model.Record
	IsSpecializedMetadataRecord(record *model.Record) bool
}

// Retrier is an interface that defines a method to retry a function
// a given number of times with a backoff strategy.
// It is used to handle transient errors in a consistent way across different components.
type Retrier interface {
	Retry(func() error) error
}

// DryRunabler is an interface that defines a method to run a function
// in dry run mode. It is used to handle dry run scenarios in a consistent way across different components.
type DryRunabler interface {
	// DryRunable runs the given function in dry run mode.
	// It provides alternative optional functions to run in dry run mode.
	DryRunable(func() error, ...func() error) error
}

// ConcurrentLimiter is an interface that defines a method to limit the number of concurrent tasks.
type ConcurrentLimiter interface {
	ConcurrentTasks([]func() error) error
	ConcurrentQueue(func() error) error
	ConcurrentQueueWait() error
}

// InstrumentationGetter is an interface that provides relevant OpenTelemetry instrumentation components.
type InstrumentationGetter interface {
	Meter() metric.Meter
}

// Common is an extension of the component.Core struct
type Common struct {
	Core            *component.Core
	dryRun          bool
	dryRunPCs       map[uintptr]bool
	retryMax        int
	retryBackoffMs  int64
	concurrency     int
	concurrentQueue cq.ConcurrentQueue
	metadataPrefix  string

	// metrics related
	m                 metric.Meter
	recordCount       metric.Int64Counter
	recordBytes       metric.Int64Counter
	recordBytesBucket metric.Int64Histogram
	concurrentLimits  atomic.Int64
	concurrentCount   atomic.Int64
	processDurationMs metric.Int64Histogram
	retryCount        metric.Int64Counter
	attrFunc          func(...attribute.KeyValue) []attribute.KeyValue
}

var _ ConcurrentLimiter = (*Common)(nil)
var _ Retrier = (*Common)(nil)
var _ DryRunabler = (*Common)(nil)
var _ RecordHelper = (*Common)(nil)
var _ InstrumentationGetter = (*Common)(nil)

// NewCommon creates a new Common struct
func NewCommon(c *component.Core) (*Common, error) {
	common := &Common{
		Core:           c,
		m:              otel.GetMeter(c.Component(), c.Name()),
		dryRun:         false,                  // default
		dryRunPCs:      make(map[uintptr]bool), // default
		retryMax:       1,                      // default
		retryBackoffMs: 1000,                   // default
		concurrency:    4,                      // default
		metadataPrefix: "__METADATA__",         // default
	}
	// initialize attrFunc to append component and name attributes
	common.attrFunc = func(kv ...attribute.KeyValue) []attribute.KeyValue {
		return append(otel.GetAttributes(c.Component(), c.Name()), kv...)
	}
	if err := common.initializeMetrics(); err != nil {
		return nil, errors.WithStack(err)
	}
	return common, nil
}

func (c *Common) initializeMetrics() error {
	var err error

	// non-observable metrics
	if c.recordCount, err = c.m.Int64Counter(otel.Record, metric.WithDescription("The total number of records processed"), metric.WithUnit("1")); err != nil {
		return errors.WithStack(err)
	}
	if c.recordBytes, err = c.m.Int64Counter(otel.RecordBytes, metric.WithDescription("The total number of bytes processed"), metric.WithUnit("bytes")); err != nil {
		return errors.WithStack(err)
	}
	if c.recordBytesBucket, err = c.m.Int64Histogram(otel.RecordBytesBucket, metric.WithDescription("The total number of bytes processed in buckets"), metric.WithUnit("bytes")); err != nil {
		return errors.WithStack(err)
	}
	if c.processDurationMs, err = c.m.Int64Histogram(otel.ProcessDuration, metric.WithDescription("The duration of the process in milliseconds"), metric.WithUnit("ms")); err != nil {
		return errors.WithStack(err)
	}
	if c.retryCount, err = c.m.Int64Counter(otel.Retry, metric.WithDescription("The total number of retries performed"), metric.WithUnit("1")); err != nil {
		return errors.WithStack(err)
	}

	// observable metrics
	c.concurrentLimits = atomic.Int64{}
	c.concurrentCount = atomic.Int64{}
	processLimits, err := c.m.Int64ObservableGauge(otel.ProcessLimits, metric.WithDescription("The current concurrency limit"), metric.WithUnit("1"))
	if err != nil {
		return errors.WithStack(err)
	}
	processCount, err := c.m.Int64ObservableGauge(otel.Process, metric.WithDescription("The current number of processes running"), metric.WithUnit("1"))
	if err != nil {
		return errors.WithStack(err)
	}
	// register the callback to observe the current concurrency limits and count
	_, err = c.m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(processLimits, c.concurrentLimits.Load())
		o.ObserveInt64(processCount, c.concurrentCount.Load())
		return nil
	}, processLimits, processCount)
	return nil
}

// SetOtelSDK sets up the OpenTelemetry SDK
func (c *Common) SetOtelSDK(ctx context.Context, otelCollectorGRPCEndpoint string, otelAttributes map[string]string) {
	c.Core.Logger().Debug(fmt.Sprintf("set otel sdk: %s", otelCollectorGRPCEndpoint))
	c.Core.Logger().Info(fmt.Sprintf("instrumentation version: %s", otel.InstrumentationVersion))

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

// SetDryRun sets the dry run mode
func (c *Common) SetDryRun(dryRun bool) {
	c.dryRun = dryRun
}

// SetMetadataPrefix sets the metadata prefix
func (c *Common) SetMetadataPrefix(metadataPrefix string) {
	c.metadataPrefix = metadataPrefix
}

// Retry retries the given function with the configured retry parameters
func (c *Common) Retry(f func() error) error {
	return Retry(c.Core.Logger(), c.retryMax, c.retryBackoffMs, f, func() {
		c.retryCount.Add(c.Core.Context(), 1, metric.WithAttributes(c.attrFunc()...))
	})
}

// SetConcurrency sets the concurrency limit for concurrent tasks
func (c *Common) SetConcurrency(concurrency int) {
	c.concurrency = concurrency
}

// Meter returns the OpenTelemetry meter for this current instance
func (c *Common) Meter() metric.Meter {
	return c.m
}

// DryRunable runs the given function in dry run mode
func (c *Common) DryRunable(f func() error, dryRunFuncs ...func() error) error {
	if c.dryRun {
		// for logging purpose, we need to get the caller function
		// and log it only once
		pc, _, _, _ := runtime.Caller(1)
		if _, ok := c.dryRunPCs[pc]; !ok {
			// get the file name and line number
			fileName, line := runtime.FuncForPC(pc).FileLine(pc)
			c.Core.Logger().Info(fmt.Sprintf("dry run mode, skipping function %s:%d", filepath.Base(fileName), line))
			c.dryRunPCs[pc] = true
		}
		// run dry run functions if any
		var e error
		for _, dryRunFunc := range dryRunFuncs {
			if err := dryRunFunc(); err != nil {
				e = errs.Join(e, err)
			}
		}
		return errors.WithStack(e)
	}
	return errors.WithStack(f())
}

// ConcurrentTasks runs the given functions concurrently with a limit
func (c *Common) ConcurrentTasks(funcs []func() error) error {
	// create a new concurrent queue with the specified concurrency limit
	concurrentQueue := cq.NewConcurrentQueueWithCancel(c.Core.Context(), c.Core.CancelFunc(), c.concurrency)
	c.concurrentLimits.Add(int64(c.concurrency))
	defer func() {
		c.concurrentLimits.Add(-int64(c.concurrency))
	}()

	for _, fn := range funcs {
		funcName := getFuncName(fn)
		if err := concurrentQueue.Submit(func() error {
			// capture the start time for processing duration
			startTime := time.Now()
			c.concurrentCount.Add(1)
			defer func() {
				c.concurrentCount.Add(-1)
				c.processDurationMs.Record(c.Core.Context(), time.Since(startTime).Milliseconds(), metric.WithAttributes(
					c.attrFunc(attribute.KeyValue{Key: "function", Value: attribute.StringValue(funcName)})...,
				))
			}()
			return errors.WithStack(fn())
		}); err != nil {
			return errors.WithStack(err)
		}

	}

	// wait for all queued functions to finish
	return errors.WithStack(concurrentQueue.Wait())
}

// ConcurrentQueue adds a function to the queue to be run concurrently
func (c *Common) ConcurrentQueue(fn func() error) error {
	if c.concurrentQueue == nil {
		// initialize the concurrent queue if it is not already initialized
		c.concurrentQueue = cq.NewConcurrentQueueWithCancel(c.Core.Context(), c.Core.CancelFunc(), c.concurrency)
		c.concurrentLimits.Add(int64(c.concurrency))
	}

	// add the function to the queue
	funcName := getFuncName(fn)
	if err := c.concurrentQueue.Submit(func() error {
		// capture the start time for processing duration
		startTime := time.Now()
		c.concurrentCount.Add(1)
		defer func() {
			c.concurrentCount.Add(-1)
			c.processDurationMs.Record(c.Core.Context(), time.Since(startTime).Milliseconds(), metric.WithAttributes(
				c.attrFunc(attribute.KeyValue{Key: "function", Value: attribute.StringValue(funcName)})...,
			))
		}()
		return errors.WithStack(fn())
	}); err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// ConcurrentQueueWait waits for all queued functions to finish
func (c *Common) ConcurrentQueueWait() error {
	if c.concurrentQueue == nil {
		return nil // no functions to wait for
	}

	// wait for all queued functions to finish and release concurrent limits
	defer func() {
		c.concurrentLimits.Add(-int64(c.concurrency))
	}()
	return c.concurrentQueue.Wait()
}

// RecordWithMetadata returns a new record without metadata prefix
func (c *Common) RecordWithoutMetadata(record *model.Record) *model.Record {
	return RecordWithoutMetadata(record, c.metadataPrefix)
}

// RecordWithMetadata returns a new record with metadata prefix
func (c *Common) RecordWithMetadata(record *model.Record) *model.Record {
	return RecordWithMetadata(record, c.metadataPrefix)
}

// IsSpecializedMetadataRecord checks if the record is specialized metadata record
func (c *Common) IsSpecializedMetadataRecord(record *model.Record) bool {
	return IsSpecializedMetadataRecord(record, c.metadataPrefix)
}

// IsSpecializedMetadataRecord checks if the record is specialized metadata record
func IsSpecializedMetadataRecord(record *model.Record, metadataPrefix string) bool {
	return model.HasAnyPrefix(record, metadataPrefix)
}

// RecordWithoutMetadata returns a new record without metadata prefix
// TODO: refactor this function to use the proper package for metadata
func RecordWithMetadata(record *model.Record, metadataPrefix string) *model.Record {
	recordWithMetadata := model.NewRecord()
	for k, v := range record.AllFromFront() {
		if strings.HasPrefix(k, metadataPrefix) {
			recordWithMetadata.Set(k, v)
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
func Retry(l *slog.Logger, retryMax int, retryBackoffMs int64, f func() error, hookFuncs ...func()) error {
	var err error
	sleepTime := int64(1)

	for i := range retryMax {
		err = f()
		if err == nil {
			return nil
		}

		// if hook functions are provided, execute them on each retry
		for _, hookFunc := range hookFuncs {
			hookFunc()
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
	if len(funcs) == 0 {
		return nil // nothing to run
	}
	concurrentQueue := cq.NewConcurrentQueue(ctx, concurrencyLimit)
	for _, fn := range funcs {
		if err := concurrentQueue.Submit(fn); err != nil {
			return errors.WithStack(err)
		}
	}
	return errors.WithStack(concurrentQueue.Wait())
}

func getFuncName(fn any) string {
	pc := reflect.ValueOf(fn).Pointer()
	fileName, line := runtime.FuncForPC(pc).FileLine(pc)
	return fmt.Sprintf("%s:%d", filepath.Base(fileName), line)
}
