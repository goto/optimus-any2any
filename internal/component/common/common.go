package common

import (
	"context"
	errs "errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/internal/otel"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/pkg/errors"
	opentelemetry "go.opentelemetry.io/otel"
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
	ConcurrentQueue(func() error)
	ConcurrentQueueWait() error
}

// Common is an extension of the component.Core struct
type Common struct {
	Core            *component.Core
	m               metric.Meter
	dryRun          bool
	dryRunPCs       map[uintptr]bool
	retryMax        int
	retryBackoffMs  int64
	concurrency     int
	concurrentQueue *concurrentQueue
	metadataPrefix  string
}

var _ ConcurrentLimiter = (*Common)(nil)
var _ Retrier = (*Common)(nil)
var _ DryRunabler = (*Common)(nil)
var _ RecordHelper = (*Common)(nil)

// NewCommon creates a new Common struct
func NewCommon(c *component.Core) *Common {
	return &Common{
		Core:           c,
		m:              opentelemetry.GetMeterProvider().Meter(c.Component()),
		dryRun:         false,                  // default
		dryRunPCs:      make(map[uintptr]bool), // default
		retryMax:       1,                      // default
		retryBackoffMs: 1000,                   // default
		concurrency:    4,                      // default
		metadataPrefix: "__METADATA__",         // default
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
	return Retry(c.Core.Logger(), c.retryMax, c.retryBackoffMs, f)
}

// SetConcurrency sets the concurrency limit for concurrent tasks
func (c *Common) SetConcurrency(concurrency int) {
	c.concurrency = concurrency
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
	return ConcurrentTask(c.Core.Context(), c.concurrency, funcs)
}

// ConcurrentQueue adds a function to the queue to be run concurrently
func (c *Common) ConcurrentQueue(fn func() error) {
	if c.concurrentQueue == nil {
		c.concurrentQueue = newConcurrentQueue(c.Core.Context(), c.concurrency)
	}

	// add the function to the queue
	c.concurrentQueue.add(fn)
}

// ConcurrentQueueWait waits for all queued functions to finish
func (c *Common) ConcurrentQueueWait() error {
	if c.concurrentQueue == nil {
		return nil // no functions to wait for
	}

	// wait for all queued functions to finish
	return c.concurrentQueue.wait()
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
	return model.HasAnyPrefix(record, c.metadataPrefix)
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
	if len(funcs) == 0 {
		return nil // nothing to run
	}

	cq := newConcurrentQueue(ctx, concurrencyLimit)
	for _, fn := range funcs {
		cq.add(fn)
	}
	return cq.wait()
}

// concurrentQueue is a simple concurrent queue that allows adding functions to be run concurrently
// It uses a semaphore to limit the number of concurrent tasks and a wait group to wait for all tasks to finish.
type concurrentQueue struct {
	ctx    context.Context
	cancel context.CancelFunc
	sem    chan struct{}
	wg     sync.WaitGroup
	errCh  chan error
}

func newConcurrentQueue(ctx context.Context, concurrencyLimit int) *concurrentQueue {
	ctx, cancel := context.WithCancel(ctx)
	return &concurrentQueue{
		ctx:    ctx,
		cancel: cancel,
		sem:    make(chan struct{}, concurrencyLimit),
		errCh:  make(chan error, 1),
	}
}

func (cq *concurrentQueue) add(fn func() error) {
	select {
	case cq.sem <- struct{}{}:
		cq.wg.Add(1)
		go func() {
			defer func() {
				<-cq.sem
				cq.wg.Done()
			}()

			select {
			case <-cq.ctx.Done():
				return
			default:
			}

			if err := fn(); err != nil {
				select {
				case cq.errCh <- err:
					cq.cancel() // cancel all other tasks
				default:
				}
			}
		}()
	case <-cq.ctx.Done():
		return
	}
}

func (cq *concurrentQueue) wait() error {
	cq.wg.Wait()
	select {
	case err := <-cq.errCh:
		return errors.WithStack(err)
	default:
		return nil
	}
}
