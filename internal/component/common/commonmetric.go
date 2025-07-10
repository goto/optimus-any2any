package common

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/goto/optimus-any2any/internal/otel"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/metric"
)

type commonmetric struct {
	m                 metric.Meter
	recordCount       metric.Int64Counter
	recordBytes       metric.Int64Counter
	recordBytesBucket metric.Int64Histogram
	concurrentLimits  atomic.Int64
	concurrentCount   atomic.Int64
	processDurationMs metric.Int64Histogram
	retryCount        metric.Int64Counter
}

func (cm *commonmetric) initializeMetrics(c component.Getter) error {
	cm.m = otel.GetMeter(c.Component(), c.Name())

	var err error

	// non-observable metrics
	if cm.recordCount, err = cm.m.Int64Counter(fmt.Sprintf(otel.Record, c.Component()), metric.WithDescription("The total number of records processed"), metric.WithUnit("1")); err != nil {
		return errors.WithStack(err)
	}
	if cm.recordBytes, err = cm.m.Int64Counter(fmt.Sprintf(otel.RecordBytes, c.Component()), metric.WithDescription("The total number of bytes processed"), metric.WithUnit("bytes")); err != nil {
		return errors.WithStack(err)
	}
	if cm.recordBytesBucket, err = cm.m.Int64Histogram(fmt.Sprintf(otel.RecordBytesBucket, c.Component()), metric.WithDescription("The total number of bytes processed in buckets"), metric.WithUnit("bytes")); err != nil {
		return errors.WithStack(err)
	}
	if cm.processDurationMs, err = cm.m.Int64Histogram(fmt.Sprintf(otel.ProcessDuration, c.Component()), metric.WithDescription("The duration of the process in milliseconds"), metric.WithUnit("ms")); err != nil {
		return errors.WithStack(err)
	}
	if cm.retryCount, err = cm.m.Int64Counter(fmt.Sprintf(otel.Retry, c.Component()), metric.WithDescription("The total number of retries performed"), metric.WithUnit("1")); err != nil {
		return errors.WithStack(err)
	}

	// observable metrics
	cm.concurrentLimits = atomic.Int64{}
	cm.concurrentCount = atomic.Int64{}
	processLimits, err := cm.m.Int64ObservableGauge(fmt.Sprintf(otel.ProcessLimits, c.Component()), metric.WithDescription("The current concurrency limit"))
	if err != nil {
		return errors.WithStack(err)
	}
	processCount, err := cm.m.Int64ObservableGauge(fmt.Sprintf(otel.Process, c.Component()), metric.WithDescription("The current number of processes running"))
	if err != nil {
		return errors.WithStack(err)
	}
	// register the callback to observe the current concurrency limits and count
	_, err = cm.m.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		o.ObserveInt64(processLimits, cm.concurrentLimits.Load())
		o.ObserveInt64(processCount, cm.concurrentCount.Load())
		return nil
	}, processLimits, processCount)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
