package common

import (
	"context"
	errs "errors"
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/goto/optimus-any2any/internal/otel"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	opentelemetry "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// Sink is a sink that provides commonSink functionality.
// It is used as a base for other sinks.
type Sink struct {
	Logger         *slog.Logger
	MetadataPrefix string

	m          metric.Meter
	done       chan uint8
	c          chan any
	err        error
	cleanFuncs []func() error

	retryMax       int
	retryBackoffMs int64
}

var _ flow.Sink = (*Sink)(nil)
var _ SetupOptions = (*Sink)(nil)

// NewSink creates a new commonSink sink.
// It will set up common functionality such as logging and clean functions.
func NewSink(l *slog.Logger, metadataPrefix string, opts ...Option) *Sink {
	commonSink := &Sink{
		Logger:         l,
		MetadataPrefix: metadataPrefix,

		m:          opentelemetry.GetMeterProvider().Meter("sink"),
		done:       make(chan uint8),
		c:          make(chan any),
		err:        nil,
		cleanFuncs: []func() error{},
	}

	for _, opt := range opts {
		opt(commonSink)
	}

	return commonSink
}

func (commonSink *Sink) In() chan<- any {
	return commonSink.c
}

func (commonSink *Sink) Wait() {
	<-commonSink.done
	close(commonSink.done)
}

func (commonSink *Sink) Close() error {
	commonSink.Logger.Debug("close")
	var e error
	for _, clean := range commonSink.cleanFuncs {
		e = errs.Join(e, clean())
	}
	if e != nil {
		commonSink.Logger.Warn(fmt.Sprintf("close error: %s", e.Error()))
	}
	return e
}

func (commonSink *Sink) SetName(name string) {
	commonSink.Logger = commonSink.Logger.WithGroup("sink").With("name", name)
}

func (commonSink *Sink) SetBufferSize(bufferSize int) {
	commonSink.c = make(chan any, bufferSize)
}

func (commonSink *Sink) SetOtelSDK(ctx context.Context, otelCollectorGRPCEndpoint string, otelAttributes map[string]string) {
	commonSink.Logger.Debug(fmt.Sprintf("set otel sdk: %s", otelCollectorGRPCEndpoint))
	shutdownFunc, err := otel.SetupOTelSDK(ctx, otelCollectorGRPCEndpoint, otelAttributes)
	if err != nil {
		commonSink.Logger.Error(fmt.Sprintf("set otel sdk error: %s", err.Error()))
	}
	commonSink.AddCleanFunc(func() error {
		if err := shutdownFunc(); err != nil {
			commonSink.Logger.Error(fmt.Sprintf("otel sdk shutdown error: %s", err.Error()))
			return errors.WithStack(err)
		}
		return nil
	})
}

func (commonSink *Sink) SetLogger(logLevel string) {
	l, err := logger.NewLogger(logLevel)
	if err != nil {
		commonSink.Logger.Warn(fmt.Sprintf("set logger error: %s; use default", err.Error()))
		l = logger.NewDefaultLogger()
	}
	commonSink.Logger = l
}

func (commonSink *Sink) SetRetry(retryMax int, retryBackoffMs int64) {
	commonSink.retryMax = retryMax
	commonSink.retryBackoffMs = retryBackoffMs
}

// Read reads data from the channel.
// This is additional functionality that is not part of the flow.Sink interface.
// It provides a way to read data from the channel without exposing the channel itself.
func (commonSink *Sink) Read() <-chan any {
	return commonSink.c
}

// AddCleanFunc adds a clean function to the source.
// Clean functions are called when the source is closed
// whether it is closed gracefully or due to an error.
func (commonSink *Sink) AddCleanFunc(f func() error) {
	commonSink.cleanFuncs = append(commonSink.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function should read data from the channel and process it.
// Please note that you should use the Read method to read data from the channel.
func (commonSink *Sink) RegisterProcess(f func() error) {
	go func() {
		defer func() {
			commonSink.Logger.Debug(fmt.Sprintf("close success"))
			commonSink.done <- 0
		}()
		if err := f(); err != nil {
			commonSink.Logger.Error(fmt.Sprintf("process error: %s", err.Error()))
			commonSink.err = errors.WithStack(err)
		}
		commonSink.Logger.Debug(fmt.Sprintf("skip message"))
		for _, ok := <-commonSink.Read(); ok; _, ok = <-commonSink.Read() {
			// drain the channel if it still contains messages
		}
	}()
}

// Err returns the error of the sink.
func (commonSink *Sink) Err() error {
	return commonSink.err
}

// Retry retries the function f until it returns nil or the retry limit is reached.
func (commonSink *Sink) Retry(f func() error) error {
	return retry(commonSink.Logger, commonSink.retryMax, commonSink.retryBackoffMs, f)
}
