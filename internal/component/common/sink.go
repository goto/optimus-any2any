package common

import (
	"context"
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

	name       string
	m          metric.Meter
	done       chan uint8
	c          chan any
	err        error
	cleanFuncs []func()

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

		name:       "",
		m:          opentelemetry.GetMeterProvider().Meter("sink"),
		done:       make(chan uint8),
		c:          make(chan any),
		err:        nil,
		cleanFuncs: []func(){},
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

func (commonSink *Sink) Close() {
	commonSink.Logger.Debug(fmt.Sprintf("%s: close", commonSink.name))
	for _, clean := range commonSink.cleanFuncs {
		clean()
	}
}

func (commonSink *Sink) Name() string {
	return commonSink.name
}

func (commonSink *Sink) SetName(name string) {
	commonSink.Logger = commonSink.Logger.With("component_name", name)
	commonSink.name = name
}

func (commonSink *Sink) SetBufferSize(bufferSize int) {
	commonSink.c = make(chan any, bufferSize)
}

func (commonSink *Sink) SetOtelSDK(ctx context.Context, otelCollectorGRPCEndpoint string, otelAttributes map[string]string) {
	commonSink.Logger.Debug(fmt.Sprintf("%s: set otel sdk: %s", commonSink.name, otelCollectorGRPCEndpoint))
	shutdownFunc, err := otel.SetupOTelSDK(ctx, otelCollectorGRPCEndpoint, otelAttributes)
	if err != nil {
		commonSink.Logger.Error(fmt.Sprintf("%s: set otel sdk error: %s", commonSink.name, err.Error()))
	}
	commonSink.AddCleanFunc(func() {
		if err := shutdownFunc(); err != nil {
			commonSink.Logger.Error(fmt.Sprintf("%s: otel sdk shutdown error: %s", commonSink.name, err.Error()))
		}
	})
}

func (commonSink *Sink) SetLogger(logLevel string) {
	logger, err := logger.NewLogger(logLevel)
	if err != nil {
		commonSink.Logger.Warn(fmt.Sprintf("%s: set logger error: %s; use default", commonSink.name, err.Error()))
		logger = slog.Default()
	}
	commonSink.Logger = logger
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
func (commonSink *Sink) AddCleanFunc(f func()) {
	commonSink.cleanFuncs = append(commonSink.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function should read data from the channel and process it.
// Please note that you should use the Read method to read data from the channel.
func (commonSink *Sink) RegisterProcess(f func() error) {
	go func() {
		defer func() {
			commonSink.Logger.Debug(fmt.Sprintf("%s: close success", commonSink.name))
			commonSink.done <- 0
		}()
		if err := f(); err != nil {
			commonSink.Logger.Error(fmt.Sprintf("%s: process error: %s", commonSink.name, err.Error()))
			commonSink.setError(errors.WithStack(err))
		}
	}()
}

// SetError sets the error of the sink.
// This is additional functionality that is not part of the flow.Sink interface.
func (commonSink *Sink) setError(err error) {
	if commonSink.err == nil {
		go func() {
			commonSink.Logger.Debug(fmt.Sprintf("%s: skip message", commonSink.name))
			for _ = range commonSink.Read() {
			}
		}()
	}
	commonSink.err = errors.WithStack(err)
}

// Err returns the error of the sink.
func (commonSink *Sink) Err() error {
	return commonSink.err
}

// Retry retries the function f until it returns nil or the retry limit is reached.
func (commonSink *Sink) Retry(f func() error) error {
	return retry(commonSink.Logger, commonSink.retryMax, commonSink.retryBackoffMs, f)
}
