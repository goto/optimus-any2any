package sink

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/goto/optimus-any2any/internal/otel"
	"github.com/goto/optimus-any2any/pkg/flow"
	opentelemetry "go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// CommonSink is a sink that provides commonSink functionality.
// It is used as a base for other sinks.
type CommonSink struct {
	Logger     *slog.Logger
	m          metric.Meter
	done       chan uint8
	c          chan any
	err        error
	cleanFuncs []func()
}

var _ flow.Sink = (*CommonSink)(nil)
var _ option.SetupOptions = (*CommonSink)(nil)

// NewCommonSink creates a new commonSink sink.
// It will set up common functionality such as logging and clean functions.
func NewCommonSink(l *slog.Logger, opts ...option.Option) *CommonSink {
	commonSink := &CommonSink{
		Logger:     l,
		m:          opentelemetry.GetMeterProvider().Meter("source"),
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

func (commonSink *CommonSink) In() chan<- any {
	return commonSink.c
}

func (commonSink *CommonSink) Wait() {
	<-commonSink.done
	close(commonSink.done)
}

func (commonSink *CommonSink) Close() {
	commonSink.Logger.Debug("sink: close")
	for _, clean := range commonSink.cleanFuncs {
		clean()
	}
	close(commonSink.c)
}

func (commonSink *CommonSink) SetBufferSize(bufferSize int) {
	commonSink.c = make(chan any, bufferSize)
}

func (commonSink *CommonSink) SetOtelSDK(ctx context.Context, otelCollectorGRPCEndpoint string, otelAttributes map[string]string) {
	commonSink.Logger.Debug(fmt.Sprintf("sink: set otel sdk: %s", otelCollectorGRPCEndpoint))
	shutdownFunc, err := otel.SetupOTelSDK(ctx, otelCollectorGRPCEndpoint, otelAttributes)
	if err != nil {
		commonSink.Logger.Error(fmt.Sprintf("sink: set otel sdk error: %s", err.Error()))
	}
	commonSink.AddCleanFunc(func() {
		if err := shutdownFunc(); err != nil {
			commonSink.Logger.Error(fmt.Sprintf("source: otel sdk shutdown error: %s", err.Error()))
		}
	})
}

func (commonSink *CommonSink) SetLogger(logLevel string) {
	logger, err := logger.NewLogger(logLevel)
	if err != nil {
		commonSink.Logger.Error(fmt.Sprintf("sink: set logger error: %s", err.Error()))
		return
	}
	commonSink.Logger = logger
}

// Read reads data from the channel.
// This is additional functionality that is not part of the flow.Sink interface.
// It provides a way to read data from the channel without exposing the channel itself.
func (commonSink *CommonSink) Read() <-chan any {
	commonSink.Logger.Debug("sink: read")
	return commonSink.c
}

// AddCleanFunc adds a clean function to the source.
// Clean functions are called when the source is closed
// whether it is closed gracefully or due to an error.
func (commonSink *CommonSink) AddCleanFunc(f func()) {
	commonSink.cleanFuncs = append(commonSink.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function should read data from the channel and process it.
// Please note that you should use the Read method to read data from the channel.
func (commonSink *CommonSink) RegisterProcess(f func()) {
	go func() {
		defer func() {
			commonSink.Logger.Debug("sink: close success")
			commonSink.done <- 0
		}()
		f()
	}()
}

// SetError sets the error of the sink.
// This is additional functionality that is not part of the flow.Sink interface.
func (commonSink *CommonSink) SetError(err error) {
	commonSink.err = errors.Join(commonSink.err, err)
}

// Err returns the error of the sink.
func (commonSink *CommonSink) Err() error {
	return commonSink.err
}
