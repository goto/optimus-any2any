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

// Source is a source that provides commonSource functionality.
// It is used as a base for other sources.
type Source struct {
	Logger *slog.Logger

	m          metric.Meter
	c          chan any
	err        error
	cleanFuncs []func() error
}

var _ flow.Source = (*Source)(nil)
var _ SetupOptions = (*Source)(nil)

// NewSource creates a new commonSource source.
// It will set up common functionality such as logging and clean functions.
func NewSource(l *slog.Logger, opts ...Option) *Source {
	commonSource := &Source{
		Logger:     l,
		m:          opentelemetry.GetMeterProvider().Meter("source"),
		c:          make(chan any),
		err:        nil,
		cleanFuncs: []func() error{},
	}

	for _, opt := range opts {
		opt(commonSource)
	}

	return commonSource
}

func (commonSource *Source) Out() <-chan any {
	return commonSource.c
}

func (commonSource *Source) Close() error {
	commonSource.Logger.Debug("close")
	var e error
	for _, clean := range commonSource.cleanFuncs {
		e = errs.Join(e, clean())
	}
	if e != nil {
		commonSource.Logger.Warn(fmt.Sprintf("close error: %s", e.Error()))
	}
	return e
}
func (commonSource *Source) SetName(name string) {
	commonSource.Logger = commonSource.Logger.WithGroup("source").With("name", name)
}

func (commonSource *Source) SetBufferSize(bufferSize int) {
	commonSource.c = make(chan any, bufferSize)
}

func (commonSource *Source) SetOtelSDK(ctx context.Context, otelCollectorGRPCEndpoint string, otelAttributes map[string]string) {
	commonSource.Logger.Debug(fmt.Sprintf("set otel sdk: %s", otelCollectorGRPCEndpoint))
	shutdownFunc, err := otel.SetupOTelSDK(ctx, otelCollectorGRPCEndpoint, otelAttributes)
	if err != nil {
		commonSource.Logger.Error(fmt.Sprintf("set otel sdk error: %s", err.Error()))
	}
	commonSource.AddCleanFunc(func() error {
		if err := shutdownFunc(); err != nil {
			commonSource.Logger.Error(fmt.Sprintf("otel sdk shutdown error: %s", err.Error()))
			return err
		}
		return nil
	})
}

func (commonSource *Source) SetLogger(logLevel string) {
	l, err := logger.NewLogger(logLevel)
	if err != nil {
		commonSource.Logger.Warn(fmt.Sprintf("set logger error: %s; use default", err.Error()))
		l = logger.NewDefaultLogger()
	}
	commonSource.Logger = l
}

func (commonSource *Source) SetRetry(int, int64) {
	// no implementation needed
}

// Send sends data to the channel.
// This is additional functionality that is not part of the flow.Source interface.
// It provides a way to send data to the channel without exposing the channel itself.
func (commonSource *Source) Send(v any) {
	// TODO: move metric related code to a separate function
	// capture count of sent data (this is just a sample on how to use metric)
	sendCount, err := commonSource.m.Int64Counter("send_count", metric.WithDescription("The total number of data sent"))
	if err != nil {
		commonSource.Logger.Error(fmt.Sprintf("send count error: %s", err.Error()))
	}
	sendBytes, err := commonSource.m.Int64Counter("send_bytes", metric.WithDescription("The total number of bytes sent"), metric.WithUnit("bytes"))
	if err != nil {
		commonSource.Logger.Error(fmt.Sprintf("send bytes error: %s", err.Error()))
	}
	sendCount.Add(context.Background(), 1)
	sendBytes.Add(context.Background(), int64(len(v.([]byte))))

	commonSource.c <- v
}

// AddCleanFunc adds a clean function to the source.
// Clean functions are called when the source is closed
// whether it is closed gracefully or due to an error.
func (commonSource *Source) AddCleanFunc(f func() error) {
	commonSource.cleanFuncs = append(commonSource.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function should read data from the source and send it to the channel.
// Please note that you should use the Send method to send data to the channel.
func (commonSource *Source) RegisterProcess(f func() error) {
	go func() {
		defer func() {
			commonSource.Logger.Debug(fmt.Sprintf("close success"))
			close(commonSource.c)
		}()
		if err := f(); err != nil {
			commonSource.Logger.Error(fmt.Sprintf("process error: %s", err.Error()))
			commonSource.setError(errors.WithStack(err))
		}
	}()
}

// SetError sets the error of the source.
// This is additional functionality that is not part of the flow.Source interface.
func (commonSource *Source) setError(err error) {
	commonSource.err = errors.WithStack(err)
}

// Err returns the error of the source.
func (commonSource *Source) Err() error {
	return commonSource.err
}
