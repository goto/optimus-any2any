package sink

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/goto/optimus-any2any/pkg/flow"
)

// CommonSink is a sink that provides commonSink functionality.
// It is used as a base for other sinks.
type CommonSink struct {
	Logger     *slog.Logger
	done       chan uint8
	c          chan any
	cleanFuncs []func()
}

var _ flow.Sink = (*CommonSink)(nil)
var _ option.SetupOptions = (*CommonSink)(nil)

// NewCommonSink creates a new commonSink sink.
// It will set up common functionality such as logging and clean functions.
func NewCommonSink(l *slog.Logger, opts ...option.Option) *CommonSink {
	commonSink := &CommonSink{
		Logger:     l,
		done:       make(chan uint8),
		c:          make(chan any),
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
}

func (commonSink *CommonSink) SetBufferSize(bufferSize int) {
	commonSink.c = make(chan any, bufferSize)
}

func (commonSink *CommonSink) SetOtelSDK() {

}

func (commonSink *CommonSink) SetLogger(logLevel string) {
	logger, err := logger.NewLogger(logLevel)
	if err != nil {
		commonSink.Logger.Error(fmt.Sprintf("sink: set logger error: %s", err.Error()))
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
