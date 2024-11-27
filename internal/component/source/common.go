package source

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/logger"
	"github.com/goto/optimus-any2any/pkg/flow"
)

// CommonSource is a source that provides commonSource functionality.
// It is used as a base for other sources.
type CommonSource struct {
	Logger     *slog.Logger
	c          chan any
	cleanFuncs []func()
}

var _ flow.Source = (*CommonSource)(nil)
var _ option.SetupOptions = (*CommonSource)(nil)

// NewCommonSource creates a new commonSource source.
// It will set up common functionality such as logging and clean functions.
func NewCommonSource(l *slog.Logger, opts ...option.Option) *CommonSource {
	commonSource := &CommonSource{
		Logger:     l,
		c:          make(chan any),
		cleanFuncs: []func(){},
	}

	for _, opt := range opts {
		opt(commonSource)
	}

	return commonSource
}

func (commonSource *CommonSource) Out() <-chan any {
	return commonSource.c
}

func (commonSource *CommonSource) Close() {
	commonSource.Logger.Debug("source: close")
	for _, clean := range commonSource.cleanFuncs {
		clean()
	}
}

func (commonSource *CommonSource) SetBufferSize(bufferSize int) {
	commonSource.c = make(chan any, bufferSize)
}

func (commonSource *CommonSource) SetOtelSDK() {

}

func (commonSource *CommonSource) SetLogger(logLevel string) {
	logger, err := logger.NewLogger(logLevel)
	if err != nil {
		commonSource.Logger.Error(fmt.Sprintf("source: set logger error: %s", err.Error()))
	}
	commonSource.Logger = logger
}

// Send sends data to the channel.
// This is additional functionality that is not part of the flow.Source interface.
// It provides a way to send data to the channel without exposing the channel itself.
func (commonSource *CommonSource) Send(v any) {
	commonSource.Logger.Debug(fmt.Sprintf("source: send: %s", string(v.([]byte))))
	commonSource.c <- v
	commonSource.Logger.Debug(fmt.Sprintf("source: done: %s", string(v.([]byte))))
}

// AddCleanFunc adds a clean function to the source.
// Clean functions are called when the source is closed
// whether it is closed gracefully or due to an error.
func (commonSource *CommonSource) AddCleanFunc(f func()) {
	commonSource.cleanFuncs = append(commonSource.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function should read data from the source and send it to the channel.
// Please note that you should use the Send method to send data to the channel.
func (commonSource *CommonSource) RegisterProcess(f func()) {
	go func() {
		defer func() {
			commonSource.Logger.Debug("source: close success")
			close(commonSource.c)
		}()
		f()
	}()
}
