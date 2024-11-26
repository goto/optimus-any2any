package sink

import (
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

type Common struct {
	logger     *slog.Logger
	done       chan uint8
	c          chan any
	cleanFuncs []func()
}

var _ flow.Sink = (*Common)(nil)

func NewCommon(l *slog.Logger, opts ...flow.Option) *Common {
	common := &Common{
		logger:     l,
		done:       make(chan uint8),
		c:          make(chan any),
		cleanFuncs: []func(){},
	}

	// apply options
	for _, opt := range opts {
		opt(common)
	}
	return common
}

func (common *Common) SetBufferSize(size int) {
	common.c = make(chan any, size)
}

func (common *Common) In() chan<- any {
	return common.c
}

func (common *Common) Wait() {
	<-common.done
	close(common.done)
}

func (common *Common) Close() {
	common.logger.Debug("sink: close")
	for _, clean := range common.cleanFuncs {
		clean()
	}
}

// Read reads data from the channel.
// This is additional functionality that is not part of the flow.Sink interface.
// It provides a way to read data from the channel without exposing the channel itself.
func (common *Common) Read() <-chan any {
	common.logger.Debug("sink: read")
	return common.c
}

// AddCleanFunc adds a clean function to the source.
// Clean functions are called when the source is closed
// whether it is closed gracefully or due to an error.
func (common *Common) AddCleanFunc(f func()) {
	common.cleanFuncs = append(common.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function should read data from the channel and process it.
// Please note that you should use the Read method to read data from the channel.
func (common *Common) RegisterProcess(f func()) {
	go func() {
		defer func() {
			common.logger.Debug("sink: close success")
			common.done <- 0
		}()
		f()
	}()
}
