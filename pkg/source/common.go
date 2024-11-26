package source

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// Common is a source that provides common functionality.
// It is used as a base for other sources.
type Common struct {
	logger     *slog.Logger
	c          chan any
	cleanFuncs []func()
}

var _ flow.Source = (*Common)(nil)

// NewCommonSource creates a new common source.
func NewCommonSource(l *slog.Logger, opts ...flow.Option) *Common {
	common := &Common{
		logger:     l,
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

func (common *Common) Out() <-chan any {
	return common.c
}

func (common *Common) Close() {
	common.logger.Debug("source: close")
	for _, clean := range common.cleanFuncs {
		clean()
	}
}

// Send sends data to the channel.
// This is additional functionality that is not part of the flow.Source interface.
// It provides a way to send data to the channel without exposing the channel itself.
func (common *Common) Send(v any) {
	common.logger.Debug(fmt.Sprintf("source: send: %s", string(v.([]byte))))
	common.c <- v
	common.logger.Debug(fmt.Sprintf("source: done: %s", string(v.([]byte))))
}

// AddCleanFunc adds a clean function to the source.
// Clean functions are called when the source is closed
// whether it is closed gracefully or due to an error.
func (common *Common) AddCleanFunc(f func()) {
	common.cleanFuncs = append(common.cleanFuncs, f)
}

// RegisterProcess registers a process function that is run in a goroutine.
// The process function should read data from the source and send it to the channel.
// Please note that you should use the Send method to send data to the channel.
func (common *Common) RegisterProcess(f func()) {
	go func() {
		defer func() {
			common.logger.Debug("source: close success")
			close(common.c)
		}()
		f()
	}()
}
