package common

import (
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
)

// CommonSink is a common sink that implements the flow.Sink interface.
type CommonSink struct {
	*component.CoreSink
	*Common
}

var _ flow.Sink = (*CommonSink)(nil)

// NewCommonSink creates a new CommonSink.
func NewCommonSink(l *slog.Logger, name string, opts ...Option) *CommonSink {
	coreSink := component.NewCoreSink(l, name)
	c := &CommonSink{
		CoreSink: coreSink,
		Common:   NewCommon(coreSink.Core),
	}
	for _, opt := range opts {
		opt(c.Common)
	}
	return c
}
