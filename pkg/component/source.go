package component

import (
	"context"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// CoreSource is an implementation of the source interface.
type CoreSource struct {
	*Core
}

var _ flow.Source = (*CoreSource)(nil)

// NewCoreSource creates a new CoreSource instance.
func NewCoreSource(ctx context.Context, cancelFn context.CancelCauseFunc, l *slog.Logger, name string) *CoreSource {
	c := &CoreSource{
		Core: NewCore(ctx, cancelFn, l, "source", name),
	}
	// special case for source to close the channel
	// after all processes are done
	c.postHookProcess = func() error {
		c.l.Debug("close inlet")
		return c.CloseInlet()
	}
	return c
}
