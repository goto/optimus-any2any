package connector

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// PassThrough is a connector that passes data from outlet to inlet.
func PassThrough(l *slog.Logger) flow.Connect {
	return func(outlet flow.Outlet, inlet flow.Inlet) {
		defer func() {
			l.Debug("connector: close")
			close(inlet.In())
		}()
		for v := range outlet.Out() {
			l.Debug(fmt.Sprintf("connector: send: %s", string(v.([]byte))))
			inlet.In() <- v
			l.Debug(fmt.Sprintf("connector: done: %s", string(v.([]byte))))
		}
	}
}
