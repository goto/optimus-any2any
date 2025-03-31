package connector

import (
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// PassThroughWithJQ is a connector that passes data from outlet to inlet with jq query transformation.
func PassThroughWithJQ(l *slog.Logger, query string) flow.Connect {
	if query == "" {
		return PassThrough(l) // fallback to fanout
	}
	return func(outlet flow.Outlet, inlet flow.Inlet) {
		go func() {
			defer func() {
				l.Debug("connector(passthroughjq): close")
				inlet.CloseInlet()
			}()

			transformWithJQ(l, query, outlet, inlet)
		}()
	}
}
