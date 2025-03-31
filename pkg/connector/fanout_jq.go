package connector

import (
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// FanOutWithJQ is a connector that passes data from outlet to multiple inlets with jq query transformation.
func FanOutWithJQ(l *slog.Logger, query string) flow.ConnectMultiSink {
	if query == "" {
		return FanOut(l) // fallback to fanout
	}
	return func(outlet flow.Outlet, inlets ...flow.Inlet) {
		go func() {
			defer func() {
				l.Debug("connector(passthroughjq): close")
				for _, inlet := range inlets {
					inlet.CloseInlet()
				}
			}()
			transformWithJQ(l, query, outlet, inlets...)
		}()
	}
}
