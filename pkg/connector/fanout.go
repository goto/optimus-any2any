package connector

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// FanOut is a connector that passes data from outlet to multiple inlets.
func FanOut(l *slog.Logger) flow.ConnectMultiSink {
	return func(outlet flow.Outlet, inlets ...flow.Inlet) {
		go func() {
			defer func() {
				l.Debug("connector(fanout): close")
				for _, inlet := range inlets {
					inlet.CloseInlet()
				}
			}()
			for v := range outlet.Out() {
				l.Debug(fmt.Sprintf("connector(fanout): send: %s", string(v.([]byte))))
				for _, inlet := range inlets {
					inlet.In(v)
				}
				l.Debug(fmt.Sprintf("connector(fanout): done: %s", string(v.([]byte))))
			}
		}()
	}
}
