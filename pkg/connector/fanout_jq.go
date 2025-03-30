package connector

import (
	"fmt"
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
				l.Debug("connector(fanoutjq): close")
				for _, inlet := range inlets {
					inlet.CloseInlet()
				}
			}()
			for v := range outlet.Out() {
				// Transform the input JSON using the given jq query
				outputJSON, err := JQBinaryTransformation(l, query, v)
				if err != nil {
					l.Error(fmt.Sprintf("connector(fanoutjq): failed to transform JSON: %v", err))
					continue
				}
				l.Debug(fmt.Sprintf("connector(fanoutjq): output JSON: %s", outputJSON))
				for _, inlet := range inlets {
					inlet.In(outputJSON)
				}
				l.Debug("connector(fanoutjq): output JSON sent")
			}
		}()
	}
}
