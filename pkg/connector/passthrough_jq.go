package connector

import (
	"fmt"
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

			for v := range outlet.Out() {
				// Transform the input JSON using the given jq query
				outputJSON, err := JQBinaryTransformation(l, query, v)
				if err != nil {
					l.Error(fmt.Sprintf("connector(passthroughjq): failed to transform JSON: %v", err))
					continue
				}
				l.Debug(fmt.Sprintf("connector(passthroughjq): output JSON: %s", outputJSON))
				inlet.In(outputJSON)
				l.Debug("connector(passthroughjq): output JSON sent")
			}
		}()
	}
}
