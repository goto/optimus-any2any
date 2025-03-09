package connector

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/itchyny/gojq"
)

// PassThroughWithJQ is a connector that passes data from outlet to inlet with jq query transformation.
func PassThroughWithJQ(l *slog.Logger, query string) flow.Connect {
	if query == "" {
		return PassThrough(l) // fallback to fanout
	}
	jqQuery, err := gojq.Parse(query)
	if err != nil {
		l.Error(fmt.Sprintf("connector(passthroughjq): failed to parse jq query: %v", err))
		return PassThrough(l) // fallback to passthrough
	}
	return func(outlet flow.Outlet, inlet flow.Inlet) {
		go func() {
			defer func() {
				l.Debug("connector(passthroughjq): close")
				close(inlet.In())
			}()

			for v := range outlet.Out() {
				b, ok := v.([]byte)
				if !ok {
					l.Error(fmt.Sprintf("connector(passthroughjq): message type assertion error: %T", v))
					continue
				}

				// Transform the input JSON using the given jq query
				for _, outputJSON := range JQTransformation(l, jqQuery, b) {
					l.Debug(fmt.Sprintf("connector(passthroughjq): output JSON: %s", outputJSON))
					inlet.In() <- outputJSON
					l.Debug("connector(passthroughjq): output JSON sent")
				}
			}
		}()
	}
}
