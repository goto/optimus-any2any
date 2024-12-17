package connector

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/itchyny/gojq"
)

// JQPassthrough is a connector that passes data from outlet to inlet with jq query transformation.
func JQPassthrough(l *slog.Logger, query string) flow.Connect {
	jqQuery, err := gojq.Parse(query)
	if err != nil {
		l.Error(fmt.Sprintf("connector: failed to parse jq query: %v", err))
		return PassThrough(l) // fallback to passthrough
	}
	return func(outlet flow.Outlet, inlet flow.Inlet) {
		go func() {
			defer func() {
				l.Debug("connector: close")
				close(inlet.In())
			}()

			for v := range outlet.Out() {
				b, ok := v.([]byte)
				if !ok {
					l.Error(fmt.Sprintf("connector: message type assertion error: %T", v))
					continue
				}

				// Transform the input JSON using the given jq query
				for _, outputJSON := range JQTransformation(l, jqQuery, b) {
					l.Debug(fmt.Sprintf("connector: output JSON: %s", outputJSON))
					inlet.In() <- outputJSON
					l.Debug("connector: output JSON sent")
				}
			}
		}()
	}
}
