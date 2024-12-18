package connector

import (
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/itchyny/gojq"
)

// FanOutWithJQ is a connector that passes data from outlet to multiple inlets with jq query transformation.
func FanOutWithJQ(l *slog.Logger, query string) flow.ConnectMultiSink {
	jqQuery, err := gojq.Parse(query)
	if err != nil {
		l.Error(fmt.Sprintf("connector: failed to parse jq query: %v", err))
		return FanOut(l) // fallback to fanout
	}
	return func(outlet flow.Outlet, inlets ...flow.Inlet) {
		go func() {
			defer func() {
				l.Debug("connector: close")
				for _, inlet := range inlets {
					close(inlet.In())
				}
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
					for _, inlet := range inlets {
						inlet.In() <- outputJSON
					}
					l.Debug("connector: output JSON sent")
				}
			}
		}()
	}
}
