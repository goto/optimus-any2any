package connector

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/itchyny/gojq"
)

func JQProcessor(l *slog.Logger, query string) flow.Connect {
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

				// Parse the input JSON into a Go map
				var input map[string]interface{}
				if err := json.Unmarshal(b, &input); err != nil {
					l.Error(fmt.Sprintf("connector: failed to parse input JSON: %v", err))
					continue
				}

				// Create an evaluator
				iter := jqQuery.Run(input)

				// Collect and format the result
				for {
					result, ok := iter.Next()
					if !ok {
						break
					}
					if err, ok := result.(error); ok {
						l.Error(fmt.Sprintf("Failed to evaluate jq query: %v", err))
						continue
					}

					// Print the transformed JSON
					outputJSON, err := json.Marshal(result)
					if err != nil {
						l.Error(fmt.Sprintf("connector: failed to marshal output JSON: %v", err))
						continue
					}
					l.Debug(fmt.Sprintf("connector: output JSON: %s", outputJSON))
					inlet.In() <- outputJSON
					l.Debug("connector: output JSON sent")
				}
			}
		}()
	}
}
