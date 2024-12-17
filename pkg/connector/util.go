package connector

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/itchyny/gojq"
)

// JQTransformation transforms the input JSON using the given jq query.
func JQTransformation(l *slog.Logger, jqQuery *gojq.Query, input []byte) [][]byte {
	var inputMap map[string]interface{}
	if err := json.Unmarshal(input, &inputMap); err != nil {
		l.Error(fmt.Sprintf("failed to parse input JSON: %v", err))
		return nil
	}

	// Create an evaluator
	iter := jqQuery.Run(inputMap)

	// Collect and format the result
	var outputs [][]byte
	for {
		result, ok := iter.Next()
		if !ok {
			break
		}
		if err, ok := result.(error); ok {
			l.Error(fmt.Sprintf("failed to evaluate jq query: %v", err))
			continue
		}

		outputJSON, err := json.Marshal(result)
		if err != nil {
			l.Error(fmt.Sprintf("failed to marshal output JSON: %v", err))
			continue
		}
		outputs = append(outputs, outputJSON)
	}

	return outputs
}
