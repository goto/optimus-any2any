package jq

import (
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/itchyny/gojq"
)

// JQProcessor is a processor that transforms JSON using jq.
type JQProcessor struct {
	i      chan any
	o      chan any
	query  string
	logger *slog.Logger
}

var _ flow.Processor = (*JQProcessor)(nil)

// NewJQProcessor creates a new JQ processor.
func NewJQProcessor(l *slog.Logger, query string, bufferSize int) (*JQProcessor, error) {
	jq := &JQProcessor{
		i:      make(chan any),
		o:      make(chan any),
		query:  query,
		logger: l,
	}
	if bufferSize > 0 {
		jq.i = make(chan any, bufferSize)
		jq.o = make(chan any, bufferSize)
	}

	go func() {
		defer close(jq.o)
		jq.process()
	}()
	return jq, nil
}

// process reads from the input channel, applies the jq query, and writes to the output channel.
func (j *JQProcessor) process() {
	for v := range j.i {
		b, ok := v.([]byte)
		if !ok {
			j.logger.Error(fmt.Sprintf("message type assertion error: %T", v))
			continue
		}

		// Parse the input JSON into a Go map
		var input map[string]interface{}
		if err := json.Unmarshal(b, &input); err != nil {
			j.logger.Error(fmt.Sprintf("Failed to parse input JSON: %v", err))
			continue
		}

		// Compile the jq query
		jqQuery, err := gojq.Parse(j.query)
		if err != nil {
			j.logger.Error(fmt.Sprintf("Failed to parse jq query: %v", err))
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
				j.logger.Error(fmt.Sprintf("Failed to evaluate jq query: %v", err))
				continue
			}

			// Print the transformed JSON
			outputJSON, err := json.Marshal(result)
			if err != nil {
				j.logger.Error(fmt.Sprintf("Failed to marshal output JSON: %v", err))
				continue
			}
			j.o <- outputJSON
		}
	}
}

// In returns the input channel.
func (j *JQProcessor) In() chan<- any {
	return j.i
}

// Out returns the output channel.
func (j *JQProcessor) Out() <-chan any {
	return j.o
}
