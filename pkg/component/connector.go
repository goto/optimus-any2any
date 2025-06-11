package component

import (
	"context"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
)

// Connector is a struct that useful for connecting source and multiple sinks.
type Connector struct {
	*Base
	name  string
	query string

	batchSize        int
	batchIndexColumn string
	bufferSizeInMB   int
}

// NewConnector creates a new Connector instance.
func NewConnector(ctx context.Context, cancelFn context.CancelCauseFunc, logger *slog.Logger, query string, batchSize int, batchIndexColumn string, bufferSizeInMB int) *Connector {
	name := "passthrough"
	if query != "" {
		name = "jq"
	}
	l := logger.WithGroup("connector").With("name", name)

	c := &Connector{
		Base:             NewBase(ctx, cancelFn, l),
		name:             name,
		query:            query,
		batchSize:        batchSize,
		batchIndexColumn: batchIndexColumn,
		bufferSizeInMB:   bufferSizeInMB,
	}
	return c
}

// Connect is a method that connects the source and multiple sinks.
func (c *Connector) Connect() flow.ConnectMultiSink {
	return func(o flow.Outlet, i ...flow.Inlet) {
		c.RegisterProcess(func() error {
			if c.query != "" {
				return transformWithJQ(c.ctx, c.l, c.query, c.batchSize, c.batchIndexColumn, c.bufferSizeInMB, o, i...)
			}
			return passthrough(c.l, o, i...)
		})
		c.postHookProcess = func() error {
			c.l.Debug("close inlets")
			for _, inlet := range i {
				inlet.CloseInlet()
			}
			return nil
		}
		c.AddCleanFunc(func() error {
			c.l.Debug("connector close")
			return nil
		})
	}
}

func passthrough(_ *slog.Logger, outlet flow.Outlet, inlets ...flow.Inlet) error {
	for v := range outlet.Out() {
		for _, inlet := range inlets {
			inlet.In(v)
		}
	}
	return nil
}
