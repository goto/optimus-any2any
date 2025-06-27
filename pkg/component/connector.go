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

	metadataPrefix   string
	batchSize        int
	batchIndexColumn string
}

// NewConnector creates a new Connector instance.
func NewConnector(ctx context.Context, cancelFn context.CancelCauseFunc, logger *slog.Logger, query string, metadataPrefix string, batchSize int, batchIndexColumn string) *Connector {
	name := "passthrough"
	if query != "" {
		name = "jq"
	}
	l := logger.WithGroup("connector").With("name", name)

	c := &Connector{
		Base:             NewBase(ctx, cancelFn, l),
		name:             name,
		query:            query,
		metadataPrefix:   metadataPrefix,
		batchSize:        batchSize,
		batchIndexColumn: batchIndexColumn,
	}
	return c
}

// Connect is a method that connects the source and multiple sinks.
func (c *Connector) Connect() flow.ConnectMultiSink {
	return func(o flow.Outlet, i ...flow.Inlet) {
		c.RegisterProcess(func() error {
			if c.query != "" {
				return transformWithJQ(c.ctx, c.l, c.query, c.metadataPrefix, c.batchSize, c.batchIndexColumn, o, i...)
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
