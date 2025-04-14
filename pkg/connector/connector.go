package connector

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
)

type Connector struct {
	ctx      context.Context
	cancelFn context.CancelFunc
	l        *slog.Logger
	query    string
	err      error
}

func GetConnector(ctx context.Context, cancelFn context.CancelFunc, logger *slog.Logger, query string) *Connector {
	l := logger.WithGroup("connector").With("name", "jq")
	if query == "" {
		l = logger.WithGroup("connector").With("name", "passthrough")
	}
	c := &Connector{
		ctx:      ctx,
		cancelFn: cancelFn,
		l:        l,
		query:    query,
	}
	return c
}

func (c *Connector) Err() error {
	return c.err
}

func (c *Connector) Connect() flow.ConnectMultiSink {
	return c.connect
}

func (c *Connector) connect(outlet flow.Outlet, inlets ...flow.Inlet) {
	errChan := make(chan error)
	go func() {
		defer func() {
			c.l.Info("close inlets")
			for _, inlet := range inlets {
				inlet.CloseInlet()
			}
		}()
		if c.query != "" {
			errChan <- transformWithJQ(c.l, c.query, outlet, inlets...)
		} else {
			errChan <- passthrough(c.l, outlet, inlets...)
		}
	}()

	go func() {
		select {
		case <-c.ctx.Done():
			c.l.Info(fmt.Sprintf("context canceled: %s", c.ctx.Err()))
		case err := <-errChan:
			c.l.Info("done processing")
			if err != nil {
				c.l.Error(fmt.Sprintf("error: %s", err.Error()))
				c.err = errors.WithStack(err)
				c.cancelFn()
			}
		}
	}()
}
