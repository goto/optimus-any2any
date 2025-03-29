package common

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"go.opentelemetry.io/otel/metric"
)

// CommonSource is a common source that implements the flow.Source interface.
type CommonSource struct {
	*component.CoreSource
	*Common
}

var _ flow.Source = (*CommonSource)(nil)

// NewCommonSource creates a new CommonSource.
func NewCommonSource(l *slog.Logger, name string, opts ...Option) *CommonSource {
	coreSource := component.NewCoreSource(l, name)
	c := &CommonSource{
		CoreSource: coreSource,
		Common:     NewCommon(coreSource.Core, coreSource.Component()),
	}
	for _, opt := range opts {
		opt(c.Common)
	}
	return c
}

// Send sends the given data to the source.
// This is a wrapper around the CoreSource's Send method.
func (c *CommonSource) Send(v any) {
	// TODO: move metric related code to a separate function
	// capture count of sent data (this is just a sample on how to use metric)
	sendCount, err := c.m.Int64Counter("send_count", metric.WithDescription("The total number of data sent"))
	if err != nil {
		c.Logger().Error(fmt.Sprintf("send count error: %s", err.Error()))
	}
	sendBytes, err := c.m.Int64Counter("send_bytes", metric.WithDescription("The total number of bytes sent"), metric.WithUnit("bytes"))
	if err != nil {
		c.Logger().Error(fmt.Sprintf("send bytes error: %s", err.Error()))
	}
	sendCount.Add(context.Background(), 1)
	sendBytes.Add(context.Background(), int64(len(v.([]byte))))

	c.CoreSource.Send(v)
}
