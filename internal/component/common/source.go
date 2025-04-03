package common

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/goccy/go-json"
	"github.com/goto/optimus-any2any/internal/model"
	"github.com/goto/optimus-any2any/pkg/component"
	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel/metric"
)

// Sender is an interface that defines a method to send data to a source.
type Sender interface {
	Send(v []byte)
}

// RecordWriter is an interface that defines a method to write records.
type RecordSender interface {
	SendRecord(*model.Record) error
}

// CommonSource is a common source that implements the flow.Source interface.
type CommonSource struct {
	*component.CoreSource
	*Common
}

var _ flow.Source = (*CommonSource)(nil)
var _ Sender = (*CommonSource)(nil)
var _ RecordSender = (*CommonSource)(nil)

// NewCommonSource creates a new CommonSource.
func NewCommonSource(l *slog.Logger, name string, opts ...Option) *CommonSource {
	coreSource := component.NewCoreSource(l, name)
	c := &CommonSource{
		CoreSource: coreSource,
		Common:     NewCommon(coreSource.Core),
	}
	for _, opt := range opts {
		opt(c.Common)
	}
	return c
}

// Send sends the given data to the source.
// This is a wrapper around the CoreSource's Send method.
func (c *CommonSource) Send(v []byte) {
	sendCount, err := c.m.Int64Counter("send_count", metric.WithDescription("The total number of data sent"))
	if err != nil {
		c.Logger().Error(fmt.Sprintf("send count error: %s", err.Error()))
	}
	sendBytes, err := c.m.Int64Counter("send_bytes", metric.WithDescription("The total number of bytes sent"), metric.WithUnit("bytes"))
	if err != nil {
		c.Logger().Error(fmt.Sprintf("send bytes error: %s", err.Error()))
	}
	sendCount.Add(context.Background(), 1)
	sendBytes.Add(context.Background(), int64(len(v)))

	c.Common.Core.In(v)
}

// SendRecord sends a record to the source.
// It marshals the record into JSON format and then sends it using the Send method.
func (c *CommonSource) SendRecord(record *model.Record) error {
	if record == nil {
		return nil
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return errors.WithStack(err)
	}
	c.Send(raw)
	return nil
}
