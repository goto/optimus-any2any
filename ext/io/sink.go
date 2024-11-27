package io

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/internal/component/option"
	"github.com/goto/optimus-any2any/internal/component/sink"
	"github.com/goto/optimus-any2any/pkg/flow"
)

type IOSink struct {
	*sink.CommonSink
	w io.Writer
}

var _ flow.Sink = (*IOSink)(nil)

func NewSink(l *slog.Logger, opts ...option.Option) *IOSink {
	// create common
	commonSink := sink.NewCommonSink(l, opts...)
	s := &IOSink{
		CommonSink: commonSink,
		w:          os.Stdout,
	}

	// add clean func
	commonSink.AddCleanFunc(func() {
		commonSink.Logger.Debug("sink: close func called")
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s
}

func (s *IOSink) process() {
	// read from channel
	for v := range s.Read() {
		s.Logger.Debug(fmt.Sprintf("sink: read: %s", string(v.([]byte))))
		fmt.Fprintf(s.w, "%s\n", string(v.([]byte)))
		s.Logger.Debug(fmt.Sprintf("sink: done: %s", string(v.([]byte))))
	}
}
