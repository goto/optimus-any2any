package io

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/goto/optimus-any2any/pkg/flow"
	"github.com/goto/optimus-any2any/pkg/sink"
)

type IOSink struct {
	*sink.Common
	logger *slog.Logger
	w      io.Writer
}

var _ flow.Sink = (*IOSink)(nil)

func NewSink(l *slog.Logger, opts ...flow.Option) *IOSink {
	// create common
	common := sink.NewCommon(l, opts...)
	s := &IOSink{
		Common: common,
		logger: l,
		w:      os.Stdout,
	}

	// add clean func
	common.AddCleanFunc(func() {
		l.Debug("sink: close func called")
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	common.RegisterProcess(s.process)

	return s
}

func (s *IOSink) process() {
	// read from channel
	for v := range s.Read() {
		s.logger.Debug(fmt.Sprintf("sink: read: %s", string(v.([]byte))))
		fmt.Fprintf(s.w, "%s\n", string(v.([]byte)))
		s.logger.Debug(fmt.Sprintf("sink: done: %s", string(v.([]byte))))
	}
}
