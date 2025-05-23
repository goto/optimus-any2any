package io

import (
	"fmt"
	"io"
	"os"

	"github.com/goto/optimus-any2any/internal/component/common"
	"github.com/goto/optimus-any2any/pkg/flow"
)

type IOSink struct {
	common.Sink
	w io.Writer
}

var _ flow.Sink = (*IOSink)(nil)

func NewSink(commonSink common.Sink) *IOSink {
	s := &IOSink{
		Sink: commonSink,
		w:    os.Stdout,
	}

	// add clean func
	commonSink.AddCleanFunc(func() error {
		s.Logger().Debug("close func called")
		return nil
	})
	// register process, it will immediately start the process
	// in a separate goroutine
	commonSink.RegisterProcess(s.process)

	return s
}

func (s *IOSink) process() error {
	// read from channel
	for v := range s.Read() {
		s.Logger().Debug(fmt.Sprintf("read: %s", string(v)))
		fmt.Fprintf(s.w, "%s\n", string(v))
		s.Logger().Debug(fmt.Sprintf("done: %s", string(v)))
	}
	return nil
}
