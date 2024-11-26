package io

import (
	"context"
	"fmt"
	"time"

	"github.com/goto/optimus-any2any/pkg/flow"
)

type IOSink struct {
	ctx  context.Context
	done chan uint8
	c    chan any
}

var _ flow.Sink = (*IOSink)(nil)

func NewSink(ctx context.Context, opts ...flow.Option) *IOSink {
	s := &IOSink{
		ctx:  ctx,
		done: make(chan uint8),
		c:    make(chan any),
	}
	for _, opt := range opts {
		opt(s)
	}
	s.init()
	return s
}

func (s *IOSink) init() {
	go func() {
		defer func() {
			println("DEBUG: sink: close success")
			s.done <- 0
		}()
		for v := range s.c {
			println("DEBUG: sink: send:", string(v.([]byte)))
			fmt.Printf("%s\n", string(v.([]byte)))
			time.Sleep(2 * time.Second)
			println("DEBUG: sink: done:", string(v.([]byte)))
		}
	}()
}

func (s *IOSink) SetBufferSize(size int) {
	s.c = make(chan any, size)
}

func (s *IOSink) In() chan<- any {
	return s.c
}

func (s *IOSink) Wait() {
	<-s.done
	close(s.done)
}

func (mc *IOSink) Close() {
	println("DEBUG: sink: close")
}
