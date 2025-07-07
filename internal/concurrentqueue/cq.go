package concurrentqueue

import (
	"context"
	"sync"

	"github.com/pkg/errors"
)

// ConcurrentQueue is an interface that defines a simple concurrent queue
// that allows adding functions to be run concurrently.
type ConcurrentQueue interface {
	Submit(fn func() error) error
	Wait() error
}

// concurrentQueue is a simple concurrent queue that allows adding functions to be run concurrently
// It uses a semaphore to limit the number of concurrent tasks and a wait group to wait for all tasks to finish.
type concurrentQueue struct {
	ctx    context.Context
	cancel context.CancelCauseFunc
	sem    chan struct{}
	wg     sync.WaitGroup
	errCh  chan error
}

// NewConcurrentQueueWithCancel creates a new concurrent queue with a specified concurrency limit and a cancel function.
func NewConcurrentQueueWithCancel(ctx context.Context, cancel context.CancelCauseFunc, concurrencyLimit int) ConcurrentQueue {
	return &concurrentQueue{
		ctx:    ctx,
		cancel: cancel,
		sem:    make(chan struct{}, concurrencyLimit),
		errCh:  make(chan error, 1),
	}
}

// NewConcurrentQueue creates a new concurrent queue with a specified concurrency limit.
func NewConcurrentQueue(ctx context.Context, concurrencyLimit int) ConcurrentQueue {
	ctx, cancel := context.WithCancelCause(ctx)
	return NewConcurrentQueueWithCancel(ctx, cancel, concurrencyLimit)
}

// Submit adds a function to the queue to be executed concurrently.
func (cq *concurrentQueue) Submit(fn func() error) error {
	select {
	case cq.sem <- struct{}{}:
		cq.wg.Add(1)
		go func() {
			defer func() {
				cq.wg.Done()
				<-cq.sem
			}()

			if err := fn(); err != nil {
				select {
				case cq.errCh <- err:
					cq.cancel(errors.WithStack(err))
				default:
				}
			}
		}()
		return nil
	case <-cq.ctx.Done():
		select {
		case err := <-cq.errCh:
			return errors.WithStack(err)
		default:
			if err := cq.ctx.Err(); err != nil {
				return errors.WithStack(err)
			}
			return nil
		}
	}
}

// Wait waits for all submitted functions to finish and returns an error if any function returned an error.
func (cq *concurrentQueue) Wait() error {
	cq.wg.Wait()
	select {
	case err := <-cq.errCh:
		return errors.WithStack(err)
	default:
		return nil
	}
}
