package workgroup

import (
	"context"
	"fmt"
	"sync"
)

// Group is an interface that defines the capabilities
// of a Group. A Group is intended to concurrently
// perform a defined unit of work over an input set.
//
// Push sends a value to the Group for processing and returns
// a receive channel that will either emit an error or be closed
// when the associated input has been processed. Push may block
// if the Group cannot accept more inputs at that time. If the
// Group has been closed, the receive channel will emit an error.
// If the context has been canceled or reached its deadline, the
// receive channel will emit an error.
//
// Close will signal to the Group that it must finish processing
// and cleanup any resources, such as closing an internal channel.
// Close may block until all of the Group's go routines have
// completed.
type Group[T any] interface {
	Push(context.Context, T) <-chan error
	Close() error
}

// boundGroup is a type of Group that processes its input by
// spawning a new go routine for each input up to a specified
// limit. Once a go routine finishes processing its input, the
// go routine ends. If there is no input waiting to be processed,
// no go routines will be running in a boundGroup.
type boundGroup[T any] struct {
	wg      sync.WaitGroup
	limiter chan struct{}
	fn      func(T) error
	once    sync.Once
	ctx     context.Context
	cancel  func()
}

func (p *boundGroup[T]) Push(ctx context.Context, t T) <-chan error {
	ch := make(chan error, 1)

	if p.ctx.Err() != nil {
		ch <- p.ctx.Err()
		return ch
	}

	if ctx.Err() != nil {
		ch <- ctx.Err()
		return ch
	}

	select {
	case <-p.ctx.Done():
		ch <- ctx.Err()
	case <-ctx.Done():
		ch <- ctx.Err()
	case p.limiter <- struct{}{}:
		p.wg.Add(1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					ch <- fmt.Errorf("%s", r)
				}
				<-p.limiter
				close(ch)
				p.wg.Done()
			}()
			err := p.fn(t)
			if err != nil {
				ch <- err
			}
		}()
	}
	return ch
}

func (p *boundGroup[T]) Close() error {
	p.once.Do(func() {
		p.cancel()
		close(p.limiter)
	})
	p.wg.Wait()
	return nil
}

// Bound returns a Group that processes input by spawning
// a new go routine over function fn for each input up to
// limit. Once a go routine finishes processing its input,
// it will end. When the Group is idle, no go routines are
// running. If the function fn returns an error, that error
// will be emitted by the receive channel returned from the
// call to Push.
func Bound[T any](limit uint32, fn func(T) error) Group[T] {
	limiter := make(chan struct{}, limit)
	ctx, cancel := context.WithCancel(context.Background())
	return &boundGroup[T]{
		limiter: limiter,
		fn:      fn,
		ctx:     ctx,
		cancel:  cancel,
	}
}
