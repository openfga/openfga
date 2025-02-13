package workgroup

import (
	"context"
	"sync"
)

// Group is an interface that defines the capabilities
// of a Group. A Group is intended to concurrently
// perform a defined unit of work over an input set.
//
// Push sends a value to the Group for processing. Push
// may block if the Group cannot accept more inputs at
// that time. If the Group has been closed, Push will
// return an error. If the context has been canceled or
// reached its deadline, an error will be returned.
//
// Close will signal to the Group that it must finish processing
// and cleanup any resources, such as closing an internal channel.
// Close may block until all of the Group's go routines have
// completed.
type Group[T any] interface {
	Push(context.Context, T) error
	Close() error
}

// boundGroup is a type of Group that processes its
// input by spawning a new go routine for each input up
// to a specified limit. Once a go routine finishes processing
// its input, the go routine ends. If there is no input waiting
// to be processed, no go routines will be running in a boundGroup.
type boundGroup[T any] struct {
	wg      sync.WaitGroup
	limiter chan struct{}
	fn      func(T)
	once    sync.Once
	ctx     context.Context
	cancel  func()
}

func (p *boundGroup[T]) Push(ctx context.Context, t T) error {
	if p.ctx.Err() != nil {
		return p.ctx.Err()
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	select {
	case <-p.ctx.Done():
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case p.limiter <- struct{}{}:
		p.wg.Add(1)
		go func() {
			defer func() {
				<-p.limiter
				p.wg.Done()
			}()
			p.fn(t)
		}()
		return nil
	}
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
// running.
func Bound[T any](limit uint32, fn func(T)) Group[T] {
	limiter := make(chan struct{}, limit)
	ctx, cancel := context.WithCancel(context.Background())
	return &boundGroup[T]{
		limiter: limiter,
		fn:      fn,
		ctx:     ctx,
		cancel:  cancel,
	}
}

// staticGroup is a Group that spawns a static number of
// go routines that continue running until the staticGroup
// has been closed.
type staticGroup[T any] struct {
	wg     sync.WaitGroup
	c      chan T
	fn     func(T)
	once   sync.Once
	ctx    context.Context
	cancel func()
}

func (p *staticGroup[T]) Push(ctx context.Context, t T) error {
	if p.ctx.Err() != nil {
		return p.ctx.Err()
	}

	if ctx.Err() != nil {
		return ctx.Err()
	}

	select {
	case <-p.ctx.Done():
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	case p.c <- t:
		return nil
	}
}

func (p *staticGroup[T]) Close() error {
	p.once.Do(func() {
		close(p.c)
	})
	p.wg.Wait()
	return nil
}

// Static returns a Group that processes input by spawning
// a static number of go routines over function fn up to
// limit. The spawned go routines continue running for the
// lifetime of the Group, until the Group has been closed.
func Static[T any](limit uint32, fn func(T)) Group[T] {
	c := make(chan T, limit)
	ctx, cancel := context.WithCancel(context.Background())
	p := &staticGroup[T]{
		c:      c,
		fn:     fn,
		ctx:    ctx,
		cancel: cancel,
	}

	for range limit {
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			for t := range c {
				fn(t)
			}
		}()
	}

	return p
}
