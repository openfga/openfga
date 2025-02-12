package workgroup

import (
	"context"
	"sync"
)

type Group[T any] interface {
	Push(context.Context, T) error
	Close() error
}

type boundGroup[T any] struct {
	wg      sync.WaitGroup
	limiter chan struct{}
	fn      func(T)
	once    sync.Once
}

func (p *boundGroup[T]) Push(ctx context.Context, t T) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	select {
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
		close(p.limiter)
	})
	p.wg.Wait()
	return nil
}

func Bound[T any](limit uint32, fn func(T)) Group[T] {
	limiter := make(chan struct{}, limit)
	return &boundGroup[T]{
		limiter: limiter,
		fn:      fn,
	}
}

type staticGroup[T any] struct {
	wg   sync.WaitGroup
	c    chan T
	fn   func(T)
	once sync.Once
}

func (p *staticGroup[T]) Push(ctx context.Context, t T) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	select {
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

func Static[T any](limit uint32, fn func(T)) Group[T] {
	c := make(chan T, limit)
	p := &staticGroup[T]{
		c:  c,
		fn: fn,
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
