package throttler

import (
	"context"
	"time"
)

type Lessor interface {
	Acquire(context.Context) error
	Done()
}

type ImpatientLessor struct {
	throttle chan struct{}
	maxWait  time.Duration
}

func (l *ImpatientLessor) Acquire(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, l.maxWait)
	defer cancel()

	select {
	case l.throttle <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (l *ImpatientLessor) Done() {
	select {
	case <-l.throttle:
	default:
	}
}

func Impatient(n int, w time.Duration) Lessor {
	return &ImpatientLessor{
		throttle: make(chan struct{}, n),
		maxWait:  w,
	}
}

type NoopLessor struct{}

func (l NoopLessor) Acquire(ctx context.Context) error {
	return nil
}

func (l NoopLessor) Done() {}

func Noop() Lessor {
	return NoopLessor{}
}
