package graph

import (
	"context"
	"fmt"
)

type LimitedLocalChecker struct {
	resolver CheckResolver
	limiter  chan struct{}
}

type LimitedLocalCheckerOpt func(l *LimitedLocalChecker)

func WithCheckResolver(resolver CheckResolver) LimitedLocalCheckerOpt {
	return func(l *LimitedLocalChecker) {
		l.resolver = resolver
	}
}

func WithConcurrencyLimit(limit uint32) LimitedLocalCheckerOpt {
	return func(l *LimitedLocalChecker) {
		l.limiter = make(chan struct{}, limit)
	}
}

func MustNewLimitedLocalChecker(opts ...LimitedLocalCheckerOpt) *LimitedLocalChecker {
	checker, err := NewLimitedLocalChecker(opts...)
	if err != nil {
		panic(err)
	}

	return checker
}

func NewLimitedLocalChecker(opts ...LimitedLocalCheckerOpt) (*LimitedLocalChecker, error) {

	checker := &LimitedLocalChecker{
		limiter: make(chan struct{}, 50),
	}

	for _, opt := range opts {
		opt(checker)
	}

	if checker.resolver == nil {
		return nil, fmt.Errorf("the provided CheckResolver cannot be nil")
	}

	return checker, nil
}

func (l *LimitedLocalChecker) ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
	l.limiter <- struct{}{}
	defer func() {
		<-l.limiter
	}()

	return l.resolver.ResolveCheck(ctx, req)
}
