package singleflight

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/logger"
)

var (
	deduplicatedDispatchesounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "deduplicated_dispatches_counter",
		Help:      "The total number of calls to ResolveCheck that were deduplicated.",
	})
)

type singleflightCheckResolver struct {
	delegate graph.CheckResolver
	group    singleflight.Group
	logger   logger.Logger
}

var _ graph.CheckResolver = (*singleflightCheckResolver)(nil)

// SingleflightCheckResolverOpt defines an option that can be used to change the behavior of singleflightCheckResolver
// instance.
type SingleflightCheckResolverOpt func(*singleflightCheckResolver)

// WithLogger sets the logger for the singleflight check resolver
func WithLogger(logger logger.Logger) SingleflightCheckResolverOpt {
	return func(s *singleflightCheckResolver) {
		s.logger = logger
	}
}

func NewSingleflightCheckResolver(delegate graph.CheckResolver, opts ...SingleflightCheckResolverOpt) *singleflightCheckResolver {
	s := &singleflightCheckResolver{
		delegate: delegate,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Close implements graph.CheckResolver.
func (s *singleflightCheckResolver) Close() {}

// ResolveCheck implements graph.CheckResolver.
func (s *singleflightCheckResolver) ResolveCheck(
	ctx context.Context,
	req *graph.ResolveCheckRequest,
) (*graph.ResolveCheckResponse, error) {

	key, err := graph.CheckRequestCacheKey(req)
	if err != nil {
		s.logger.Error("singleflight cache key computation failed with error", zap.Error(err))
		return nil, err
	}

	resp, err, shared := s.group.Do(key, func() (interface{}, error) {
		return s.delegate.ResolveCheck(ctx, req)
	})

	if shared {
		deduplicatedDispatchesounter.Inc()
	}

	return resp.(*graph.ResolveCheckResponse), err
}
