package graph

import (
	"context"

	"go.uber.org/zap"
	"golang.org/x/sync/singleflight"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/pkg/logger"
)

var (
	deduplicatedDispatchesCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "deduplicated_dispatches_counter",
		Help:      "The total number of calls to ResolveCheck that were deduplicated because of singleflight resolver.",
	})

	deduplicatedDBQueriesCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: build.ProjectName,
		Name:      "deduplicated_db_queries_counter",
		Help:      "The total number of DB queries that were deduplicated because of singleflight resolver.",
	})
)

type singleflightCheckResolver struct {
	delegate CheckResolver
	group    singleflight.Group
	logger   logger.Logger
}

// SingleflightCheckResolverOpt defines an option that can be used to change the behavior of singleflightCheckResolver
// instance.
type SingleflightCheckResolverOpt func(*singleflightCheckResolver)

func NewSingleflightCheckResolver(delegate CheckResolver, opts ...SingleflightCheckResolverOpt) *singleflightCheckResolver {
	s := &singleflightCheckResolver{
		delegate: delegate,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// Close implements CheckResolver.
func (s *singleflightCheckResolver) Close() {}

// ResolveCheck implements CheckResolver.
func (s *singleflightCheckResolver) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	key, err := CheckRequestCacheKey(req)
	if err != nil {
		s.logger.Error("singleflight cache key computation failed with error", zap.Error(err))
		return nil, err
	}

	isUnique := false
	r, err, shared := s.group.Do(key, func() (interface{}, error) {
		isUnique = true
		return s.delegate.ResolveCheck(ctx, req)
	})

	resp := r.(*ResolveCheckResponse)

	if shared && !isUnique {
		deduplicatedDispatchesCounter.Inc()
		deduplicatedDBQueriesCounter.Add(float64(resp.ResolutionMetadata.DatastoreQueryCount))
		resp.ResolutionMetadata.DatastoreQueryCount = 0
		resp.ResolutionMetadata.Depth = req.ResolutionMetadata.Depth
	}

	return resp, err
}
