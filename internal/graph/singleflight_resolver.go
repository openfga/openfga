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

func NewSingleflightCheckResolver(delegate CheckResolver) *singleflightCheckResolver {
	return &singleflightCheckResolver{
		delegate: delegate,
	}
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
	singleFlightResp, err, shared := s.group.Do(key, func() (interface{}, error) {
		isUnique = true
		resp, err := s.delegate.ResolveCheck(ctx, req)
		if err != nil {
			return nil, err
		}
		return copyResolveResponse(*resp), nil
	})
	if err != nil {
		return nil, err
	}

	r := singleFlightResp.(ResolveCheckResponse)
	// Important to create a deferenced copy of the group.Do's response because because otherwise
	// it establishes a shared memory reference between the goroutines that were involved in the
	//de-duplication, and thus is subject to race conditions.
	resp := copyResolveResponse(r)

	if shared && !isUnique {
		deduplicatedDispatchesCounter.Inc()
		deduplicatedDBQueriesCounter.Add(float64(resp.GetResolutionMetadata().DatastoreQueryCount))
		resp.ResolutionMetadata.DatastoreQueryCount = 0
	}

	return &resp, err
}

func copyResolveResponse(original ResolveCheckResponse) ResolveCheckResponse {
	copy := ResolveCheckResponse{
		Allowed: original.GetAllowed(),
		ResolutionMetadata: &ResolutionMetadata{
			Depth:               original.GetResolutionMetadata().Depth,
			DatastoreQueryCount: original.GetResolutionMetadata().DatastoreQueryCount,
		},
	}

	return copy
}
