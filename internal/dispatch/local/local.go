package local

import (
	"context"

	"github.com/openfga/openfga/internal/dispatch"
	"github.com/openfga/openfga/internal/graph/check"
	dispatchv1 "github.com/openfga/openfga/internal/proto/dispatch/v1alpha1"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

type localDispatcher struct {
	checkResolver      *check.CheckResolver
	typesystemResolver typesystem.TypesystemResolverFunc

	limits localDispatcherLimits
}

type localDispatcherLimits struct {
	maxDispatchBatchSize    int
	maxConcurrentDispatches int
	maxReadQueriesInflight  int
}

type LocalDispatcherOpt func(l *localDispatcher)

// WithMaxConcurrentDispatches sets the maximum number of concurrent (outbound) dispatches for a given branch of resolution
// w.r.t. direct relationships involving non-terminal subproblems (e.g. usersets requiring further dispatch).
func WithMaxConcurrentDispatches(concurrentDispatches int) LocalDispatcherOpt {
	return func(c *localDispatcher) {
		c.limits.maxConcurrentDispatches = concurrentDispatches
	}
}

// WithMaxDispatchBatchSize sets the maximum size of a batch of objects ids to be included in a
// single dispatched request.
func WithMaxDispatchBatchSize(batchSize int) LocalDispatcherOpt {
	return func(l *localDispatcher) {
		l.limits.maxDispatchBatchSize = batchSize
	}
}

// WithMaxReadQueriesInflight sets the maximum number of concurrent (outbound) queries to the storage
// layer that can be in flight.
func WithMaxReadQueriesInflight(maxQueriesInflight int) LocalDispatcherOpt {
	return func(c *localDispatcher) {
		c.limits.maxReadQueriesInflight = maxQueriesInflight
	}
}

// NewLocalDispatcher constructs a LocalDispatcher which can be used to dispatch Check subproblems
// to the localized CheckResolver constructed for this LocalDispatcher..
func NewLocalDispatcher(
	tupleReader storage.RelationshipTupleReader,
	typesystemResolver typesystem.TypesystemResolverFunc,
	opts ...LocalDispatcherOpt,
) *localDispatcher {
	l := &localDispatcher{
		typesystemResolver: typesystemResolver,
		limits: localDispatcherLimits{
			maxDispatchBatchSize:    check.DefaultMaxDispatchBatchSize,
			maxConcurrentDispatches: check.DefaultMaxConcurrentDispatches,
			maxReadQueriesInflight:  check.DefaultMaxReadQueriesInflight,
		},
	}

	for _, opt := range opts {
		opt(l)
	}

	checkResolver := check.NewCheckResolver(
		tupleReader,
		check.WithDispatcher(l),
		check.WithMaxConcurrentDispatches(l.limits.maxConcurrentDispatches),
		check.WithMaxDispatchBatchSize(l.limits.maxDispatchBatchSize),
		check.WithMaxReadQueriesInflight(l.limits.maxReadQueriesInflight),
	)
	l.checkResolver = checkResolver

	return l
}

var _ dispatch.CheckDispatcher = (*localDispatcher)(nil)

// DispatchCheck implements dispatch.CheckDispatcher with local dispatch composition.
func (l *localDispatcher) DispatchCheck(
	ctx context.Context,
	req *dispatchv1.DispatchCheckRequest,
) (*dispatchv1.DispatchCheckResponse, error) {
	typesys, err := l.typesystemResolver(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	return l.checkResolver.Check(ctx, &check.CheckRequest{
		StoreID:         req.GetStoreId(),
		Typesystem:      typesys,
		ObjectType:      req.GetObjectType(),
		ObjectIDs:       req.GetObjectIds(),
		Relation:        req.GetRelation(),
		SubjectType:     req.GetSubjectType(),
		SubjectID:       req.GetSubjectId(),
		SubjectRelation: req.GetSubjectRelation(),
	})
}
