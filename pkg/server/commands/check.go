package commands

import (
	"context"
	"errors"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

type CheckQueryV2 struct {
	logger                    logger.Logger
	model                     *modelgraph.AuthorizationModelGraph
	datastore                 storage.RelationshipTupleReader
	cache                     storage.InMemoryCache[any]
	cacheTTL                  time.Duration
	lastCacheInvalidationTime time.Time
	planner                   planner.Manager
	concurrencyLimit          int
	upstreamTimeout           time.Duration
}

type CheckQueryV2Option func(*CheckQueryV2)

func WithCheckQueryV2Logger(l logger.Logger) CheckQueryV2Option {
	return func(c *CheckQueryV2) {
		c.logger = l
	}
}

func WithCheckQueryV2Datastore(ds storage.RelationshipTupleReader) CheckQueryV2Option {
	return func(c *CheckQueryV2) {
		c.datastore = ds
	}
}

func WithCheckQueryV2Model(m *modelgraph.AuthorizationModelGraph) CheckQueryV2Option {
	return func(c *CheckQueryV2) {
		c.model = m
	}
}

func WithCheckQueryV2Cache(c storage.InMemoryCache[any]) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.cache = c
	}
}

func WithCheckQueryV2CacheTTL(ttl time.Duration) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.cacheTTL = ttl
	}
}

func WithCheckQueryV2LastCacheInvalidationTime(t time.Time) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.lastCacheInvalidationTime = t
	}
}

func WithCheckQueryV2Planner(p planner.Manager) CheckQueryV2Option {
	return func(c *CheckQueryV2) {
		c.planner = p
	}
}

func WithCheckQueryV2ConcurrencyLimit(limit int) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.concurrencyLimit = limit
	}
}

func WithCheckQueryV2UpstreamTimeout(timeout time.Duration) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.upstreamTimeout = timeout
	}
}

func NewCheckQuery(opts ...CheckQueryV2Option) *CheckQueryV2 {
	q := &CheckQueryV2{
		logger: logger.NewNoopLogger(),
	}

	for _, opt := range opts {
		opt(q)
	}

	return q
}

func (q *CheckQueryV2) Execute(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, error) {
	r, err := check.NewRequest(check.RequestParams{
		StoreID:              req.GetStoreId(),
		AuthorizationModelID: q.model.GetModelID(),
		TupleKey:             tuple.ConvertCheckRequestTupleKeyToTupleKey(req.GetTupleKey()),
		ContextualTuples:     req.GetContextualTuples().GetTupleKeys(),
		Context:              req.GetContext(),
		Consistency:          req.GetConsistency(),
	})

	if err != nil {
		if errors.Is(err, check.ErrInvalidUser) {
			// TODO: Why is it invalid relation error and not invalid tuple error?
			return nil, &InvalidRelationError{Cause: err}
		}
		return nil, err
	}
	resolver := check.New(check.Config{
		Model:                     q.model,
		Datastore:                 q.datastore,
		Cache:                     q.cache,
		CacheTTL:                  q.cacheTTL,
		LastCacheInvalidationTime: q.lastCacheInvalidationTime,
		Planner:                   q.planner,
		ConcurrencyLimit:          q.concurrencyLimit,
		UpstreamTimeout:           q.upstreamTimeout,
		Logger:                    q.logger,
	})

	res, err := resolver.ResolveCheck(ctx, r)
	if err != nil {
		// handle panic/500
		if errors.Is(err, check.ErrValidation) {
			return nil, &InvalidRelationError{Cause: err}
		}
		return nil, err
	}

	return &openfgav1.CheckResponse{
		Allowed: res.Allowed,
	}, nil
}
