package commands

import (
	"context"
	"errors"
	"fmt"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/check"
	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/internal/shared"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
)

// V2CheckMethodName is used to differentiate v2Check from base Check for metric reporting when both are running.
const V2CheckMethodName = "v2Check"

type CheckQueryV2 struct {
	logger                    logger.Logger
	model                     *modelgraph.AuthorizationModelGraph
	datastore                 storage.RelationshipTupleReader
	datastoreOp               storagewrappers.Operation
	cache                     storage.InMemoryCache[any]
	queryCacheEnabled         bool
	queryCacheTTL             time.Duration
	lastCacheInvalidationTime time.Time
	planner                   planner.Manager
	concurrencyLimit          int
	upstreamTimeout           time.Duration

	// Shared resources for iterator cache (singleflight, waitgroup)
	sharedResources *shared.SharedDatastoreResources
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

func WithCheckQueryV2QueryCacheEnabled(enabled bool) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.queryCacheEnabled = enabled
	}
}

func WithCheckQueryV2QueryCacheTTL(ttl time.Duration) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.queryCacheTTL = ttl
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

func WithCheckQueryV2MaxConcurrentReads(n uint32) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.datastoreOp.Concurrency = n
	}
}

func WithCheckQueryV2DatastoreThrottling(enabled bool, threshold int, duration time.Duration) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.datastoreOp.ThrottlingEnabled = enabled
		cmd.datastoreOp.ThrottleThreshold = threshold
		cmd.datastoreOp.ThrottleDuration = duration
	}
}

func WithCheckQueryV2UpstreamTimeout(timeout time.Duration) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.upstreamTimeout = timeout
	}
}

// WithCheckQueryV2SharedResources sets shared resources for iterator caching.
// This includes the shared singleflight.Group and sync.WaitGroup to prevent
// cache stampedes across concurrent requests.
func WithCheckQueryV2SharedResources(r *shared.SharedDatastoreResources) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.sharedResources = r
	}
}

func NewCheckQuery(opts ...CheckQueryV2Option) *CheckQueryV2 {
	q := &CheckQueryV2{
		logger: logger.NewNoopLogger(),
		datastoreOp: storagewrappers.Operation{
			Method:      V2CheckMethodName, // Must be different from base Check to avoid metric pollution when both Check algorithms are running
			Concurrency: defaultMaxConcurrentReadsForCheck,
		},
	}

	for _, opt := range opts {
		opt(q)
	}

	return q
}

func (q *CheckQueryV2) Execute(ctx context.Context, req *openfgav1.CheckRequest) (*openfgav1.CheckResponse, storagewrappers.Metadata, error) {
	err := validateRequest(req)
	if err != nil {
		return nil, storagewrappers.Metadata{}, serverErrors.ValidationError(err)
	}

	boundedDS := storagewrappers.NewBoundedTupleReader(q.datastore, &q.datastoreOp) // Datastore throttling and concurrency limiting
	var datastore storage.RelationshipTupleReader = boundedDS

	r, err := check.NewRequest(check.RequestParams{
		StoreID:          req.GetStoreId(),
		Model:            q.model,
		TupleKey:         tuple.ConvertCheckRequestTupleKeyToTupleKey(req.GetTupleKey()),
		ContextualTuples: req.GetContextualTuples().GetTupleKeys(),
		Context:          req.GetContext(),
		Consistency:      req.GetConsistency(),
	})

	if err != nil {
		return nil, boundedDS.GetMetadata(), err
	}

	// Wrap datastore with iterator cache using SHARED resources to prevent cache stampedes.
	// The singleflight.Group and sync.WaitGroup are shared across all requests.
	if q.sharedResources != nil &&
		q.sharedResources.V2IteratorCacheEnabled &&
		q.cache != nil {
		datastore = storagewrappers.NewCachedTupleReader(
			q.sharedResources.ServerCtx,
			datastore,
			q.cache,
			q.sharedResources.V2IteratorCacheMaxSize,
			q.sharedResources.V2IteratorCacheTTL,
			q.sharedResources.SingleflightGroup, // SHARED across requests
			q.sharedResources.WaitGroup,         // SHARED across requests
			q.sharedResources.V2IteratorDrainTimeout,
		)
	}

	queryCache := storage.InMemoryCache[any](storage.NewNoopCache())
	if q.queryCacheEnabled {
		queryCache = q.cache
	}

	resolver := check.New(check.Config{
		Model:                     q.model,
		Datastore:                 datastore,
		Cache:                     queryCache,
		CacheTTL:                  q.queryCacheTTL,
		LastCacheInvalidationTime: q.lastCacheInvalidationTime,
		Planner:                   q.planner,
		ConcurrencyLimit:          q.concurrencyLimit,
		UpstreamTimeout:           q.upstreamTimeout,
		Logger:                    q.logger,
	})

	res, err := resolver.ResolveCheck(ctx, r)
	metadata := boundedDS.GetMetadata()
	if err != nil {
		if metadata.WasThrottled && errors.Is(err, context.DeadlineExceeded) {
			err = &ThrottledError{Cause: err}
		}
		return nil, metadata, err
	}

	return &openfgav1.CheckResponse{
		Allowed: res.GetAllowed(),
	}, metadata, nil
}

func validateRequest(req *openfgav1.CheckRequest) error {
	tk := req.GetTupleKey()
	if utils.ContainsForbiddenChars(tk.GetObject()) ||
		utils.ContainsForbiddenChars(tk.GetRelation()) ||
		utils.ContainsForbiddenChars(tk.GetUser()) {
		return fmt.Errorf("request tuple_key contains forbidden characters")
	}

	for _, ct := range req.GetContextualTuples().GetTupleKeys() {
		if utils.ContainsForbiddenChars(ct.GetObject()) ||
			utils.ContainsForbiddenChars(ct.GetRelation()) ||
			utils.ContainsForbiddenChars(ct.GetUser()) {
			return &tuple.InvalidTupleError{
				Cause:    fmt.Errorf("contextual tuple contains forbidden characters"),
				TupleKey: ct,
			}
		}
	}

	return nil
}
