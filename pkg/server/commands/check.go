package commands

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	"go.uber.org/zap"
	"google.golang.org/grpc/status"

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

// CheckResult is the transport-agnostic outcome of a single Check, normalized across the v1 and v2 implementations.
type CheckResult struct {
	Allowed             bool
	DatastoreQueryCount uint32
	DatastoreItemCount  uint64
	Duration            time.Duration
	WasThrottled        bool
	DispatchThrottled   bool   // v1 only; always false for v2
	DispatchCount       uint32 // v1 only; always 0 for v2
	CycleDetected       bool   // v1 only; always false for v2
}

// Checker runs a single authorization check.
type Checker interface {
	Execute(ctx context.Context, params *CheckCommandParams) (*CheckResult, error)
}

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

	// fallback is called when Execute returns a non-terminal error. Optional.
	fallback Checker

	primaryCount  atomic.Uint32
	fallbackCount atomic.Uint32
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

// WithCheckQueryV2Fallback sets a Checker to call when Execute encounters a
// non-terminal error. When set, CheckQueryV2 satisfies Checker with per-item fallback.
func WithCheckQueryV2Fallback(f Checker) CheckQueryV2Option {
	return func(cmd *CheckQueryV2) {
		cmd.fallback = f
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

// Execute implements Checker. When CheckQueryV2.fallback is set, it calls it on non-terminal
// errors and increments FallbackCount; otherwise increments PrimaryCount.
func (q *CheckQueryV2) Execute(ctx context.Context, params *CheckCommandParams) (*CheckResult, error) {
	if err := validateCheckCommandParams(params); err != nil {
		return nil, serverErrors.ValidationError(err)
	}

	res, err := q.resolve(ctx, params)
	if err == nil || IsV2CheckTerminalError(err) || q.fallback == nil {
		q.primaryCount.Add(1)
		return res, err
	}

	requestID, _ := grpc_ctxtags.Extract(ctx).Values()["request_id"].(string)
	q.logger.Warn("Weighted graph check failed, falling back",
		zap.Error(err),
		zap.String("store_id", params.StoreID),
		zap.String("model_id", q.model.GetModelID()),
		zap.String("request_id", requestID),
	)

	fallbackRes, err := q.fallback.Execute(ctx, params)
	if fallbackRes == nil {
		fallbackRes = &CheckResult{}
	}
	q.fallbackCount.Add(1)
	return fallbackRes, err
}

func (q *CheckQueryV2) resolve(ctx context.Context, params *CheckCommandParams) (*CheckResult, error) {
	start := time.Now()
	boundedDS := storagewrappers.NewBoundedTupleReader(q.datastore, &q.datastoreOp)
	var datastore storage.RelationshipTupleReader = boundedDS

	r, err := check.NewRequest(check.RequestParams{
		StoreID:          params.StoreID,
		Model:            q.model,
		TupleKey:         tuple.ConvertCheckRequestTupleKeyToTupleKey(params.TupleKey),
		ContextualTuples: params.ContextualTuples.GetTupleKeys(),
		Context:          params.Context,
		Consistency:      params.Consistency,
	})
	if err != nil {
		md := boundedDS.GetMetadata()
		return &CheckResult{
			DatastoreQueryCount: md.DatastoreQueryCount,
			DatastoreItemCount:  md.DatastoreItemCount,
			Duration:            time.Since(start),
			WasThrottled:        md.WasThrottled,
		}, err
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
	md := boundedDS.GetMetadata()
	if err != nil {
		if md.WasThrottled && errors.Is(err, context.DeadlineExceeded) {
			err = &ThrottledError{Cause: err}
		}
		return &CheckResult{
			DatastoreQueryCount: md.DatastoreQueryCount,
			DatastoreItemCount:  md.DatastoreItemCount,
			Duration:            time.Since(start),
			WasThrottled:        md.WasThrottled,
		}, err
	}

	return &CheckResult{
		Allowed:             res.GetAllowed(),
		DatastoreQueryCount: md.DatastoreQueryCount,
		DatastoreItemCount:  md.DatastoreItemCount,
		Duration:            time.Since(start),
		WasThrottled:        md.WasThrottled,
	}, nil
}

// PrimaryCount returns the number of times Execute resolved via the primary (v2) path.
func (q *CheckQueryV2) PrimaryCount() uint32 { return q.primaryCount.Load() }

// FallbackCount returns the number of times Execute fell back to the configured fallback checker.
func (q *CheckQueryV2) FallbackCount() uint32 { return q.fallbackCount.Load() }

func validateCheckCommandParams(params *CheckCommandParams) error {
	if params == nil {
		return fmt.Errorf("params must not be nil")
	}
	tk := params.TupleKey
	if utils.ContainsForbiddenChars(tk.GetObject()) ||
		utils.ContainsForbiddenChars(tk.GetRelation()) ||
		utils.ContainsForbiddenChars(tk.GetUser()) {
		return fmt.Errorf("request tuple_key contains forbidden characters")
	}
	for _, ct := range params.ContextualTuples.GetTupleKeys() {
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

// IsV2CheckTerminalError reports whether err should be returned directly from the v2Check path
// rather than falling back to v1. Two categories qualify:
//
//   - Context/timeout/throttle errors, where a v1 retry is pointless or harmful. Context
//     errors appear in two forms: raw (context.Canceled/DeadlineExceeded, from the model
//     graph resolver) or server-mapped (ErrRequestCancelled/ErrRequestDeadlineExceeded, from
//     the execute path). ErrThrottledTimeout and ErrTransactionThrottled are included since
//     they are not v2-specific, and retrying on v1 would only add load to an already-throttled
//     datastore.
//   - Deterministic request-validation failures (ErrValidation, ErrInvalidUser, and
//     contextual-tuple validation failures), which v1 rejects identically — the fallback is
//     wasted work and log noise.
//
// v2Check wraps most non-context errors via commands.CheckCommandErrorToServerError, which
// produces gRPC status.Error values. Errors.Is/As against the original sentinels/types no
// longer matches after that conversion, so request-validation cases must be classified by
// the resulting gRPC code instead.
func IsV2CheckTerminalError(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, serverErrors.ErrRequestDeadlineExceeded) || errors.Is(err, serverErrors.ErrRequestCancelled) ||
		errors.Is(err, serverErrors.ErrThrottledTimeout) || errors.Is(err, serverErrors.ErrTransactionThrottled) {
		return true
	}
	if st, ok := status.FromError(err); ok {
		switch openfgav1.ErrorCode(st.Code()) {
		case openfgav1.ErrorCode_validation_error, openfgav1.ErrorCode_invalid_tuple:
			return true
		}
	}
	return false
}
