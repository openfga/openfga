package graph

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
)

const (
	trackerLogLines    = 15
	trackerLogBurst    = 12
	trackerLogInterval = time.Duration(500) * time.Millisecond
	trackerInterval    = time.Duration(60) * time.Second
)

// TrackerCheckResolverOpt defines an option pattern that can be used to change the behavior of TrackerCheckResolver.
type TrackerCheckResolverOpt func(checkResolver *TrackerCheckResolver)

type trackerKey struct {
	store string
	model string
}

type resolutionNode struct {
	tm   time.Time
	hits *atomic.Uint64
}

// Expired check the current tuple entry expiration.
func (r *resolutionNode) expired() bool {
	return time.Since(r.tm) > trackerInterval
}

type TrackerCheckResolver struct {
	delegate CheckResolver
	ticker   *time.Ticker
	logger   logger.Logger
	limiter  *rate.Limiter
	ctx      context.Context
	nodes    sync.Map
}

var _ CheckResolver = (*TrackerCheckResolver)(nil)

func WithTrackerLogger(logger logger.Logger) TrackerCheckResolverOpt {
	return func(t *TrackerCheckResolver) {
		t.logger = logger
	}
}

func WithTrackerContext(ctx context.Context) TrackerCheckResolverOpt {
	return func(t *TrackerCheckResolver) {
		t.ctx = ctx
	}
}

// NewTrackCheckResolver creates an instance tracker Resolver.
func NewTrackCheckResolver(opts ...TrackerCheckResolverOpt) *TrackerCheckResolver {
	t := &TrackerCheckResolver{
		limiter: rate.NewLimiter(rate.Every(trackerLogInterval)/2, trackerLogLines),
		ticker:  time.NewTicker(trackerInterval),
	}

	for _, opt := range opts {
		opt(t)
	}

	t.delegate = t

	if t.validate() {
		t.launchFlush()
	}

	return t
}

// Validate options.
func (t *TrackerCheckResolver) validate() bool {
	if t.ctx == nil {
		return false
	}

	if t.logger == nil {
		return false
	}
	return true
}

// LogExecutionPaths reports the model and tuple path.
func (t *TrackerCheckResolver) logExecutionPaths(flush bool) {
	t.nodes.Range(func(k, v any) bool {
		paths := v.(*sync.Map)
		storeModel := k.(trackerKey)
		paths.Range(func(k, v any) bool {
			path := k.(string)
			tree := v.(*resolutionNode)
			if tree.expired() || flush {
				if !t.limiter.Allow() {
					return false
				}
				t.logger.Info("execution path hits",
					zap.String("store", storeModel.store),
					zap.String("model", storeModel.model),
					zap.String("path", path),
					zap.Uint64("hits", tree.hits.Load()))
				paths.Delete(path)
			}
			return true
		})
		return true
	})
}

// LaunchFlush starts the execution path logging and removal of entries.
func (t *TrackerCheckResolver) launchFlush() {
	go func() {
		for {
			select {
			case <-t.ctx.Done():
				t.ticker.Stop()
				t.logExecutionPaths(true)
				return
			case <-t.ticker.C:
				t.logExecutionPaths(false)
				t.ticker.Reset(trackerInterval)
			}
		}
	}()
}

// SetDelete assigns the next deletegate in the chain.
func (t *TrackerCheckResolver) SetDelegate(delegate CheckResolver) {
	t.delegate = delegate
}

// GetDelegate return the assigned delegate.
func (t *TrackerCheckResolver) GetDelegate() CheckResolver {
	return t.delegate
}

// Close implements CheckResolver.
func (*TrackerCheckResolver) Close() {}

// UserType returns the associated tuple user type.
func (t *TrackerCheckResolver) userType(userKey string) string {
	return string(tuple.GetUserTypeFromUser(userKey))
}

// GetTK returns formatted tuple suitable insertion into list formatted object#relation@user.
func (t *TrackerCheckResolver) getTK(tk *openfgav1.TupleKey) string {
	return fmt.Sprintf("%s#%s@%s", tk.GetObject(), tk.GetRelation(), t.userType(tk.GetUser()))
}

// LoadModel populate model id for individual tuple paths.
func (t *TrackerCheckResolver) loadModel(r *ResolveCheckRequest) (value any, ok bool) {
	key := trackerKey{store: r.GetStoreID(), model: r.GetAuthorizationModelID()}
	value, ok = t.nodes.Load(key)
	if !ok {
		value = &sync.Map{}
		value.(*sync.Map).Store(t.getTK(r.GetTupleKey()), &resolutionNode{tm: time.Now(), hits: &atomic.Uint64{}})
		t.nodes.Store(key, value)
	}
	return value, ok
}

// LoadPath populates the individual tuple paths.
func (t *TrackerCheckResolver) loadPath(value any, path string) {
	paths, _ := value.(*sync.Map)
	if _, ok := paths.Load(path); !ok {
		paths.Store(path, &resolutionNode{tm: time.Now(), hits: &atomic.Uint64{}})
	}
}

// IncrementPath counter.
func (t *TrackerCheckResolver) incrementPath(paths *sync.Map, path string) {
	value, ok := paths.Load(path)
	if ok {
		value.(*resolutionNode).hits.Add(1)
	}
}

// AddPathHits to list of path by model.
func (t *TrackerCheckResolver) addPathHits(r *ResolveCheckRequest) {
	path := t.getTK(r.GetTupleKey())

	value, ok := t.loadModel(r)
	if ok {
		t.loadPath(value, path)
	}

	paths, ok := value.(*sync.Map)
	if ok {
		t.incrementPath(paths, path)
	}
}

// ResolveCheck implements CheckResolver.
func (t *TrackerCheckResolver) ResolveCheck(
	ctx context.Context,
	req *ResolveCheckRequest,
) (*ResolveCheckResponse, error) {
	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.Bool("track_execution", true))

	resp, err := t.delegate.ResolveCheck(ctx, &ResolveCheckRequest{
		StoreID:              req.GetStoreID(),
		AuthorizationModelID: req.GetAuthorizationModelID(),
		TupleKey:             req.GetTupleKey(),
		ContextualTuples:     req.GetContextualTuples(),
		RequestMetadata:      req.GetRequestMetadata(),
		VisitedPaths:         req.VisitedPaths,
		Context:              req.GetContext(),
	})

	if err == nil || errors.Is(err, context.Canceled) {
		if resp != nil && resp.Allowed {
			t.addPathHits(req)
		}
	}

	return resp, err
}
