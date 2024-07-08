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

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
)

const (
	trackerInterval = time.Duration(60) * time.Second
)

// TrackerCheckResolverOpt defines an option pattern that can be used to change the behavior of TrackerCheckResolver.
type TrackerCheckResolverOpt func(checkResolver *TrackerCheckResolver)
type resolutionTree struct {
	tm   time.Time
	hits *atomic.Uint64
}

type TrackerCheckResolver struct {
	delegate CheckResolver
	ticker   *time.Ticker
	logger   logger.Logger
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

// Expired check the current tuple entry expiration.
func (r *resolutionTree) expired() bool {
	return time.Since(r.tm) > trackerInterval
}

// NewTrackCheckResolver creates an instance tracker Resolver.
func NewTrackCheckResolver(opts ...TrackerCheckResolverOpt) *TrackerCheckResolver {
	t := &TrackerCheckResolver{
		ticker: time.NewTicker(trackerInterval),
	}

	for _, opt := range opts {
		opt(t)
	}

	t.delegate = t
	t.launchFlush()
	return t
}

// LogExecutionPaths reports the model and tuple path.
func (t *TrackerCheckResolver) logExecutionPaths() {
	t.nodes.Range(func(k, v any) bool {
		modelid := k.(string)
		paths, _ := v.(*sync.Map)
		paths.Range(func(k, v any) bool {
			tree, _ := v.(*resolutionTree)
			path := k.(string)
			if tree.expired() {
				t.logger.Info("execution path hits",
					zap.String("modelid", modelid),
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
				return
			case <-t.ticker.C:
				t.logExecutionPaths()
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
	userObj, userRel := tuple.SplitObjectRelation(userKey)
	_, userObjID := tuple.SplitObject(userObj)

	if userRel == "" && userObjID == "*" {
		return string(tuple.UserSet)
	}

	if userRel == "" {
		return string(tuple.User)
	}

	return string(tuple.UserSet)
}

// GetTK returns formatted tuple suitable insertion into list.
func (t *TrackerCheckResolver) getTK(tk *openfgav1.TupleKey) string {
	return fmt.Sprintf("%s#%s#%s", t.userType(tk.GetUser()), tk.GetRelation(), tk.GetObject())
}

// LoadModel populate model id for individual tuple paths.
func (t *TrackerCheckResolver) loadModel(r *ResolveCheckRequest) (value any, ok bool) {
	model := r.GetAuthorizationModelID()
	value, ok = t.nodes.Load(model)
	if !ok {
		value = &sync.Map{}
		value.(*sync.Map).Store(t.getTK(r.GetTupleKey()), &resolutionTree{tm: time.Now(), hits: &atomic.Uint64{}})
		t.nodes.Store(model, value)
	}
	return
}

// LoadPath populates the individual tuple paths.
func (t *TrackerCheckResolver) loadPath(value any, path string) {
	paths, _ := value.(*sync.Map)
	if _, ok := paths.Load(path); !ok {
		paths.Store(path, &resolutionTree{tm: time.Now(), hits: &atomic.Uint64{}})
	}
}

// IncrementPath counter.
func (t *TrackerCheckResolver) incrementPath(paths *sync.Map, path string) {
	value, ok := paths.Load(path)
	if ok {
		rt := value.(*resolutionTree)
		rt.hits.Add(1)
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
		t.addPathHits(req)
	}

	return resp, err
}
