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
	"golang.org/x/exp/rand"
	"golang.org/x/time/rate"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
)

const (
	trackerLogLines    = 15
	trackerMaxInterval = 60
	trackerMinInterval = 30
	trackerLogInterval = time.Duration(500) * time.Millisecond
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
func (r *resolutionNode) expired(trackerInterval time.Duration) bool {
	return time.Since(r.tm) > trackerInterval
}

type TrackerCheckResolver struct {
	delegate       CheckResolver
	ticker         *time.Ticker
	tickerInterval time.Duration
	logger         logger.Logger
	limiter        *rate.Limiter
	ctx            context.Context
	cancel         context.CancelFunc
	nodes          sync.Map

	// testhookFlush is used purely for testing.
	testhookFlush func()
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

func WithTrackerInterval(d time.Duration) TrackerCheckResolverOpt {
	return func(t *TrackerCheckResolver) {
		t.tickerInterval = d
		t.ticker = time.NewTicker(d)
	}
}

// NewTrackCheckResolver creates an instance tracker Resolver.
func NewTrackCheckResolver(opts ...TrackerCheckResolverOpt) *TrackerCheckResolver {
	randomInterval := rand.Intn(trackerMaxInterval-trackerMinInterval+1) + trackerMinInterval
	defaultTickerInterval := time.Duration(randomInterval) * time.Second

	t := &TrackerCheckResolver{
		logger:         logger.NewNoopLogger(),
		limiter:        rate.NewLimiter(rate.Every(trackerLogInterval), trackerLogLines),
		ticker:         time.NewTicker(defaultTickerInterval),
		tickerInterval: defaultTickerInterval,
	}

	for _, opt := range opts {
		opt(t)
	}

	t.delegate = t

	if t.ctx == nil {
		t.ctx, t.cancel = context.WithCancel(context.Background())
	}

	t.launchFlush()

	return t
}

// LogExecutionPaths reports the model and tuple path.
func (t *TrackerCheckResolver) logExecutionPaths(flush bool) {
	t.nodes.Range(func(k, v any) bool {
		paths := v.(*sync.Map)
		storeModel := k.(trackerKey)
		paths.Range(func(k, v any) bool {
			path := k.(string)
			node := v.(*resolutionNode)
			if node.expired(t.tickerInterval) || flush {
				if !t.limiter.Allow() && !flush {
					return false
				}
				t.logger.Info("execution path hits",
					zap.String("store", storeModel.store),
					zap.String("model", storeModel.model),
					zap.String("path", path),
					zap.Uint64("hits", node.hits.Load()))

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
				t.logExecutionPaths(false)
				t.ticker.Reset(t.tickerInterval)

				if t.testhookFlush != nil {
					t.testhookFlush()
				}
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
func (t *TrackerCheckResolver) Close() {
	if t.cancel != nil {
		t.cancel()
	}

	t.logExecutionPaths(true)
}

// UserType returns the associated tuple user type.
func (t *TrackerCheckResolver) userType(userKey string) string {
	return string(tuple.GetUserTypeFromUser(userKey))
}

// getFormattedNode for a tuple like (user:anne, viewer, doc:1) returns doc:1#viewer@user.
// for a tuple like (group:fga#member, viewer, doc:1), returns doc:1#viewer@userset
// for a tuple like (user:*, viewer, doc:1), returns doc:1#viewer@userset
func (t *TrackerCheckResolver) getFormattedNode(tk *openfgav1.TupleKey) string {
	return fmt.Sprintf("%s#%s@%s", tk.GetObject(), tk.GetRelation(), t.userType(tk.GetUser()))
}

// loadModel populate model id for individual tuple paths.
func (t *TrackerCheckResolver) loadModel(r *ResolveCheckRequest) (value any, ok bool) {
	key := trackerKey{store: r.GetStoreID(), model: r.GetAuthorizationModelID()}
	value, ok = t.nodes.Load(key)
	if !ok {
		value = &sync.Map{}
		value.(*sync.Map).Store(t.getFormattedNode(r.GetTupleKey()), &resolutionNode{tm: time.Now().UTC(), hits: &atomic.Uint64{}})
		t.nodes.Store(key, value)
	}
	return value, ok
}

// AddPathHits to list of path by model.
func (t *TrackerCheckResolver) addPathHits(r *ResolveCheckRequest) {
	path := t.getFormattedNode(r.GetTupleKey())

	value, ok := t.loadModel(r)
	if ok {
		paths, _ := value.(*sync.Map)
		if _, ok := paths.Load(path); !ok {
			paths.Store(path, &resolutionNode{tm: time.Now(), hits: &atomic.Uint64{}})
		}
	}

	paths, ok := value.(*sync.Map)
	if ok {
		value, ok := paths.Load(path)
		if ok {
			value.(*resolutionNode).hits.Add(1)
		}
	}
}

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
