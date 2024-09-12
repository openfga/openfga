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

type TrackerCheckResolverOpt func(checkResolver *TrackerCheckResolver)

type trackerKey struct {
	store string
	model string
}

type resolutionNode struct {
	tm   time.Time
	hits *atomic.Uint64
}

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

	nodes sync.Map // key is a string store#model and value is another map where key has the shape doc:1#viewer@user and value is the number of hits.
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
		t.ticker.Stop() // clear the default
		t.tickerInterval = d
		t.ticker = time.NewTicker(d)
	}
}

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
	} else {
		t.cancel = func() { // no op
		}
	}

	t.launchFlush()

	return t
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
			}
		}
	}()
}

func (t *TrackerCheckResolver) SetDelegate(delegate CheckResolver) {
	t.delegate = delegate
}

func (t *TrackerCheckResolver) GetDelegate() CheckResolver {
	return t.delegate
}

func (t *TrackerCheckResolver) Close() {
	t.cancel()
	t.logExecutionPaths(true)
}

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
				t.logger.Info("hits",
					logger.String("store_id", storeModel.store),
					logger.String("model_id", storeModel.model),
					logger.String("path", path),
					logger.Uint64("hits", node.hits.Load()))

				paths.Delete(path)
			}
			return true
		})
		return true
	})
}

// getTupleKeyAsPath for a tuple like (user:anne, viewer, doc:1) returns doc:1#viewer@user
// for a tuple like (group:fga#member, viewer, doc:1), returns doc:1#viewer@userset
// for a tuple like (user:*, viewer, doc:1), returns doc:1#viewer@userset.
func getTupleKeyAsPath(tk *openfgav1.TupleKey) string {
	return fmt.Sprintf("%s#%s@%s", tk.GetObject(), tk.GetRelation(), string(tuple.GetUserTypeFromUser(tk.GetUser())))
}

func (t *TrackerCheckResolver) loadModel(r *ResolveCheckRequest) (value any, ok bool) {
	key := trackerKey{store: r.GetStoreID(), model: r.GetAuthorizationModelID()}
	value, ok = t.nodes.Load(key)
	if !ok {
		value = &sync.Map{}
		value.(*sync.Map).Store(getTupleKeyAsPath(r.GetTupleKey()), &resolutionNode{tm: time.Now().UTC(), hits: &atomic.Uint64{}})
		t.nodes.Store(key, value)
	}
	return value, ok
}

func (t *TrackerCheckResolver) addPathHits(r *ResolveCheckRequest) {
	path := getTupleKeyAsPath(r.GetTupleKey())

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

	resp, err := t.delegate.ResolveCheck(ctx, req)

	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		t.addPathHits(req)
	}

	return resp, err
}
