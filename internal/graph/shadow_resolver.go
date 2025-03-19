package graph

import (
	"context"
	"math"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/openfga/openfga/pkg/logger"
)

const Hundred = 100

type ShadowResolverOpt func(*ShadowResolver)

func ShadowResolverWithTimeout(timeout time.Duration) ShadowResolverOpt {
	return func(shadowResolver *ShadowResolver) {
		shadowResolver.shadowTimeout = timeout
	}
}

func ShadowResolverWithSamplePercentage(p int) ShadowResolverOpt {
	return func(shadowResolver *ShadowResolver) {
		shadowResolver.samplePercentage = int(math.Abs(float64(p)))
	}
}

func ShadowResolverWithLogger(logger logger.Logger) ShadowResolverOpt {
	return func(shadowResolver *ShadowResolver) {
		shadowResolver.logger = logger
	}
}

type ShadowResolver struct {
	main             CheckResolver
	shadow           CheckResolver
	shadowTimeout    time.Duration
	samplePercentage int
	logger           logger.Logger
	// only used for testing signals
	wg *sync.WaitGroup
}

var _ CheckResolver = (*ShadowResolver)(nil)

func (s ShadowResolver) ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
	ctxClone := context.WithoutCancel(ctx) // needs typesystem and datastore etc
	res, err := s.main.ResolveCheck(ctx, req)
	if err != nil {
		return nil, err
	}

	if rand.Intn(Hundred) < s.samplePercentage {
		// only successful requests will be evaluated
		resClone := res.clone()
		reqClone := req.clone()
		reqClone.VisitedPaths = nil // reset completely for evaluation
		s.wg.Add(1)
		go func() {
			ctx, cancel := context.WithTimeout(ctxClone, s.shadowTimeout)
			defer func() {
				if r := recover(); r != nil {
					s.logger.ErrorWithContext(ctx, "shadow check panic",
						zap.Any("error", err),
						zap.String("request", reqClone.GetTupleKey().String()),
						zap.String("store_id", reqClone.GetStoreID()),
						zap.String("model_id", reqClone.GetAuthorizationModelID()),
					)
				}
				cancel()
				s.wg.Done()
			}()

			shadowRes, err := s.shadow.ResolveCheck(ctx, reqClone)
			if err != nil {
				s.logger.WarnWithContext(ctx, "shadow check errored",
					zap.Error(err),
					zap.String("request", reqClone.GetTupleKey().String()),
					zap.String("store_id", reqClone.GetStoreID()),
					zap.String("model_id", reqClone.GetAuthorizationModelID()),
				)
				return
			}
			if shadowRes.GetAllowed() != resClone.GetAllowed() {
				s.logger.InfoWithContext(ctx, "shadow check difference",
					zap.String("request", reqClone.GetTupleKey().String()),
					zap.String("store_id", reqClone.GetStoreID()),
					zap.String("model_id", reqClone.GetAuthorizationModelID()),
					zap.Bool("main", resClone.GetAllowed()),
					zap.Bool("main-cycle", resClone.GetCycleDetected()),
					zap.Bool("shadow", shadowRes.GetAllowed()),
					zap.Bool("shadow-cycle", shadowRes.GetCycleDetected()),
				)
			}
		}()
	}

	return res, nil
}

func (s ShadowResolver) Close() {
	s.main.Close()
	s.shadow.Close()
}

func (s ShadowResolver) SetDelegate(delegate CheckResolver) {
	s.main.SetDelegate(delegate)
	// shadow should result in noop regardless of outcome
}

func (s ShadowResolver) GetDelegate() CheckResolver {
	return s.main.GetDelegate()
}

func NewShadowChecker(main CheckResolver, shadow CheckResolver, opts ...ShadowResolverOpt) *ShadowResolver {
	r := &ShadowResolver{main: main, shadow: shadow, wg: &sync.WaitGroup{}}

	for _, opt := range opts {
		opt(r)
	}

	return r
}
