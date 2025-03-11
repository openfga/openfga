package graph

import (
	"context"
	"github.com/openfga/openfga/pkg/logger"
	"go.uber.org/zap"
	"sync"
	"time"
)

type ShadowResolverOpt func(*ShadowResolver)

func ShadowResolverWithTimeout(timeout time.Duration) ShadowResolverOpt {
	return func(shadowResolver *ShadowResolver) {
		shadowResolver.shadowTimeout = timeout
	}
}

func ShadowResolverWithLogger(logger logger.Logger) ShadowResolverOpt {
	return func(shadowResolver *ShadowResolver) {
		shadowResolver.logger = logger
	}
}

type ShadowResolver struct {
	main          CheckResolver
	shadow        CheckResolver
	shadowTimeout time.Duration
	logger        logger.Logger
	// only used for testing signals
	wg *sync.WaitGroup
}

var _ CheckResolver = (*ShadowResolver)(nil)

func (s ShadowResolver) ResolveCheck(ctx context.Context, req *ResolveCheckRequest) (*ResolveCheckResponse, error) {
	res, err := s.main.ResolveCheck(ctx, req)
	if err != nil {
		return nil, err
	}

	// only successful requests will be evaluated
	resClone := res.clone()
	reqClone := req.clone()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), s.shadowTimeout)
		defer cancel()
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
				zap.Bool("shadow", shadowRes.GetAllowed()),
				zap.Bool("main", resClone.GetAllowed()),
			)
		}
	}()

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
