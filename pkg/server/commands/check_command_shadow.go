package commands

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/logger"
)

const ShadowCheckQueryFunction = "ShadowCheckQuery.Execute"

type shadowedCheckQuery struct {
	main   CheckQuery
	shadow CheckQuery

	shadowPct     int           // An integer representing the shadowPct of list_objects requests that will also trigger the shadow query. This allows for controlled rollout and data collection without impacting all requests. Value should be between 0 and 100.
	shadowTimeout time.Duration // A time.Duration specifying the maximum amount of time to wait for the shadow list_objects query to complete. If the shadow query exceeds this shadowTimeout, it will be cancelled, and its result will be ignored, but the shadowTimeout event will be logged.

	logger logger.Logger

	// only used for testing signals
	wg sync.WaitGroup
}

type ShadowCheckCommandConfig struct {
	enabled       bool          // A boolean indicating whether shadow mode is enabled.
	shadowPct     int           // An integer representing the shadowPct of list_objects requests that will also trigger the shadow query. This allows for controlled rollout and data collection without impacting all requests. Value should be between 0 and 100.
	shadowTimeout time.Duration // A time.Duration specifying the maximum amount of time to wait for the shadow list_objects query to complete. If the shadow query exceeds this shadowTimeout, it will be cancelled, and its result will be ignored, but the shadowTimeout event will be logged.
	logger        logger.Logger
	cfg           CheckCommandConfig // embedded to ensure we can create a shadowed check command with all necessary params
}

func (sc *ShadowCheckCommandConfig) isEnabled() bool {
	return sc.enabled && sc.shadowPct > 0
}

type ShadowCheckQueryOption func(*ShadowCheckCommandConfig)

func WithShadowCheckQueryEnabled(enabled bool) ShadowCheckQueryOption {
	return func(config *ShadowCheckCommandConfig) {
		config.enabled = enabled
	}
}

func WithShadowCheckQueryPct(pct int) ShadowCheckQueryOption {
	return func(config *ShadowCheckCommandConfig) {
		config.shadowPct = pct
	}
}

func WithShadowCheckQueryTimeout(timeout time.Duration) ShadowCheckQueryOption {
	return func(config *ShadowCheckCommandConfig) {
		config.shadowTimeout = timeout
	}
}

func WithShadowCheckQueryLogger(logger logger.Logger) ShadowCheckQueryOption {
	return func(config *ShadowCheckCommandConfig) {
		config.logger = logger
	}
}

// check that shadowedCheckQuery implements CheckCommand.
var _ CheckCommand = (*shadowedCheckQuery)(nil)

// NewCheckCommandShadowConfig creates a new ShadowCheckCommandConfig with the provided options.
// If no options are provided, it will use the default values.
// Default values:
//   - shadowPct: 0
//   - shadowTimeout: 500ms
//   - logger: noop logger
func NewCheckCommandShadowConfig(cfg CheckCommandConfig, opts ...ShadowCheckQueryOption) ShadowCheckCommandConfig {
	config := ShadowCheckCommandConfig{
		enabled:       false,
		shadowPct:     0,
		shadowTimeout: 500 * time.Millisecond,
		logger:        logger.NewNoopLogger(),
		cfg:           cfg,
	}

	for _, opt := range opts {
		opt(&config)
	}

	return config
}

func newCheckCommandWithShadowConfig(cfg CheckCommandConfig, shadowConfig ShadowCheckCommandConfig, params CheckCommandParams) CheckCommand {
	checkCommand := NewCheckCommand(cfg.datastore, cfg.checkResolver, params, cfg.options...)
	if shadowConfig.isEnabled() {
		shadowCheckCommand := NewCheckCommand(shadowConfig.cfg.datastore, shadowConfig.cfg.checkResolver, params, shadowConfig.cfg.options...)
		return newShadowCheckCommand(checkCommand, shadowCheckCommand, shadowConfig)
	}
	return checkCommand
}

func newShadowCheckCommand(mainCheck *CheckQuery, shadowCheck *CheckQuery, shadowConfig ShadowCheckCommandConfig) CheckCommand {
	return &shadowedCheckQuery{
		main:          *mainCheck,
		shadow:        *shadowCheck,
		shadowPct:     shadowConfig.shadowPct,
		shadowTimeout: shadowConfig.shadowTimeout,
		logger:        shadowConfig.logger,
	}
}

// checkShadowModeSampleRate randomly returns true based on the shadowPct.
// For example, if shadowPct is 10, it will return true 10% of the time.
func (q *shadowedCheckQuery) checkShadowModeSampleRate() bool {
	return rand.Intn(100) < q.shadowPct // randomly enable shadow mode
}

func (q *shadowedCheckQuery) Execute(ctx context.Context) (*graph.ResolveCheckResponse, *graph.ResolveCheckRequestMetadata, error) {
	ctxClone := context.WithoutCancel(ctx) // needs typesystem and datastore etc

	mainStart := time.Now()
	response, metadata, err := q.main.Execute(ctx)
	mainDuration := time.Since(mainStart)

	if err != nil {
		return nil, nil, err
	}

	if q.checkShadowModeSampleRate() {
		shadowCtx, shadowCancel := context.WithTimeout(ctxClone, q.shadowTimeout)
		defer shadowCancel()

		q.wg.Add(1)
		go func() {
			defer q.wg.Done()

			params := q.main.params
			defer func() {
				if r := recover(); r != nil {
					q.logger.ErrorWithContext(ctx, "panic recovered",
						zap.String("resolver", "check"),
						zap.String("request", params.TupleKey.String()),
						zap.String("store_id", params.StoreID),
						zap.String("model_id", q.main.typesys.GetAuthorizationModelID()),
						zap.String("function", ShadowCheckQueryFunction),
						zap.Any("error", err),
					)
				}
			}()

			shadowStart := time.Now()
			shadowRes, _, err := q.shadow.Execute(shadowCtx)
			shadowDuration := time.Since(shadowStart)
			if err != nil {
				q.logger.WarnWithContext(ctx, "shadow check errored", q.withCommonShadowCheckFields(zap.Error(err))...)
				return
			}
			if response.GetAllowed() != shadowRes.GetAllowed() {
				q.logger.InfoWithContext(ctx, "shadow check difference", q.withCommonShadowCheckFields(
					zap.Bool("main", response.GetAllowed()),
					zap.Bool("main_cycle", response.GetCycleDetected()),
					zap.Int64("main_latency", mainDuration.Milliseconds()),
					zap.Uint32("main_query_count", response.GetResolutionMetadata().DatastoreQueryCount),
					zap.Bool("shadow", shadowRes.GetAllowed()),
					zap.Bool("shadow_cycle", shadowRes.GetCycleDetected()),
					zap.Int64("shadow_latency", shadowDuration.Milliseconds()),
					zap.Uint32("shadow_query_count", shadowRes.GetResolutionMetadata().DatastoreQueryCount),
				)...)
			} else {
				q.logger.InfoWithContext(ctx, "shadow check match", q.withCommonShadowCheckFields(
					zap.Int64("main_latency", mainDuration.Milliseconds()),
					zap.Uint32("main_query_count", response.GetResolutionMetadata().DatastoreQueryCount),
					zap.Int64("shadow_latency", shadowDuration.Milliseconds()),
					zap.Uint32("shadow_query_count", shadowRes.GetResolutionMetadata().DatastoreQueryCount),
				)...)
			}
		}()
	}

	return response, metadata, nil
}

func (q *shadowedCheckQuery) withCommonShadowCheckFields(fields ...zap.Field) []zap.Field {
	params := q.main.params
	return append([]zap.Field{
		zap.String("resolver", "check"),
		zap.String("request", params.TupleKey.String()),
		zap.String("store_id", params.StoreID),
		zap.String("model_id", q.main.typesys.GetAuthorizationModelID()),
		zap.String("function", ShadowCheckQueryFunction),
	}, fields...)
}
