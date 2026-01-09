package modelgraph

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
)

var tracer = otel.Tracer("internal/modelgraph")

const CacheKeyPrefix = "wg|"
const DELIMITER = "|"

type AuthorizationModelGraphResolver struct {
	datastore storage.AuthorizationModelReadBackend // these methods are already cached at a lower level
	cache     storage.InMemoryCache[any]
	ttl       time.Duration
}

func NewResolver(datastore storage.AuthorizationModelReadBackend, cache storage.InMemoryCache[any], ttl time.Duration) *AuthorizationModelGraphResolver {
	r := &AuthorizationModelGraphResolver{
		datastore: datastore,
		cache:     cache,
		ttl:       ttl,
	}

	if r.cache == nil {
		r.cache = storage.NewNoopCache()
	}

	return r
}

func (r *AuthorizationModelGraphResolver) Resolve(ctx context.Context, storeID, modelID string) (*AuthorizationModelGraph, error) {
	ctx, span := tracer.Start(ctx, "resolve", trace.WithAttributes(
		attribute.String("store_id", storeID),
		attribute.String("model_id", modelID),
	))
	defer span.End()

	var err error

	if modelID != "" {
		// this validation should happen at the api level
		if _, err := ulid.Parse(modelID); err != nil {
			return nil, ErrModelNotFound
		}
	}

	var model *openfgav1.AuthorizationModel

	if modelID == "" {
		m, err := r.datastore.FindLatestAuthorizationModel(ctx, storeID)
		if err != nil {
			telemetry.TraceError(span, err)
			if errors.Is(err, storage.ErrNotFound) {
				return nil, ErrModelNotFound
			}

			return nil, fmt.Errorf("failed to FindLatestAuthorizationModel: %w", err)
		}
		model = m
		modelID = model.GetId()
	}
	var keyBuilder strings.Builder
	keyBuilder.WriteString(CacheKeyPrefix)
	keyBuilder.WriteString(modelID)
	key := keyBuilder.String()

	if wg := r.cache.Get(key); wg != nil {
		return wg.(*AuthorizationModelGraph), nil
	}

	// id was provided yet wasn't cached, so we need to read it
	if model == nil {
		model, err = r.datastore.ReadAuthorizationModel(ctx, storeID, modelID)
		if err != nil {
			telemetry.TraceError(span, err)
			if errors.Is(err, storage.ErrNotFound) {
				return nil, ErrModelNotFound
			}

			return nil, fmt.Errorf("failed to ReadAuthorizationModel: %w", err)
		}
	}

	mg, err := New(model)
	if err != nil {
		telemetry.TraceError(span, err)
		// likely need custom error about validation
		return nil, fmt.Errorf("%w: %w", ErrInvalidModel, err)
	}

	r.cache.Set(key, mg, r.ttl)

	return mg, nil
}
