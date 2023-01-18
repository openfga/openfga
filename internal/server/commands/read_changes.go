package commands

import (
	"context"
	"errors"
	"time"

	serverErrors "github.com/openfga/openfga/internal/server/errors"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
)

type ReadChangesQuery struct {
	backend       storage.ChangelogBackend
	logger        logger.Logger
	tracer        trace.Tracer
	encoder       encoder.Encoder
	horizonOffset time.Duration
}

// NewReadChangesQuery creates a ReadChangesQuery with specified `ChangelogBackend` and `typeDefinitionReadBackend` to use for storage
func NewReadChangesQuery(backend storage.ChangelogBackend, tracer trace.Tracer, logger logger.Logger, encoder encoder.Encoder, horizonOffset int) *ReadChangesQuery {
	return &ReadChangesQuery{
		backend:       backend,
		logger:        logger,
		tracer:        tracer,
		encoder:       encoder,
		horizonOffset: time.Duration(horizonOffset) * time.Minute,
	}
}

// Execute the ReadChangesQuery, returning paginated `openfga.TupleChange`(s) and a possibly non-empty continuation token.
func (q *ReadChangesQuery) Execute(ctx context.Context, req *openfgapb.ReadChangesRequest) (*openfgapb.ReadChangesResponse, error) {
	decodedContToken, err := q.encoder.Decode(req.GetContinuationToken())
	if err != nil {
		return nil, serverErrors.InvalidContinuationToken
	}
	paginationOptions := storage.NewPaginationOptions(req.GetPageSize().GetValue(), string(decodedContToken))

	changes, contToken, err := q.backend.ReadChanges(ctx, req.StoreId, req.Type, paginationOptions, q.horizonOffset)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &openfgapb.ReadChangesResponse{
				ContinuationToken: req.GetContinuationToken(),
			}, nil
		}
		return nil, serverErrors.HandleError("", err)
	}

	encodedContToken, err := q.encoder.Encode(contToken)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	return &openfgapb.ReadChangesResponse{
		Changes:           changes,
		ContinuationToken: encodedContToken,
	}, nil
}
