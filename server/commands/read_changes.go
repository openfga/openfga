package commands

import (
	"context"
	"errors"
	"time"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
)

type ReadChangesQuery struct {
	changelogBackend storage.ChangelogBackend
	encrypter        encoder.Encrypter
	logger           logger.Logger
	tracer           trace.Tracer
	horizonOffset    time.Duration
}

// NewReadChangesQuery creates a ReadChangesQuery with specified `ChangelogBackend` and `typeDefinitionReadBackend` to use for storage
func NewReadChangesQuery(changelogBackend storage.ChangelogBackend, tracer trace.Tracer, logger logger.Logger, encrypter encoder.Encrypter, horizonOffset int) *ReadChangesQuery {
	return &ReadChangesQuery{
		changelogBackend: changelogBackend,
		encrypter:        encrypter,
		logger:           logger,
		tracer:           tracer,
		horizonOffset:    time.Duration(horizonOffset) * time.Minute,
	}
}

// Execute the ReadChangesQuery, returning paginated `openfga.TupleChange`(s) and a possibly non-empty continuation token.
func (q *ReadChangesQuery) Execute(ctx context.Context, req *openfgapb.ReadChangesRequest) (*openfgapb.ReadChangesResponse, error) {
	decodedContToken, err := q.encrypter.Decrypt(req.GetContinuationToken())
	if err != nil {
		return nil, serverErrors.InvalidContinuationToken
	}
	paginationOptions := storage.NewPaginationOptions(req.GetPageSize().GetValue(), string(decodedContToken))
	utils.LogDBStats(ctx, q.logger, "ReadChanges", 1, 0)

	changes, contToken, err := q.changelogBackend.ReadChanges(ctx, req.StoreId, req.Type, paginationOptions, q.horizonOffset)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			return &openfgapb.ReadChangesResponse{
				ContinuationToken: req.GetContinuationToken(),
			}, nil
		}
		return nil, serverErrors.HandleError("", err)
	}

	encodedContToken, err := q.encrypter.Encrypt(contToken)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	return &openfgapb.ReadChangesResponse{
		Changes:           changes,
		ContinuationToken: encodedContToken,
	}, nil
}
