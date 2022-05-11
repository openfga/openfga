package queries

import (
	"context"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// A ReadTuplesQuery can be used to read tuples from a store
// THIS IS ONLY FOR THE PLAYGROUND, DO NOT EXPOSE THIS IN PRODUCTION ENVIRONMENTS
type ReadTuplesQuery struct {
	backend storage.TupleBackend
	encoder encoder.Encoder
	logger  logger.Logger
}

// NewReadTuplesQuery creates a ReadTuplesQuery with specified `tupleBackend` to use for storage
func NewReadTuplesQuery(backend storage.TupleBackend, encoder encoder.Encoder, logger logger.Logger) *ReadTuplesQuery {
	return &ReadTuplesQuery{
		backend: backend,
		encoder: encoder,
		logger:  logger,
	}
}

// Execute the ReadTuplesQuery, returning the `openfga.Tuple`(s) for the store
func (q *ReadTuplesQuery) Execute(ctx context.Context, req *openfgav1pb.ReadTuplesRequest) (*openfgav1pb.ReadTuplesResponse, error) {
	decodedContToken, err := q.encoder.Decode(req.GetContinuationToken())
	if err != nil {
		return nil, serverErrors.InvalidContinuationToken
	}
	paginationOptions := storage.NewPaginationOptions(req.GetPageSize().GetValue(), string(decodedContToken))
	utils.LogDBStats(ctx, q.logger, "ReadTuples", 1, 0)

	tuples, continuationToken, err := q.backend.ReadByStore(ctx, req.GetStoreId(), paginationOptions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	encodedToken, err := q.encoder.Encode(continuationToken)
	if err != nil {
		return nil, serverErrors.NewInternalError("", err)
	}

	resp := &openfgav1pb.ReadTuplesResponse{
		Tuples:            tuples,
		ContinuationToken: encodedToken,
	}

	return resp, nil
}
