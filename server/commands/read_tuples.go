package commands

import (
	"context"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// ReadTuplesQuery can be used to read tuples from a store.
type ReadTuplesQuery struct {
	backend   storage.TupleBackend
	logger    logger.Logger
	encrypter encrypter.Encrypter
	encoder   encoder.Encoder
}

// NewReadTuplesQuery creates a ReadTuplesQuery with specified `tupleBackend` to use for storage.
func NewReadTuplesQuery(backend storage.TupleBackend, logger logger.Logger, encrypter encrypter.Encrypter, encoder encoder.Encoder) *ReadTuplesQuery {
	return &ReadTuplesQuery{
		backend:   backend,
		logger:    logger,
		encrypter: encrypter,
		encoder:   encoder,
	}
}

// Execute the ReadTuplesQuery, returning the `openfga.Tuple`(s) for the store.
func (q *ReadTuplesQuery) Execute(ctx context.Context, req *openfgapb.ReadTuplesRequest) (*openfgapb.ReadTuplesResponse, error) {
	decodedContToken, err := utils.DecodeAndDecrypt(q.encrypter, q.encoder, req.GetContinuationToken())
	if err != nil {
		return nil, serverErrors.InvalidContinuationToken
	}

	paginationOptions := storage.NewPaginationOptions(req.GetPageSize().GetValue(), string(decodedContToken))
	utils.LogDBStats(ctx, q.logger, "ReadTuples", 1, 0)

	tuples, continuationToken, err := q.backend.ReadByStore(ctx, req.GetStoreId(), paginationOptions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	encodedToken, err := utils.EncryptAndEncode(q.encrypter, q.encoder, continuationToken)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	resp := &openfgapb.ReadTuplesResponse{
		Tuples:            tuples,
		ContinuationToken: encodedToken,
	}

	return resp, nil
}
