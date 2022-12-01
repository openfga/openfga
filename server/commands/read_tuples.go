package commands

import (
	"context"
	"errors"

	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

// ReadTuplesQuery can be used to read tuples from a store.
type ReadTuplesQuery struct {
	backend storage.TupleBackend
	logger  logger.Logger
	encoder encoder.Encoder
}

// NewReadTuplesQuery constructs a ReadTuplesQuery with the provided storage backend.
func NewReadTuplesQuery(backend storage.TupleBackend, logger logger.Logger, encoder encoder.Encoder) *ReadTuplesQuery {
	return &ReadTuplesQuery{
		backend: backend,
		logger:  logger,
		encoder: encoder,
	}
}

// Execute the ReadTuplesQuery, returning the `openfga.Tuple`(s) for the store.
func (q *ReadTuplesQuery) Execute(ctx context.Context, req *openfgapb.ReadTuplesRequest) (*openfgapb.ReadTuplesResponse, error) {
	return nil, errors.New("unimplemented")
}
