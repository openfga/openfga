package commands

import (
	"context"
	"fmt"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
)

// A ReadQuery can be used to read one or many tuplesets
// Each tupleset specifies keys of a set of relation tuples.
// The set can include a single tuple key, or all tuples with
// a given object ID or userset in a type, optionally
// constrained by a relation name.
type ReadQuery struct {
	datastore storage.OpenFGADatastore
	tracer    trace.Tracer
	logger    logger.Logger
	encoder   encoder.Encoder
}

// NewReadQuery creates a ReadQuery using the provided OpenFGA datastore implementation.
func NewReadQuery(datastore storage.OpenFGADatastore, tracer trace.Tracer, logger logger.Logger, encoder encoder.Encoder) *ReadQuery {
	return &ReadQuery{
		datastore: datastore,
		tracer:    tracer,
		logger:    logger,
		encoder:   encoder,
	}
}

// Execute the ReadQuery, returning paginated `openfga.Tuple`(s) that match the tuple. Return all tuples if the tuple is
// nil or empty.
func (q *ReadQuery) Execute(ctx context.Context, req *openfgapb.ReadRequest) (*openfgapb.ReadResponse, error) {
	store := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()
	tk := req.GetTupleKey()

	if tupleUtils.GetType(tk.GetObject()) == "" && tk.GetUser() == "" {
		return nil, serverErrors.ValidationError(fmt.Sprintf("objectID and user are required to read: '%s'", tk.String()))
	}

	decodedContToken, err := q.encoder.Decode(req.GetContinuationToken())
	if err != nil {
		return nil, serverErrors.InvalidContinuationToken
	}

	paginationOptions := storage.NewPaginationOptions(req.GetPageSize().GetValue(), string(decodedContToken))

	if _, err := q.datastore.ReadAuthorizationModel(ctx, store, modelID); err != nil {
		return nil, serverErrors.AuthorizationModelNotFound(modelID)
	}

	tuples, contToken, err := q.datastore.ReadPage(ctx, store, tk, paginationOptions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	encodedContToken, err := q.encoder.Encode(contToken)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	return &openfgapb.ReadResponse{
		Tuples:            tuples,
		ContinuationToken: encodedContToken,
	}, nil
}
