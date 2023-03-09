package commands

import (
	"context"

	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type WriteAssertionsCommand struct {
	datastore storage.OpenFGADatastore
	logger    logger.Logger
	typesys   *typesystem.TypeSystem
}

func NewWriteAssertionsCommand(
	datastore storage.OpenFGADatastore,
	logger logger.Logger,
	typesys *typesystem.TypeSystem,
) *WriteAssertionsCommand {
	return &WriteAssertionsCommand{
		datastore: datastore,
		logger:    logger,
		typesys:   typesys,
	}
}

func (w *WriteAssertionsCommand) Execute(ctx context.Context, req *openfgapb.WriteAssertionsRequest) (*openfgapb.WriteAssertionsResponse, error) {
	store := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()
	assertions := req.GetAssertions()

	for _, assertion := range assertions {
		if err := validation.ValidateUserObjectRelation(w.typesys, assertion.TupleKey); err != nil {
			return nil, serverErrors.ValidationError(err)
		}
	}

	err := w.datastore.WriteAssertions(ctx, store, modelID, assertions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	return &openfgapb.WriteAssertionsResponse{}, nil
}
