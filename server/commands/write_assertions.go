package commands

import (
	"context"
	"net/http"
	"strconv"

	httpmiddleware "github.com/openfga/openfga/internal/middleware/http"
	"github.com/openfga/openfga/pkg/logger"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/utils"
	"github.com/openfga/openfga/pkg/utils/grpcutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

type WriteAssertionsCommand struct {
	assertionBackend      storage.AssertionsBackend
	typeDefinitionBackend storage.TypeDefinitionReadBackend
	logger                logger.Logger
}

func NewWriteAssertionsCommand(
	assertionBackend storage.AssertionsBackend,
	typeDefinitionBackend storage.TypeDefinitionReadBackend,
	logger logger.Logger,
) *WriteAssertionsCommand {
	return &WriteAssertionsCommand{
		assertionBackend:      assertionBackend,
		typeDefinitionBackend: typeDefinitionBackend,
		logger:                logger,
	}
}

func (w *WriteAssertionsCommand) Execute(ctx context.Context, req *openfgav1pb.WriteAssertionsRequest) (*openfgav1pb.WriteAssertionsResponse, error) {
	store := req.GetStoreId()
	authzModelId := req.GetAuthorizationModelId()
	assertions := req.Params.GetAssertions()
	dbCallsCounter := utils.NewDBCallCounter()

	for _, assertion := range assertions {
		if _, err := tupleUtils.ValidateTuple(ctx, w.typeDefinitionBackend, store, authzModelId, assertion.TupleKey, dbCallsCounter); err != nil {
			return nil, serverErrors.HandleTupleValidateError(err)
		}
	}
	dbCallsCounter.AddWriteCall()
	utils.LogDBStats(ctx, w.logger, "WriteAssertions", dbCallsCounter.GetReadCalls(), dbCallsCounter.GetWriteCalls())

	err := w.assertionBackend.WriteAssertions(ctx, store, authzModelId, assertions)
	if err != nil {
		return nil, serverErrors.HandleError("", err)
	}

	grpcutils.SetHeaderLogError(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusNoContent), w.logger)

	return &openfgav1pb.WriteAssertionsResponse{}, nil
}
