package commands

import (
	"context"
	"fmt"

	"github.com/oklog/ulid/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"go.uber.org/zap"

	"github.com/openfga/openfga/pkg/logger"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/typesystem"
)

// WriteAuthorizationModelCommand performs updates of the store authorization model.
type WriteAuthorizationModelCommand struct {
	backend                          storage.TypeDefinitionWriteBackend
	logger                           logger.Logger
	maxAuthorizationModelSizeInBytes int
}

type WriteAuthModelOption func(*WriteAuthorizationModelCommand)

func WithWriteAuthModelLogger(l logger.Logger) WriteAuthModelOption {
	return func(m *WriteAuthorizationModelCommand) {
		m.logger = l
	}
}

func WithWriteAuthModelMaxSizeInBytes(size int) WriteAuthModelOption {
	return func(m *WriteAuthorizationModelCommand) {
		m.maxAuthorizationModelSizeInBytes = size
	}
}

func NewWriteAuthorizationModelCommand(backend storage.TypeDefinitionWriteBackend, opts ...WriteAuthModelOption) *WriteAuthorizationModelCommand {
	model := &WriteAuthorizationModelCommand{
		backend:                          backend,
		logger:                           logger.NewNoopLogger(),
		maxAuthorizationModelSizeInBytes: serverconfig.DefaultMaxAuthorizationModelSizeInBytes,
	}

	for _, opt := range opts {
		opt(model)
	}
	return model
}

// Execute the command using the supplied request.
func (w *WriteAuthorizationModelCommand) Execute(ctx context.Context, req *openfgav1.WriteAuthorizationModelRequest) (*openfgav1.WriteAuthorizationModelResponse, error) {
	// Until this is solved: https://github.com/envoyproxy/protoc-gen-validate/issues/74
	if len(req.GetTypeDefinitions()) > w.backend.MaxTypesPerAuthorizationModel() {
		return nil, serverErrors.ExceededEntityLimit("type definitions in an authorization model", w.backend.MaxTypesPerAuthorizationModel())
	}

	// Fill in the schema version for old requests, which don't contain it, while we migrate to the new schema version.
	if req.GetSchemaVersion() == "" {
		req.SchemaVersion = typesystem.SchemaVersion1_1
	}

	model := &openfgav1.AuthorizationModel{
		Id:              ulid.Make().String(),
		SchemaVersion:   req.GetSchemaVersion(),
		TypeDefinitions: req.GetTypeDefinitions(),
		Conditions:      req.GetConditions(),
	}

	// Validate the size in bytes of the wire-format encoding of the authorization model.
	modelSize := proto.Size(model)
	if modelSize > w.maxAuthorizationModelSizeInBytes {
		// Consider using serverErrors.ExceededEntityLimit.
		return nil, status.Error(
			codes.Code(openfgav1.ErrorCode_exceeded_entity_limit),
			fmt.Sprintf("model exceeds size limit: %d bytes vs %d bytes", modelSize, w.maxAuthorizationModelSizeInBytes),
		)
	}

	typesys, err := typesystem.NewAndValidate(ctx, model)
	if err != nil {
		return nil, serverErrors.InvalidAuthorizationModelInput(err)
	}

	// The model passed validation, but the weighted authorization-model graph
	// can still fail to build (e.g., a TTU whose computed relation is missing
	// on one of the tupleset's directly related types). In that case every
	// weighted-graph-gated evaluation path silently falls back to the default
	// Check evaluator for the whole model. Surface the failure here, at
	// model-write time, so an operator sees the strategy change when the
	// model is deployed rather than only inferring it from degraded
	// performance later. See typesystem.WeightedGraphBuildError (issue #3306).
	if buildErr := typesys.WeightedGraphBuildError(); buildErr != nil {
		w.logger.WarnWithContext(ctx, "authorization model accepted, but the weighted graph failed to build; all relations fall back to the default Check evaluator",
			zap.String("authorization_model_id", model.GetId()),
			zap.Error(buildErr),
		)
	}

	err = w.backend.WriteAuthorizationModel(ctx, req.GetStoreId(), model)
	if err != nil {
		return nil, serverErrors.
			HandleError("Error writing authorization model configuration", err)
	}

	return &openfgav1.WriteAuthorizationModelResponse{
		AuthorizationModelId: model.GetId(),
	}, nil
}
