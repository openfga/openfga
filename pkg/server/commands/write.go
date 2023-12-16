package commands

import (
	"context"
	"errors"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

// WriteCommand is used to Write and Delete tuples. Instances may be safely shared by multiple goroutines.
type WriteCommand struct {
	logger                    logger.Logger
	datastore                 storage.OpenFGADatastore
	conditionContextByteLimit int
}

type WriteCommandResponse struct {
	*openfgav1.WriteResponse
	ModelIDUsed string
}

type WriteCommandOption func(*WriteCommand)

func WithWriteCmdLogger(l logger.Logger) WriteCommandOption {
	return func(wc *WriteCommand) {
		wc.logger = l
	}
}

func WithConditionContextByteLimit(limit int) WriteCommandOption {
	return func(wc *WriteCommand) {
		wc.conditionContextByteLimit = limit
	}
}

// NewWriteCommand creates a WriteCommand with specified storage.OpenFGADatastore to use for storage.
func NewWriteCommand(datastore storage.OpenFGADatastore, opts ...WriteCommandOption) *WriteCommand {
	cmd := &WriteCommand{
		datastore:                 datastore,
		logger:                    logger.NewNoopLogger(),
		conditionContextByteLimit: config.DefaultWriteContextByteLimit,
	}

	for _, opt := range opts {
		opt(cmd)
	}
	return cmd
}

// Execute deletes and writes the specified tuples. Deletes are applied first, then writes.
func (c *WriteCommand) Execute(ctx context.Context, req *openfgav1.WriteRequest) (*WriteCommandResponse, error) {
	modelID, err := c.validateWriteRequest(ctx, req)
	if err != nil {
		return nil, err
	}

	err = c.datastore.Write(
		ctx,
		req.GetStoreId(),
		req.GetDeletes().GetTupleKeys(),
		req.GetWrites().GetTupleKeys(),
	)
	if err != nil {
		return nil, handleError(err)
	}

	return &WriteCommandResponse{WriteResponse: &openfgav1.WriteResponse{}, ModelIDUsed: modelID}, nil
}

func (c *WriteCommand) validateWriteRequest(ctx context.Context, req *openfgav1.WriteRequest) (string, error) {
	ctx, span := tracer.Start(ctx, "validateWriteRequest")
	defer span.End()

	store := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()
	deletes := req.GetDeletes().GetTupleKeys()
	writes := req.GetWrites().GetTupleKeys()

	if len(deletes) == 0 && len(writes) == 0 {
		return modelID, serverErrors.InvalidWriteInput
	}

	if len(writes) > 0 {
		typesys, err := storage.ResolveAuthorizationModel(ctx, c.datastore, store, modelID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return modelID, serverErrors.AuthorizationModelNotFound(modelID)
			}
			if errors.Is(err, storage.ErrLatestAuthorizationModelNotFound) {
				return modelID, serverErrors.LatestAuthorizationModelNotFound(store)
			}
			return modelID, serverErrors.HandleError("", err)
		}
		modelID = typesys.GetAuthorizationModelID()

		for _, tk := range writes {
			err := validation.ValidateTuple(typesys, tk)
			if err != nil {
				return modelID, serverErrors.ValidationError(err)
			}

			contextSize := proto.Size(tk.GetCondition().GetContext())
			if contextSize > c.conditionContextByteLimit {
				return modelID, serverErrors.ValidationError(&tupleUtils.InvalidTupleError{
					Cause:    fmt.Errorf("condition context size limit exceeded: %d bytes exceeds %d bytes", contextSize, c.conditionContextByteLimit),
					TupleKey: tk,
				})
			}
		}
	}

	for _, tk := range deletes {
		if ok := tupleUtils.IsValidUser(tk.GetUser()); !ok {
			return "", serverErrors.ValidationError(
				&tupleUtils.InvalidTupleError{
					Cause:    fmt.Errorf("the 'user' field is malformed"),
					TupleKey: tk,
				},
			)
		}
	}

	if err := c.validateNoDuplicatesAndCorrectSize(deletes, writes); err != nil {
		return "", err
	}

	return modelID, nil
}

// validateNoDuplicatesAndCorrectSize ensures the deletes and writes contain no duplicates and length fits.
func (c *WriteCommand) validateNoDuplicatesAndCorrectSize(
	deletes []*openfgav1.TupleKeyWithoutCondition,
	writes []*openfgav1.TupleKey,
) error {
	tuples := map[string]struct{}{}

	for _, tk := range deletes {
		key := tupleUtils.TupleKeyToString(tk)
		if _, ok := tuples[key]; ok {
			return serverErrors.DuplicateTupleInWrite(tk)
		}
		tuples[key] = struct{}{}
	}

	for _, tk := range writes {
		key := tupleUtils.TupleKeyToString(tk)
		if _, ok := tuples[key]; ok {
			return serverErrors.DuplicateTupleInWrite(tk)
		}
		tuples[key] = struct{}{}
	}

	if len(tuples) > c.datastore.MaxTuplesPerWrite() {
		return serverErrors.ExceededEntityLimit("write operations", c.datastore.MaxTuplesPerWrite())
	}
	return nil
}

func handleError(err error) error {
	if errors.Is(err, storage.ErrTransactionalWriteFailed) {
		return status.Error(codes.Aborted, err.Error())
	} else if errors.Is(err, storage.ErrInvalidWriteInput) {
		return serverErrors.WriteFailedDueToInvalidInput(err)
	}

	return serverErrors.HandleError("", err)
}
