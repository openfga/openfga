package commands

import (
	"context"
	"errors"
	"fmt"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// WriteCommand is used to Write and Delete tuples. Instances may be safely shared by multiple goroutines.
type WriteCommand struct {
	logger    logger.Logger
	datastore storage.OpenFGADatastore
}

// NewWriteCommand creates a WriteCommand with specified storage.TupleBackend to use for storage.
func NewWriteCommand(datastore storage.OpenFGADatastore, logger logger.Logger) *WriteCommand {
	return &WriteCommand{
		logger:    logger,
		datastore: datastore,
	}
}

// Execute deletes and writes the specified tuples. Deletes are applied first, then writes.
func (c *WriteCommand) Execute(ctx context.Context, req *openfgav1.WriteRequest) (*openfgav1.WriteResponse, error) {
	if err := c.validateWriteRequest(ctx, req); err != nil {
		return nil, err
	}

	err := c.datastore.Write(ctx, req.GetStoreId(), req.GetDeletes().GetTupleKeys(), req.GetWrites().GetTupleKeys())
	if err != nil {
		return nil, handleError(err)
	}

	return &openfgav1.WriteResponse{}, nil
}

func (c *WriteCommand) validateWriteRequest(ctx context.Context, req *openfgav1.WriteRequest) error {
	ctx, span := tracer.Start(ctx, "validateWriteRequest")
	defer span.End()

	store := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()
	deletes := req.GetDeletes().GetTupleKeys()
	writes := req.GetWrites().GetTupleKeys()

	if deletes == nil && writes == nil {
		return serverErrors.InvalidWriteInput
	}

	if len(writes) > 0 {
		authModel, err := c.datastore.ReadAuthorizationModel(ctx, store, modelID)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return serverErrors.AuthorizationModelNotFound(modelID)
			}
			return err
		}

		if !typesystem.IsSchemaVersionSupported(authModel.GetSchemaVersion()) {
			return serverErrors.ValidationError(typesystem.ErrInvalidSchemaVersion)
		}

		typesys := typesystem.New(authModel)

		for _, tk := range writes {
			err := validation.ValidateTuple(typesys, tk)
			if err != nil {
				return serverErrors.ValidationError(err)
			}
		}
	}

	for _, tk := range deletes {
		if ok := tupleUtils.IsValidUser(tk.GetUser()); !ok {
			return serverErrors.ValidationError(
				&tupleUtils.InvalidTupleError{
					Cause:    fmt.Errorf("the 'user' field is malformed"),
					TupleKey: tk,
				},
			)
		}
	}

	if err := c.validateNoDuplicatesAndCorrectSize(deletes, writes); err != nil {
		return err
	}

	return nil
}

// validateNoDuplicatesAndCorrectSize ensures the deletes and writes contain no duplicates and length fits.
func (c *WriteCommand) validateNoDuplicatesAndCorrectSize(deletes []*openfgav1.TupleKey, writes []*openfgav1.TupleKey) error {
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
		return serverErrors.NewInternalError("concurrent write conflict", err)
	} else if errors.Is(err, storage.ErrInvalidWriteInput) {
		return serverErrors.WriteFailedDueToInvalidInput(err)
	}

	return serverErrors.HandleError("", err)
}
