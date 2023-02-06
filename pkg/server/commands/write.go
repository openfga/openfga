package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const (
	IndirectWriteErrorReason = "Attempting to write directly to an indirect only relationship"
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
func (c *WriteCommand) Execute(ctx context.Context, req *openfgapb.WriteRequest) (*openfgapb.WriteResponse, error) {
	if err := c.validateWriteRequest(ctx, req); err != nil {
		return nil, err
	}

	err := c.datastore.Write(ctx, req.GetStoreId(), req.GetDeletes().GetTupleKeys(), req.GetWrites().GetTupleKeys())
	if err != nil {
		return nil, handleError(err)
	}

	return &openfgapb.WriteResponse{}, nil
}

func (c *WriteCommand) validateWriteRequest(ctx context.Context, req *openfgapb.WriteRequest) error {
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
			return err
		}

		typesys := typesystem.New(authModel)

		for _, tk := range writes {
			err := validation.ValidateTuple(typesys, tk)
			if err != nil {
				return serverErrors.ValidationError(err)
			}

			objectType, _ := tupleUtils.SplitObject(tk.GetObject())

			relation, err := typesys.GetRelation(objectType, tk.GetRelation())
			if err != nil {
				if errors.Is(err, typesystem.ErrObjectTypeUndefined) {
					return serverErrors.TypeNotFound(objectType)
				}

				if errors.Is(err, typesystem.ErrRelationUndefined) {
					return serverErrors.RelationNotFound(tk.GetRelation(), objectType, tk)
				}

				return serverErrors.HandleError("", err)
			}

			// Validate that we are not trying to write to an indirect-only relationship
			if !typesystem.RewriteContainsSelf(relation.GetRewrite()) {
				return serverErrors.HandleTupleValidateError(&tupleUtils.IndirectWriteError{Reason: IndirectWriteErrorReason, TupleKey: tk})
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
func (c *WriteCommand) validateNoDuplicatesAndCorrectSize(deletes []*openfgapb.TupleKey, writes []*openfgapb.TupleKey) error {
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
		return serverErrors.WriteFailedDueToInvalidInput(nil)
	} else if errors.Is(err, storage.ErrInvalidWriteInput) {
		return serverErrors.WriteFailedDueToInvalidInput(err)
	}

	return serverErrors.HandleError("", err)
}
