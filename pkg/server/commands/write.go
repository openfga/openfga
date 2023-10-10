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

const (
	IndirectWriteErrorReason = "Attempting to write directly to an indirect only relationship"
)

// WriteCommand is used to Write and Delete tuples. Instances may be safely shared by multiple
// goroutines.
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
func (c *WriteCommand) Execute(
	ctx context.Context,
	req *openfgav1.WriteRequest,
) (*openfgav1.WriteResponse, error) {
	if err := c.validateWriteRequest(ctx, req); err != nil {
		return nil, err
	}

	err := c.datastore.Write(
		ctx,
		req.GetStoreId(),
		tupleUtils.ConvertWriteRequestsTupleKeysToTupleKeys(req.GetDeletes()),
		tupleUtils.ConvertWriteRequestsTupleKeysToTupleKeys(req.GetWrites()),
	)
	if err != nil {
		return nil, handleError(err)
	}

	return &openfgav1.WriteResponse{}, nil
}

func (c *WriteCommand) validateWriteRequest(
	ctx context.Context,
	req *openfgav1.WriteRequest,
) error {
	ctx, span := tracer.Start(ctx, "validateWriteRequest")
	defer span.End()

	store := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()
	deletes := tupleUtils.ConvertWriteRequestsTupleKeysToTupleKeys(req.GetDeletes())
	writes := tupleUtils.ConvertWriteRequestsTupleKeysToTupleKeys(req.GetWrites())

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
				return serverErrors.HandleTupleValidateError(
					&tupleUtils.IndirectWriteError{Reason: IndirectWriteErrorReason, TupleKey: tk},
				)
			}

			if err := c.validateConditionInTuple(typesys, tk); err != nil {
				return err
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

// validateNoDuplicatesAndCorrectSize ensures the deletes and writes contain no duplicates and
// length fits.
func (c *WriteCommand) validateNoDuplicatesAndCorrectSize(
	deletes []*openfgav1.TupleKey,
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

func (c *WriteCommand) validateConditionInTuple(
	ts *typesystem.TypeSystem,
	tk *openfgav1.TupleKey,
) error {
	conditions := ts.GetConditions()
	objectType := tupleUtils.GetType(tk.Object)
	userType := tupleUtils.GetType(tk.User)
	userRelation := tupleUtils.GetRelation(tk.User)

	typeRestrictions, err := ts.GetDirectlyRelatedUserTypes(objectType, tk.Relation)
	if err != nil {
		return err
	}

	if tk.Condition == nil {
		hasConditionedTypeRestriction := false
		hasUnconditionedTypeRestriction := false

		for _, userset := range typeRestrictions {
			if userset.Type != userType {
				continue
			}

			if userset.Condition == "" {
				hasUnconditionedTypeRestriction = true
				continue
			}

			if userset.RelationOrWildcard == nil {
				hasConditionedTypeRestriction = true
				continue
			}

			if _, ok := userset.RelationOrWildcard.(*openfgav1.RelationReference_Relation); ok {
				if userset.GetRelation() == userRelation {
					hasConditionedTypeRestriction = true
					continue
				}
			}
			if _, ok := userset.RelationOrWildcard.(*openfgav1.RelationReference_Wildcard); ok {
				hasConditionedTypeRestriction = true
				continue
			}
		}

		// A condition is only required if all the direct relationship type
		// restrictions are conditioned. Example:
		// define viewer: [user, user with condition]: not required
		// define viewer: [user]: not required
		// define viewer: [user with condition]: required
		if hasConditionedTypeRestriction && !hasUnconditionedTypeRestriction {
			return serverErrors.ValidationError(&tupleUtils.InvalidConditionalTupleError{
				Cause: fmt.Errorf("condition is missing"), TupleKey: tk,
			})
		}

		return nil
	} else { // condition defined in the write
		condition, conditionDefined := conditions[tk.Condition.ConditionName]
		if !conditionDefined {
			return serverErrors.ValidationError(&tupleUtils.InvalidConditionalTupleError{
				Cause: fmt.Errorf("undefined condition"), TupleKey: tk,
			})
		}

		validCondition := false
		for _, userset := range typeRestrictions {
			if userset.Type == userType && userset.Condition == tk.Condition.ConditionName {
				validCondition = true
				continue
			}
		}

		if !validCondition {
			return serverErrors.ValidationError(&tupleUtils.InvalidConditionalTupleError{
				Cause: fmt.Errorf("invalid condition for type restriction"), TupleKey: tk,
			})
		}

		_, err := condition.CastContextToTypedParameters(tk.Condition.Context.AsMap())
		if err != nil {
			return serverErrors.ValidationError(&tupleUtils.InvalidConditionalTupleError{
				Cause: err, TupleKey: tk,
			})
		}
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
