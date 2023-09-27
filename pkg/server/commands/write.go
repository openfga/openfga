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
	"google.golang.org/protobuf/types/known/structpb"
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
func (c *WriteCommand) Execute(ctx context.Context, req *openfgav1.WriteRequest) (*openfgav1.WriteResponse, error) {
	if err := c.validateWriteRequest(ctx, req); err != nil {
		return nil, err
	}

	err := c.datastore.Write(ctx, req.GetStoreId(), tupleUtils.ConvertWriteRequestsTupleKeyToTupleKeys(req.GetDeletes()), tupleUtils.ConvertWriteRequestsTupleKeyToTupleKeys(req.GetWrites()))
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
	deletes := req.GetDeletes()
	writes := req.GetWrites()

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

		for _, writeTk := range writes {
			tk := tupleUtils.ConvertWriteRequestTupleKeyToTupleKey(writeTk)
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

	for _, deleteTk := range deletes {
		tk := tupleUtils.ConvertWriteRequestTupleKeyToTupleKey(deleteTk)
		if ok := tupleUtils.IsValidUser(tk.GetUser()); !ok {
			return serverErrors.ValidationError(
				&tupleUtils.InvalidTupleError{
					Cause:    fmt.Errorf("the 'user' field is malformed"),
					TupleKey: tk,
				},
			)
		}
	}

	if err := c.validateNoDuplicatesAndCorrectSize(tupleUtils.ConvertWriteRequestsTupleKeyToTupleKeys(deletes), tupleUtils.ConvertWriteRequestsTupleKeyToTupleKeys(writes)); err != nil {
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

func validateConditionsInTuples(ts *typesystem.TypeSystem, deletes []*openfgav1.WriteRequestTupleKey, writes []*openfgav1.WriteRequestTupleKey) error {
	for _, write := range writes {
		objectType := tupleUtils.GetType(write.Object)
		userType := tupleUtils.GetType(write.User)
		userRelation := tupleUtils.GetRelation(write.User)
		relation, err := ts.GetRelation(objectType, write.Relation)
		if err != nil {
			return err
		}

		if write.Condition == nil {
			conditionShouldBeDefined := false
			for _, userset := range relation.TypeInfo.GetDirectlyRelatedUserTypes() {
				if userset.RelationOrWildcard == nil {
					if userset.Type == userType {
						conditionShouldBeDefined = true
					}
				}
				if _, ok := userset.RelationOrWildcard.(*openfgav1.RelationReference_Relation); ok {
					if userset.Type == userType && userset.GetRelation() == userRelation {
						conditionShouldBeDefined = true
					}
				}
				if _, ok := userset.RelationOrWildcard.(*openfgav1.RelationReference_Wildcard); ok {
					if userset.Type == userType {
						conditionShouldBeDefined = true
					}
				}
			}
			if conditionShouldBeDefined {
				return serverErrors.ValidationError(&tupleUtils.InvalidConditionalTupleError{
					Cause: fmt.Errorf("condition is missing"), TupleKey: write})
			}
		} else { // condition defined in the write
			conditionContext, conditionDefined := ts.GetConditions()[write.Condition.ConditionName]
			if !conditionDefined {
				return serverErrors.ValidationError(&tupleUtils.InvalidConditionalTupleError{
					Cause: fmt.Errorf("undefined condition"), TupleKey: write})
			}

			validCondition := false
			for _, userset := range relation.TypeInfo.GetDirectlyRelatedUserTypes() {
				if userset.Type == userType && userset.Condition == write.Condition.ConditionName {
					validCondition = true
				}
			}
			if !validCondition {
				return serverErrors.ValidationError(&tupleUtils.InvalidConditionalTupleError{
					Cause: fmt.Errorf("invalid condition for type restriction"), TupleKey: write})
			}

			for conditionInWriteParamName, conditionInWriteParamContext := range write.Condition.Context.Fields {
				_, paramDefined := conditionContext.Parameters[conditionInWriteParamName]
				if !paramDefined {
					return serverErrors.ValidationError(&tupleUtils.InvalidConditionalTupleError{
						Cause: fmt.Errorf("undefined parameter"), TupleKey: write})
				}
				paramIncorrectType := false
				switch conditionInWriteParamContext.Kind.(type) {
				case *structpb.Value_StringValue:
					//TODO
				case *structpb.Value_NumberValue:
					//TODO
				case *structpb.Value_BoolValue:
					//TODO
				case *structpb.Value_NullValue:
					//TODO
				case *structpb.Value_ListValue:
					//TODO
				case *structpb.Value_StructValue:
					//TODO
				}

				if paramIncorrectType {
					return serverErrors.ValidationError(&tupleUtils.InvalidConditionalTupleError{
						Cause: fmt.Errorf("invalid type for parameter"), TupleKey: write})
				}

			}
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
