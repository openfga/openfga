package commands

import (
	"context"
	"errors"
	"fmt"

	"github.com/openfga/openfga/pkg/logger"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/validation"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
)

const (
	IndirectWriteErrorReason = "Attempting to write directly to an indirect only relationship"
)

// WriteCommand is used to Write and Delete tuples. Instances may be safely shared by multiple goroutines.
type WriteCommand struct {
	logger    logger.Logger
	tracer    trace.Tracer
	datastore storage.OpenFGADatastore
}

// NewWriteCommand creates a WriteCommand with specified storage.TupleBackend to use for storage.
func NewWriteCommand(datastore storage.OpenFGADatastore, tracer trace.Tracer, logger logger.Logger) *WriteCommand {
	return &WriteCommand{
		logger:    logger,
		tracer:    tracer,
		datastore: datastore,
	}
}

// Execute deletes and writes the specified tuples. Deletes are applied first, then writes.
func (c *WriteCommand) Execute(ctx context.Context, req *openfgapb.WriteRequest) (*openfgapb.WriteResponse, error) {
	dbCallsCounter := utils.NewDBCallCounter()
	if err := c.validateTuplesets(ctx, req, dbCallsCounter); err != nil {
		utils.LogDBStats(ctx, c.logger, "Write", dbCallsCounter.GetReadCalls(), 0)
		return nil, err
	}

	utils.LogDBStats(ctx, c.logger, "Write", dbCallsCounter.GetReadCalls(), 1)
	err := c.datastore.Write(ctx, req.GetStoreId(), req.GetDeletes().GetTupleKeys(), req.GetWrites().GetTupleKeys())
	if err != nil {
		return nil, handleError(err)
	}

	return &openfgapb.WriteResponse{}, nil
}

func (c *WriteCommand) validateTuplesets(ctx context.Context, req *openfgapb.WriteRequest, dbCallsCounter utils.DBCallCounter) error {
	ctx, span := c.tracer.Start(ctx, "validateAndAuthenticateTuplesets")
	defer span.End()

	store := req.GetStoreId()
	modelID := req.GetAuthorizationModelId()
	deletes := req.GetDeletes().GetTupleKeys()
	writes := req.GetWrites().GetTupleKeys()

	if deletes == nil && writes == nil {
		return serverErrors.InvalidWriteInput
	}

	var authModel *openfgapb.AuthorizationModel

	if len(writes) > 0 {
		// only read the auth model if we are adding tuples
		dbCallsCounter.AddReadCall()
		var err error
		authModel, err = c.datastore.ReadAuthorizationModel(ctx, store, modelID)
		if err != nil {
			return err
		}
	}

	for _, tk := range writes {
		tupleUserset, err := validation.ValidateTuple(ctx, c.datastore, store, modelID, tk, dbCallsCounter)
		if err != nil {
			return serverErrors.HandleTupleValidateError(err)
		}

		// Validate that we are not trying to write to an indirect-only relationship
		if !typesystem.ContainsSelf(tupleUserset) {
			return serverErrors.HandleTupleValidateError(&tupleUtils.IndirectWriteError{Reason: IndirectWriteErrorReason, TupleKey: tk})
		}

		if err := c.validateTypesForTuple(authModel, tk); err != nil {
			return err
		}
	}

	for _, tk := range deletes {
		// For delete, we only need to ensure it is well form but no need to validate whether relation exists
		if err := tupleUtils.ValidateUser(tk); err != nil {
			return serverErrors.HandleTupleValidateError(err)
		}
	}

	if err := c.validateNoDuplicatesAndCorrectSize(deletes, writes); err != nil {
		return err
	}

	return nil
}

// validateTypesForTuple makes sure that when writing a tuple, the types are compatible.
// 1. If the tuple is of the form (user=person:bob, relation=reader, object=doc:budget), then the type "doc", relation "reader" must allow type "person".
// 2. If the tuple is of the form (user=group:abc#member, relation=reader, object=doc:budget), then the type "doc", relation "reader" must allow type "group", relation "member".
// 3. If the tuple is of the form (user=*, relation=reader, object=doc:budget), we allow it only if the type "doc" relation "reader" allows at least one type (with no relation)
func (c *WriteCommand) validateTypesForTuple(authModel *openfgapb.AuthorizationModel, tk *openfgapb.TupleKey) error {
	objectType, _ := tupleUtils.SplitObject(tk.GetObject())    // e.g. "doc"
	userType, userID := tupleUtils.SplitObject(tk.GetUser())   // e.g. (person, bob) or (group, abc#member) or ("", *)
	_, userRel := tupleUtils.SplitObjectRelation(tk.GetUser()) // e.g. (person:bob, "") or (group:abc, member) or (*, "")

	ts := typesystem.New(authModel)

	typeDefinitionForObject := ts.TypeDefinitions[objectType]
	typeDefinitionForUser := ts.TypeDefinitions[userType]

	relationsForObject := typeDefinitionForObject.GetMetadata().GetRelations()
	if relationsForObject == nil {
		if ts.Version == "1.1" {
			// if we get here, there's a bug in the validation of WriteAuthorizationModel API
			msg := "invalid authorization model"
			return serverErrors.NewInternalError(msg, errors.New(msg))
		} else {
			// authorization model is old/unspecified and does not have type information
			return nil
		}
	}

	// at this point we know the auth model has type information

	if userType != "" && typeDefinitionForUser == nil {
		return serverErrors.InvalidWriteInput
	}

	relationInformation := relationsForObject[tk.Relation]

	// case 1
	if userRel == "" && userID != "*" {
		for _, typeInformation := range relationInformation.GetDirectlyRelatedUserTypes() {
			if typeInformation.GetType() == userType {
				return nil
			}
		}
	} else if userRel != "" { // case 2
		for _, typeInformation := range relationInformation.GetDirectlyRelatedUserTypes() {
			if typeInformation.GetType() == userType && typeInformation.GetRelation() == userRel {
				return nil
			}
		}
	} else if userID == "*" { // case 3
		for _, typeInformation := range relationInformation.GetDirectlyRelatedUserTypes() {
			if typeInformation.GetType() != "" && typeInformation.GetRelation() == "" {
				return nil
			}
		}
	}

	return serverErrors.InvalidTuple(fmt.Sprintf("User '%s' is not allowed to have relation %s with %s", tk.User, tk.Relation, tk.Object), tk)
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
	if len(tuples) > c.datastore.MaxTuplesInWriteOperation() {
		return serverErrors.ExceededEntityLimit("write operations", c.datastore.MaxTuplesInWriteOperation())
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
