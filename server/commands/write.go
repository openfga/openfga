package commands

import (
	"context"
	"errors"

	"github.com/openfga/openfga/pkg/logger"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/validation"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
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

	for _, tk := range writes {
		tupleUserset, err := validation.ValidateTuple(ctx, c.datastore, store, modelID, tk, dbCallsCounter)
		if err != nil {
			return serverErrors.HandleTupleValidateError(err)
		}

		// Validate that we are not trying to write to an indirect-only relationship
		err = validateHasDirectRelationship(tupleUserset, tk)
		if err != nil {
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

func validateHasDirectRelationship(tupleUserset *openfgapb.Userset, tk *openfgapb.TupleKey) error {
	indirectWriteErrorReason := "Attempting to write directly to an indirect only relationship"
	switch usType := tupleUserset.Userset.(type) {
	case *openfgapb.Userset_Intersection:
		if !isDirectIntersection(usType, tk) {
			return serverErrors.HandleTupleValidateError(&tupleUtils.IndirectWriteError{Reason: indirectWriteErrorReason, TupleKey: tk})
		}

	case *openfgapb.Userset_Union:
		if !isDirectUnion(usType, tk) {
			return serverErrors.HandleTupleValidateError(&tupleUtils.IndirectWriteError{Reason: indirectWriteErrorReason, TupleKey: tk})
		}

	case *openfgapb.Userset_Difference:
		if !isDirectDifference(usType, tk) {
			return serverErrors.HandleTupleValidateError(&tupleUtils.IndirectWriteError{Reason: indirectWriteErrorReason, TupleKey: tk})
		}

	case *openfgapb.Userset_ComputedUserset:
		// if Userset.type is a ComputedUserset then we know it can't be direct
		return serverErrors.HandleTupleValidateError(&tupleUtils.IndirectWriteError{Reason: indirectWriteErrorReason, TupleKey: tk})

	case *openfgapb.Userset_TupleToUserset:
		// if Userset.type is a TupleToUserset then we know it can't be direct
		return serverErrors.HandleTupleValidateError(&tupleUtils.IndirectWriteError{Reason: indirectWriteErrorReason, TupleKey: tk})

	default:
		return nil
	}

	return nil
}

func isDirectIntersection(nodes *openfgapb.Userset_Intersection, tk *openfgapb.TupleKey) bool {
	for _, userset := range nodes.Intersection.Child {
		switch userset.Userset.(type) {
		case *openfgapb.Userset_This:
			return true

		default:
			continue
		}
	}

	return false
}

func isDirectUnion(nodes *openfgapb.Userset_Union, tk *openfgapb.TupleKey) bool {
	for _, userset := range nodes.Union.Child {
		switch userset.Userset.(type) {
		case *openfgapb.Userset_This:
			return true

		default:
			continue
		}
	}

	return false
}

func isDirectDifference(node *openfgapb.Userset_Difference, tk *openfgapb.TupleKey) bool {
	sets := []*openfgapb.Userset{node.Difference.GetBase(), node.Difference.GetSubtract()}
	for _, userset := range sets {
		switch userset.Userset.(type) {
		case *openfgapb.Userset_This:
			return true

		default:
			continue
		}
	}

	return false
}
