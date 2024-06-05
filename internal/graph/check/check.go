package check

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"go.opentelemetry.io/otel"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/dispatch"
	dispatchv1 "github.com/openfga/openfga/internal/proto/dispatch/v1alpha1"
	"github.com/openfga/openfga/internal/utils"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/types"
	"github.com/openfga/openfga/pkg/typesystem"
)

var tracer = otel.Tracer("internal/graph/check")

const (
	DefaultMaxDispatchBatchSize    = 100
	DefaultMaxConcurrentDispatches = 100
	DefaultMaxReadQueriesInflight  = math.MaxUint32
)

// CheckResolver implements the main Check resolution mechanics.
//
// A CheckResolver orchestrates resolution of a Check query by following relationship rewrite
// rules defined in the provided FGA model (provided via the TypeSystem) and doing various
// database lookups and delegating/dispatching subproblems to the provided dispatch.Dispatcher
// as needed.
type CheckResolver struct {
	tupleReader storage.RelationshipTupleReader
	dispatcher  dispatch.CheckDispatcher
	limits      checkResolverLimits
}

type checkResolverLimits struct {
	maxReadQueriesInflight  int
	maxDispatchBatchSize    int
	maxConcurrentDispatches int
}

type CheckResolverOpt func(c *CheckResolver)

// WithMaxDispatchBatchSize sets the maximum size of a batch of objects ids to be included in a
// single dispatched request.
func WithMaxDispatchBatchSize(batchSize int) CheckResolverOpt {
	return func(c *CheckResolver) {
		c.limits.maxDispatchBatchSize = batchSize
	}
}

// WithMaxConcurrentDispatches sets the maximum number of concurrent (outbound) dispatches for a given branch of resolution
// w.r.t. direct relationships involving non-terminal subproblems (e.g. usersets requiring further dispatch).
func WithMaxConcurrentDispatches(concurrentDispatches int) CheckResolverOpt {
	return func(c *CheckResolver) {
		c.limits.maxConcurrentDispatches = concurrentDispatches
	}
}

// WithMaxReadQueriesInflight sets the maximum number of concurrent (outbound) queries to the storage
// layer that can be in flight
func WithMaxReadQueriesInflight(maxQueriesInflight int) CheckResolverOpt {
	return func(c *CheckResolver) {
		c.limits.maxReadQueriesInflight = maxQueriesInflight
	}
}

// WithDispatcher sets the CheckDispatcher delegate to be used for the CheckResolver.
//
// Any Check subproblems that must be dispatched for resolution will get dispatched through
// this dispatcher.
func WithDispatcher(dispatcher dispatch.CheckDispatcher) CheckResolverOpt {
	return func(c *CheckResolver) {
		c.dispatcher = dispatcher
	}
}

func NewCheckResolver(
	reader storage.RelationshipTupleReader,
	opts ...CheckResolverOpt,
) *CheckResolver {
	checkResolver := &CheckResolver{
		tupleReader: reader,
		limits: checkResolverLimits{
			maxReadQueriesInflight:  DefaultMaxReadQueriesInflight,
			maxDispatchBatchSize:    DefaultMaxDispatchBatchSize,
			maxConcurrentDispatches: DefaultMaxConcurrentDispatches,
		},
	}

	for _, opt := range opts {
		opt(checkResolver)
	}

	return checkResolver
}

// checkContext stores the current Check subproblem context. We use this in favor of passing values via Go context
// to make it more explicitly part of the function signatures.
type checkRequestContext struct {
	tupleReader        storage.RelationshipTupleReader
	resolutionBehavior dispatchv1.DispatchCheckRequest_ResolutionBehavior
}

type CheckRequest struct {
	StoreID         string
	Typesystem      *typesystem.TypeSystem
	ObjectType      string
	ObjectIDs       []string
	Relation        string
	SubjectType     string
	SubjectID       string
	SubjectRelation string

	checkCtx checkRequestContext
}

type CheckResponse struct {
	DispatchResp *dispatchv1.DispatchCheckResponse
	Err          error
}

// WithDispatcher sets this CheckResolver's CheckDispatcher delegate for dispatching
// subproblems.
func (c *CheckResolver) WithDispatcher(dispatcher dispatch.CheckDispatcher) *CheckResolver {
	c.dispatcher = dispatcher
	return c
}

// Check is the public (top-level) entrypoint which implements internal Check resolution.
//
// This function does a little boilerplate setup but promptly delegates to the unexported
// check function contained herein.
func (c *CheckResolver) Check(
	ctx context.Context,
	req *CheckRequest,
) (*dispatchv1.DispatchCheckResponse, error) {
	ctx, span := tracer.Start(ctx, "Check")
	defer span.End()

	if req.checkCtx.tupleReader == nil {
		req.checkCtx.tupleReader = storagewrappers.NewBoundedConcurrencyTupleReader(c.tupleReader, uint32(c.limits.maxReadQueriesInflight))
	}

	if req.checkCtx.resolutionBehavior == dispatchv1.DispatchCheckRequest_RESOLUTION_BEHAVIOR_UNSPECIFIED {
		req.checkCtx.resolutionBehavior = dispatchv1.DispatchCheckRequest_RESOLUTION_BEHAVIOR_RESOLVE_ALL
	}

	checkResp := c.check(ctx, req)
	if checkResp.Err != nil {
		return nil, checkResp.Err
	}

	return checkResp.DispatchCheckResponse, nil
}

// check is the main (internal) entrypoint which implements the mechanics of the Check algorithm.
func (c *CheckResolver) check(
	ctx context.Context,
	req *CheckRequest,
) DispatchCheckResponseOrError {
	ctx, span := tracer.Start(ctx, "check")
	defer span.End()

	relation, err := req.Typesystem.GetRelation(req.ObjectType, req.Relation)
	if err != nil {
		return DispatchCheckResponseOrError{Err: err}
	}

	objectIDsRequiringEval := utils.Uniq(req.ObjectIDs)

	resolvedCheckSet := NewResolvedCheckSet()

	// handle the reflexive property - for any subject matching the incoming object#relation,
	// add the result to the resolved set and remove that item from the list of residual objectIDs
	// that need further evaluation.
	//
	// For example,
	//
	// req.ObjectType: "group", req.ObjectIDs: []string{"eng", "fga"}, req.Relation: "member"
	// req.SubjectType: "group", req.SubjectID: "eng", req.SubjectRelation: "member"
	//
	// We can add `group:eng#member` to the resolved set, and the residual set contains `group:fga#member`
	if req.ObjectType == req.SubjectType && req.Relation == req.SubjectRelation {
		i := slices.Index(objectIDsRequiringEval, req.SubjectID)
		if i > -1 {
			resolvedCheckSet.Set(req.SubjectID, &dispatchv1.DispatchCheckResult{
				Allowed: true,
			})

			objectIDsRequiringEval = append(objectIDsRequiringEval[:i], objectIDsRequiringEval[i+1:]...)
		}
	}

	if len(objectIDsRequiringEval) == 0 {
		return dispatchCheckResponseFromResolvedCheckSet(resolvedCheckSet, emptyDispatchCheckResolutionMetadata())
	}

	resolutionBehavior := req.checkCtx.resolutionBehavior
	if len(objectIDsRequiringEval) == 1 {
		// if we're only evaluating a single residual objectID, then this Check resolution and any
		// child subproblems derived from it inherit the same resolution behavior - only
		// the first resolution path that returns a permittable decision is required.
		resolutionBehavior = dispatchv1.DispatchCheckRequest_RESOLUTION_BEHAVIOR_RESOLVE_ANY
	}

	rewrite := relation.GetRewrite()
	checkRespOrErr := c.checkRewrite(ctx, &CheckRequest{
		StoreID:         req.StoreID,
		Typesystem:      req.Typesystem,
		ObjectType:      req.ObjectType,
		ObjectIDs:       objectIDsRequiringEval,
		Relation:        req.Relation,
		SubjectType:     req.SubjectType,
		SubjectID:       req.SubjectID,
		SubjectRelation: req.SubjectRelation,
		checkCtx: checkRequestContext{
			tupleReader:        req.checkCtx.tupleReader,
			resolutionBehavior: resolutionBehavior,
		},
	}, rewrite)
	if checkRespOrErr.Err != nil {
		return checkRespOrErr
	}

	mergedResolvedCheckSet := resolvedCheckSet.Union(checkRespOrErr.GetResultsForeachObjectId())
	return dispatchCheckResponseFromResolvedCheckSet(mergedResolvedCheckSet, checkRespOrErr.GetResolutionMetadata())
}

func (c *CheckResolver) checkRewrite(
	ctx context.Context,
	req *CheckRequest,
	rewrite *openfgav1.Userset,
) DispatchCheckResponseOrError {
	switch rw := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return c.checkDirect(ctx, req)
	case *openfgav1.Userset_ComputedUserset:
		_ = rw
	case *openfgav1.Userset_TupleToUserset:
		_ = rw
	case *openfgav1.Userset_Union:
	case *openfgav1.Userset_Intersection:
	case *openfgav1.Userset_Difference:
	default:
		return DispatchCheckResponseOrError{Err: fmt.Errorf("unexpected relation rewrite rule encountered")}
	}

	return DispatchCheckResponseOrError{Err: fmt.Errorf("unimplemented rewrite rule")}
}

// checkDirect is used to resolve direct relationship rewrite rules.
func (c *CheckResolver) checkDirect(
	ctx context.Context,
	req *CheckRequest,
) DispatchCheckResponseOrError {
	ctx, span := tracer.Start(ctx, "checkDirect")
	defer span.End()

	typesys := req.Typesystem

	storeID := req.StoreID
	modelID := typesys.GetAuthorizationModelID()
	objectType := req.ObjectType
	objectIDs := req.ObjectIDs
	relation := req.Relation
	subjectType := req.SubjectType
	subjectID := req.SubjectID
	subjectRelation := req.SubjectRelation

	directlyAssignableTypes, err := typesys.GetDirectlyRelatedUserTypes(req.ObjectType, req.Relation)
	if err != nil {
		return DispatchCheckResponseOrError{Err: err}
	}

	reachabilityInfo := typesystem.SubjectReachability(subjectType, subjectRelation, directlyAssignableTypes)

	tupleReader := req.checkCtx.tupleReader

	objectIDsRequiringEval := ObjectIDsSetFromSlice(req.ObjectIDs...)

	resolvedCheckSet := ResolvedCheckSetFromSlice(req.ObjectIDs...)

	datastoreQueryCount := uint32(0)
	dispatchCount := uint32(0)

	if reachabilityInfo.HasPossibleDirectRelationship || reachabilityInfo.HasPossibleWildcardRelationship {
		filter := storage.ReadRelationshipTuplesFilter{
			ObjectType: objectType,
			ObjectIDs:  objectIDs,
			Relation:   relation,
		}

		if reachabilityInfo.HasPossibleDirectRelationship {
			filter.SubjectsFilter = append(filter.SubjectsFilter, storage.SubjectsFilter{
				SubjectType:     subjectType,
				SubjectIDs:      []string{subjectID}, // search for the terminal subject (e.g. 'user:jon')
				SubjectRelation: "",
			})
		}

		if reachabilityInfo.HasPossibleWildcardRelationship {
			filter.SubjectsFilter = append(filter.SubjectsFilter, storage.SubjectsFilter{
				SubjectType:     subjectType,
				SubjectIDs:      []string{tuple.Wildcard}, // search for the wildcard subject (e.g. 'user:*')
				SubjectRelation: "",
			})
		}

		iter, err := tupleReader.ReadRelationshipTuples(ctx, storeID, filter)
		if err != nil {
			return DispatchCheckResponseOrError{Err: err}
		}
		defer iter.Stop()
		datastoreQueryCount++

		for {
			relationshipTuple, err := iter.Next(ctx)
			if err != nil {
				if errors.Is(err, storage.ErrIteratorDone) {
					break
				}

				return DispatchCheckResponseOrError{Err: err}
			}

			foundObjectID := relationshipTuple.Object.ID
			resolvedCheckSet.Get(foundObjectID).Allowed = true
			objectIDsRequiringEval.Remove(foundObjectID)

			if req.checkCtx.resolutionBehavior == dispatchv1.DispatchCheckRequest_RESOLUTION_BEHAVIOR_RESOLVE_ANY {
				return dispatchCheckResponseFromResolvedCheckSet(resolvedCheckSet, &dispatchv1.DispatchCheckResolutionMetadata{
					// Depth: todo: add the depth
					DatastoreQueryCount: datastoreQueryCount,
					DispatchCount:       dispatchCount,
				})
			}
		}
		iter.Stop() // promptly stop the iterator after we're done here to release it quicker
	}

	if !reachabilityInfo.HasNonTerminalRelationships() || len(objectIDsRequiringEval) == 0 {
		return dispatchCheckResponseFromResolvedCheckSet(resolvedCheckSet, &dispatchv1.DispatchCheckResolutionMetadata{
			DatastoreQueryCount: datastoreQueryCount,
			DispatchCount:       dispatchCount,
		})
	}

	// for any of the residual set of objectIDs requiring further evaluation, evaluate
	// the non-terminal relationships (e.g. the usersets requiring dispatch)

	filter := storage.ReadRelationshipTuplesFilter{
		ObjectType: objectType,
		ObjectIDs:  objectIDsRequiringEval.AsSlice(),
		Relation:   relation,
	}

	for _, relationRef := range reachabilityInfo.NonTerminalRelationRefs {
		// search for the non-terminal subjects (e.g. 'group:eng#member') or the ones requiring further dispatch
		filter.SubjectsFilter = append(filter.SubjectsFilter, storage.SubjectsFilter{
			SubjectType:     relationRef.GetType(),
			SubjectRelation: relationRef.GetRelation(),
		})
	}

	iter, err := tupleReader.ReadRelationshipTuples(ctx, storeID, filter)
	if err != nil {
		return DispatchCheckResponseOrError{Err: err}
	}
	defer iter.Stop()
	datastoreQueryCount++

	dispatchSet := DispatchSetGroup{}
	subjectMappedTuples := map[string][]*types.RelationshipTuple{}

	for {
		relationshipTuple, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}

			return DispatchCheckResponseOrError{Err: err}
		}

		subject, ok := relationshipTuple.Subject.(*types.Userset)
		if !ok {
			return DispatchCheckResponseOrError{Err: fmt.Errorf("expected a userset subject to be returned")}
		}

		dispatchSet.Add(DispatchItem{
			Type:     subject.Type,
			Relation: subject.Relation,
			ObjectID: subject.ID,
		})

		subjectKey := subject.String()
		subjectMappedTuples[subjectKey] = append(subjectMappedTuples[subjectKey], relationshipTuple)
	}
	iter.Stop() // promptly stop the iterator after we're done here to release it quicker

	// batch the subproblems
	var subproblemsToDispatch []IndividualDispatch

	for _, item := range dispatchSet {
		objectIDBatches := utils.Chunk(item.ObjectIDs, c.limits.maxDispatchBatchSize)

		for _, objectIDBatch := range objectIDBatches {
			subproblemsToDispatch = append(subproblemsToDispatch, IndividualDispatch{
				Type:      item.Type,
				Relation:  item.Relation,
				ObjectIDs: objectIDBatch,
			})
		}
	}

	// dispatch the batches concurrently (up to maxConcurrentDispatches at once)
	checkRespOrErr := union(
		ctx,
		req.checkCtx,
		subproblemsToDispatch,
		func(innerCtx context.Context, individualDispatch IndividualDispatch) DispatchCheckResponseOrError {
			// Code within this func will get executed concurrently in a goroutine within the 'union'
			// reducer. Avoid (as much as possible) contention through sync primitives.
			dispatchedCheckRespOrErr := c.dispatch(innerCtx, &dispatchv1.DispatchCheckRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				ObjectType:           individualDispatch.Type,
				ObjectIds:            individualDispatch.ObjectIDs,
				Relation:             individualDispatch.Relation,
				SubjectType:          subjectType,
				SubjectId:            subjectID,
				SubjectRelation:      subjectRelation,
				RequestMetadata: &dispatchv1.DispatchCheckRequestMetadata{
					// todo: finish filling out RequestMetadata
					DatastoreQueryCount: datastoreQueryCount,
					DispatchCount:       dispatchCount + 1,
				},
				ResolutionBehavior: req.checkCtx.resolutionBehavior,
			})

			// map the resolved objectIDs to the subject involved in the dispatch
			dispatchedResolvedCheckSet := NewResolvedCheckSet()
			for resolvedObjectID, checkResult := range dispatchedCheckRespOrErr.GetResultsForeachObjectId() {
				resolvedSubject := types.Userset{
					Type:     individualDispatch.Type,
					ID:       resolvedObjectID,
					Relation: individualDispatch.Relation,
				}

				resolvedSubjectKey := resolvedSubject.String()
				for _, relationshipTuple := range subjectMappedTuples[resolvedSubjectKey] {
					dispatchedResolvedCheckSet.Set(relationshipTuple.Object.ID, checkResult)
				}
			}

			return dispatchCheckResponseFromResolvedCheckSet(
				dispatchedResolvedCheckSet,
				dispatchedCheckRespOrErr.GetResolutionMetadata(),
			)
		},
		c.limits.maxConcurrentDispatches,
	)
	if checkRespOrErr.Err != nil {
		return checkRespOrErr
	}

	mergedResolvedCheckSet := resolvedCheckSet.Union(checkRespOrErr.GetResultsForeachObjectId())

	return dispatchCheckResponseFromResolvedCheckSet(
		mergedResolvedCheckSet,
		checkRespOrErr.GetResolutionMetadata(),
	)
}

// dispatch dispatches the provided request to this CheckResolver's Dispatcher delegate.
func (c *CheckResolver) dispatch(
	ctx context.Context,
	req *dispatchv1.DispatchCheckRequest,
) DispatchCheckResponseOrError {
	resp, err := c.dispatcher.DispatchCheck(ctx, req)
	if err != nil {
		return DispatchCheckResponseOrError{Err: err}
	}

	return normalizeDispatchCheckResponse(resp)
}

// checkReducer is any function which takes a context and some operand of a generic
// type and produces a DispatchCheckResponseOrError.
type checkReducer[T any] func(ctx context.Context, operand T) DispatchCheckResponseOrError

// union invokes the reducer for each of the provided operands, limited up to 'maxBatchesAtOnce' at a time, and
// it terminates as soon as the first reducer finds the complete set of objects in the search set. That is, it is
// short-circuited as soon as the first complete outcome can be determined, but no earlier.
func union(
	ctx context.Context,
	checkContext checkRequestContext,
	operands []IndividualDispatch,
	reducer checkReducer[IndividualDispatch],
	maxBatchesAtOnce int,
) DispatchCheckResponseOrError {
	if len(operands) == 0 {
		return emptyDispatchCheckResponse()
	}

	if len(operands) == 1 {
		// special case which avoids the overhead of the channel synchronization
		return reducer(ctx, operands[0])
	}

	respChan := make(chan DispatchCheckResponseOrError, len(operands))

	cancelCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	limiter := make(chan struct{}, maxBatchesAtOnce)
	go func() {
		for _, operand := range operands {
			operand := operand

			limiter <- struct{}{}
			go func() {
				respChan <- reducer(cancelCtx, operand)
				<-limiter
			}()
		}
	}()

	resolvedCheckSet := NewResolvedCheckSet()
	resolutionMetadata := emptyDispatchCheckResolutionMetadata()

	var err error
	for range operands {
		select {
		case respOrErr := <-respChan:
			respMetadata := respOrErr.DispatchCheckResponse.GetResolutionMetadata()
			resolutionMetadata = mergeResolutionMetadata(resolutionMetadata, respMetadata)

			if respOrErr.Err != nil {
				err = respOrErr.Err
				continue
			}

			resolvedCheckSet.Union(respOrErr.DispatchCheckResponse.GetResultsForeachObjectId())

			// if the current resolution path only requires a single object id in the set to have a permittable decision,
			// then return promptly and avoid resolving all outcomes for every object in the set.
			if resolvedCheckSet.HasAnyPermittedDecision() && checkContext.resolutionBehavior == dispatchv1.DispatchCheckRequest_RESOLUTION_BEHAVIOR_RESOLVE_ANY {
				return dispatchCheckResponseFromResolvedCheckSet(resolvedCheckSet, resolutionMetadata)
			}
		case <-cancelCtx.Done():
			return DispatchCheckResponseOrError{Err: cancelCtx.Err()}
		}
	}

	if err != nil {
		return DispatchCheckResponseOrError{Err: err}
	}

	return dispatchCheckResponseFromResolvedCheckSet(resolvedCheckSet, resolutionMetadata)
}

type DispatchCheckResponseOrError struct {
	*dispatchv1.DispatchCheckResponse
	Err error
}

// normalizeDispatchCheckResponse is a helper to fill in empty/nil values in a DispatchCheckResponse
// with normalized ones.
//
// Maintainers modifying code within this package are encouraged to use this function prior to returning a
// response to ensure that all fields of the response structs adhere to the same field invariants calling code
// would hope for.
func normalizeDispatchCheckResponse(resp *dispatchv1.DispatchCheckResponse) DispatchCheckResponseOrError {
	resolvedCheckSet := resp.GetResultsForeachObjectId()
	resolutionMetadata := resp.GetResolutionMetadata()

	metadata := emptyDispatchCheckResolutionMetadata()
	if resolutionMetadata != nil {
		metadata = resolutionMetadata
	}

	return DispatchCheckResponseOrError{
		DispatchCheckResponse: &dispatchv1.DispatchCheckResponse{
			ResultsForeachObjectId: resolvedCheckSet,
			ResolutionMetadata:     metadata,
		},
		Err: nil,
	}
}

func emptyDispatchCheckResolutionMetadata() *dispatchv1.DispatchCheckResolutionMetadata {
	return &dispatchv1.DispatchCheckResolutionMetadata{}
}

func emptyDispatchCheckResponse() DispatchCheckResponseOrError {
	return emptyDispatchCheckResponseWithMetadata(emptyDispatchCheckResolutionMetadata())
}

func emptyDispatchCheckResponseWithMetadata(checkResolutionMetadata *dispatchv1.DispatchCheckResolutionMetadata) DispatchCheckResponseOrError {
	return normalizeDispatchCheckResponse(&dispatchv1.DispatchCheckResponse{
		ResultsForeachObjectId: map[string]*dispatchv1.DispatchCheckResult{},
		ResolutionMetadata:     checkResolutionMetadata,
	})
}

func dispatchCheckResponseFromResolvedCheckSet(
	resolvedCheckSet *ResolvedCheckSet,
	resolutionMetadata *dispatchv1.DispatchCheckResolutionMetadata,
) DispatchCheckResponseOrError {
	return normalizeDispatchCheckResponse(&dispatchv1.DispatchCheckResponse{
		ResultsForeachObjectId: resolvedCheckSet.AsMap(),
		ResolutionMetadata:     resolutionMetadata,
	})
}

// mergeResolutionMetadata merges the left and right DispatchCheckResolutionMetadata
// by taking the aggregate sum of the DatastoreQueryCount and DispatchCount metrics,
// the maximum of the resolution Depth, and the union of the CycleDetected signal.
func mergeResolutionMetadata(
	left *dispatchv1.DispatchCheckResolutionMetadata,
	right *dispatchv1.DispatchCheckResolutionMetadata,
) *dispatchv1.DispatchCheckResolutionMetadata {
	return &dispatchv1.DispatchCheckResolutionMetadata{
		Depth:               max(left.GetDepth(), right.GetDepth()),
		DatastoreQueryCount: left.GetDatastoreQueryCount() + right.GetDatastoreQueryCount(),
		DispatchCount:       left.GetDispatchCount() + right.GetDispatchCount(),
		CycleDetected:       left.GetCycleDetected() || right.GetCycleDetected(),
	}
}
