package commands

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/tuple"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/pkg/utils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/validation"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/unit"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const Wildcard = "*"

// A CheckQuery can be used to Check if a User has a Relation to an Object
// CheckQuery instances may be safely shared by multiple go-routines
type CheckQuery struct {
	logger           logger.Logger
	tracer           trace.Tracer
	meter            metric.Meter
	datastore        storage.OpenFGADatastore
	resolveNodeLimit uint32
}

// NewCheckQuery creates a CheckQuery with specified `tupleBackend` and `typeDefinitionReadBackend` to use for storage
func NewCheckQuery(datastore storage.OpenFGADatastore, t trace.Tracer, m metric.Meter, l logger.Logger, resolveNodeLimit uint32) *CheckQuery {
	return &CheckQuery{
		logger:           l,
		tracer:           t,
		meter:            m,
		datastore:        datastore,
		resolveNodeLimit: resolveNodeLimit,
	}
}

// Execute the query in `checkRequest`, returning the response or an error.
func (query *CheckQuery) Execute(ctx context.Context, req *openfgapb.CheckRequest) (*openfgapb.CheckResponse, error) {
	statCheckResolutionDepth, _ := query.meter.AsyncInt64().Gauge(
		"openfga.check.resolution.depth",
		instrument.WithDescription("Number of recursive resolutions needed to execute check requests"),
		instrument.WithUnit(unit.Dimensionless),
	)
	statCheckDBCalls, _ := query.meter.AsyncInt64().Gauge(
		"openfga.check.db.calls",
		instrument.WithDescription("Number of db queries needed to execute check requests"),
		instrument.WithUnit(unit.Dimensionless),
	)

	var resolutionTracer resolutionTracer = &noopResolutionTracer{}
	if req.GetTrace() {
		resolutionTracer = newStringResolutionTracer()
	}

	tk := req.GetTupleKey()
	contextualTuples, err := validateAndPreprocessTuples(tk, req.GetContextualTuples().GetTupleKeys())
	if err != nil {
		return nil, err
	}

	model, err := query.datastore.ReadAuthorizationModel(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		if err == storage.ErrNotFound {
			return nil, serverErrors.AuthorizationModelNotFound(req.GetAuthorizationModelId())
		}

		return nil, serverErrors.HandleError("", err)
	}

	typesys := typesystem.New(model)

	rc := newResolutionContext(req.GetStoreId(), req.GetAuthorizationModelId(), tk, contextualTuples, resolutionTracer, utils.NewResolutionMetadata(), &circuitBreaker{breakerState: false})

	userset, err := query.getTypeDefinitionRelationUsersets(ctx, rc)
	if err != nil {
		utils.LogDBStats(ctx, query.logger, "Check", rc.metadata.GetReadCalls(), 0)
		return nil, err
	}

	if err := query.resolveNode(ctx, rc, userset, typesys); err != nil {
		utils.LogDBStats(ctx, query.logger, "Check", rc.metadata.GetReadCalls(), 0)
		return nil, err
	}

	var resolution string
	r, ok := rc.users.Get(rc.targetUser)
	if ok && r != nil {
		resolution = r.GetResolution()
	}

	utils.LogDBStats(ctx, query.logger, "Check", rc.metadata.GetReadCalls(), 0)
	if statCheckResolutionDepth != nil {
		statCheckResolutionDepth.Observe(ctx, int64(rc.metadata.GetResolve()))
	}
	if statCheckDBCalls != nil {
		statCheckDBCalls.Observe(ctx, int64(rc.metadata.GetReadCalls()))
	}

	return &openfgapb.CheckResponse{
		Allowed:    ok,
		Resolution: resolution,
	}, nil
}

func (query *CheckQuery) getTypeDefinitionRelationUsersets(ctx context.Context, rc *resolutionContext) (*openfgapb.Userset, error) {
	ctx, span := query.tracer.Start(ctx, "getTypeDefinitionRelationUsersets")
	defer span.End()

	userset, err := validation.ValidateTuple(ctx, query.datastore, rc.store, rc.modelID, rc.tk, rc.metadata)
	if err != nil {
		return nil, serverErrors.HandleTupleValidateError(err)
	}
	return userset, nil
}

// resolveNode recursively resolves userset starting from a supplied UserTree node.
func (query *CheckQuery) resolveNode(ctx context.Context, rc *resolutionContext, nsUS *openfgapb.Userset, typesys *typesystem.TypeSystem) error {
	if rc.metadata.AddResolve() >= query.resolveNodeLimit {
		query.logger.Warn("resolution too complex", zap.String("resolution", rc.tracer.GetResolution()))
		return serverErrors.AuthorizationModelResolutionTooComplex
	}
	ctx, span := query.tracer.Start(ctx, "resolveNode")
	defer span.End()
	if rc.shouldShortCircuit() {
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("short-circuit")})
		return nil // short circuit subsequent operations
	}

	switch usType := nsUS.Userset.(type) {
	case nil, *openfgapb.Userset_This:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("this")})
		return query.resolveDirectUserSet(ctx, rc, typesys)
	case *openfgapb.Userset_Union:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("union")})
		return query.resolveUnion(ctx, rc, usType, typesys)
	case *openfgapb.Userset_Intersection:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("intersection")})
		return query.resolveIntersection(ctx, rc, usType, typesys)
	case *openfgapb.Userset_Difference:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("difference")})
		return query.resolveDifference(ctx, rc, usType, typesys)
	case *openfgapb.Userset_ComputedUserset:
		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("computed")})
		return query.resolveComputed(ctx, rc, usType, typesys)
	case *openfgapb.Userset_TupleToUserset:
		tupleset := usType.TupleToUserset.GetTupleset().GetRelation()

		objectType, _ := tuple.SplitObject(rc.tk.Object)
		relation, ok := typesys.GetRelation(objectType, tupleset)
		if !ok {
			return serverErrors.RelationNotFound(tupleset, objectType, tuple.NewTupleKey(rc.tk.Object, tupleset, rc.tk.User))
		}

		tuplesetRewrite := relation.GetRewrite().GetUserset()
		if reflect.TypeOf(tuplesetRewrite) != reflect.TypeOf(&openfgapb.Userset_This{}) {
			query.logger.Warn(
				fmt.Sprintf("unexpected rewrite on tupleset relation '%s#%s'", objectType, tupleset),
				zap.String("store_id", rc.store),
				zap.String("authorization_model_id", rc.modelID),
				zap.String("object_type", objectType),
				zap.String("relation", tupleset),
			)

			return serverErrors.InvalidAuthorizationModelInput(
				errors.Errorf("unexpected rewrite on relation '%s#%s'", objectType, tupleset),
			)
		}

		span.SetAttributes(attribute.KeyValue{Key: "operation", Value: attribute.StringValue("tuple-to-userset")})
		return query.resolveTupleToUserset(ctx, rc, usType, typesys)
	default:
		return serverErrors.UnsupportedUserSet
	}
}

func (query *CheckQuery) resolveComputed(
	ctx context.Context,
	rc *resolutionContext,
	nodes *openfgapb.Userset_ComputedUserset,
	typesys *typesystem.TypeSystem,
) error {
	computedTK := &openfgapb.TupleKey{Object: rc.tk.GetObject(), Relation: nodes.ComputedUserset.GetRelation(), User: rc.tk.GetUser()}
	tracer := rc.tracer.AppendComputed().AppendString(tupleUtils.ToObjectRelationString(computedTK.GetObject(), computedTK.GetRelation()))
	nestedRC := rc.fork(computedTK, tracer, false)
	userset, err := query.getTypeDefinitionRelationUsersets(ctx, nestedRC)
	if err != nil {
		return err
	}
	return query.resolveNode(ctx, nestedRC, userset, typesys)
}

// resolveDirectUserSet attempts to find individual user concurrently by resolving the usersets. If the user is found
// in the direct user search or in any of the usersets, the peer goroutines will be short-circuited.
func (query *CheckQuery) resolveDirectUserSet(
	ctx context.Context,
	rc *resolutionContext,
	typesys *typesystem.TypeSystem,
) error {
	done := make(chan struct{})
	defer close(done)

	var wg sync.WaitGroup
	c := make(chan *chanResolveResult)

	wg.Add(1)
	go func(c chan<- *chanResolveResult) {
		defer wg.Done()

		found := false
		tk, err := rc.readUserTuple(ctx, query.datastore)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				err = nil
			}
		}

		if tk != nil && err == nil {
			rc.users.Add(rc.tracer.AppendDirect(), tk.GetUser())
			found = true
		}
		select {
		case c <- &chanResolveResult{err: err, found: found}:
		case <-done:
		}
	}(c)

	iter, err := rc.readUsersetTuples(ctx, query.datastore)
	if err != nil {
		return serverErrors.HandleError("", err)
	}

	for {
		usersetTuple, err := iter.Next()
		if err != nil {
			if err == storage.ErrIteratorDone {
				break
			}
			return serverErrors.HandleError("", err)
		}

		// If a single star is available, then assume user exists and break.
		if usersetTuple.GetUser() == "*" {
			rc.users.Add(rc.tracer.AppendDirect(), rc.targetUser)
			break
		}

		// Avoid launching more goroutines by checking if the user has been found in another goroutine.
		if rc.shouldShortCircuit() {
			break
		}

		userset := usersetTuple.GetUser()
		object, relation := tupleUtils.SplitObjectRelation(userset)
		tracer := rc.tracer.AppendDirect().AppendString(userset)
		tupleKey := &openfgapb.TupleKey{
			Object:   object,
			Relation: relation,
			User:     rc.tk.GetUser(),
		}
		nestedRC := rc.fork(tupleKey, tracer, false)

		wg.Add(1)
		go func(c chan<- *chanResolveResult) {
			defer wg.Done()

			userset, err := query.getTypeDefinitionRelationUsersets(ctx, nestedRC)
			if err == nil {
				err = query.resolveNode(ctx, nestedRC, userset, typesys)
			}

			select {
			case c <- &chanResolveResult{err: err, found: nestedRC.userFound()}:
			case <-done:
			}
		}(c)
	}

	// If any `break` was triggered, immediately release any possible resources held by the iterator.
	iter.Stop()

	go func(c chan *chanResolveResult) {
		wg.Wait()
		close(c)
	}(c)

	for res := range c {
		if res.found {
			return nil
		}
		if res.err != nil {
			err = res.err
		}
	}

	return err
}

func (query *CheckQuery) resolveUnion(
	ctx context.Context,
	rc *resolutionContext,
	nodes *openfgapb.Userset_Union,
	typesys *typesystem.TypeSystem,
) error {
	var wg sync.WaitGroup
	c := make(chan *chanResolveResult, len(nodes.Union.Child))

	for idx, userset := range nodes.Union.Child {
		if rc.shouldShortCircuit() {
			break
		}

		us := userset
		tracer := rc.tracer.AppendUnion().AppendIndex(idx)
		nestedRC := rc.fork(rc.tk, tracer, true)

		wg.Add(1)
		go func(c chan<- *chanResolveResult) {
			defer wg.Done()
			err := query.resolveNode(ctx, nestedRC, us, typesys)
			c <- &chanResolveResult{err: err, found: nestedRC.userFound()}
		}(c)
	}

	go func(c chan *chanResolveResult) {
		wg.Wait()
		close(c)
	}(c)

	var err error
	for res := range c {
		if res.found {
			return nil
		}
		if res.err != nil {
			err = res.err
		}
	}

	return err
}

func (query *CheckQuery) resolveIntersection(
	ctx context.Context,
	rc *resolutionContext,
	nodes *openfgapb.Userset_Intersection,
	typesys *typesystem.TypeSystem,
) error {
	userSetsPerChild := newUserSets()
	grp, ctx := errgroup.WithContext(ctx)
	breaker := &circuitBreaker{breakerState: false}
	for idx, userset := range nodes.Intersection.Child {
		idx, userset := idx, userset
		tracer := rc.tracer.AppendIndex(idx)
		nestedRC := newResolutionContext(rc.store, rc.modelID, rc.tk, rc.contextualTuples, tracer, rc.metadata, breaker)
		grp.Go(func() error {
			err := query.resolveNode(ctx, nestedRC, userset, typesys)
			if err != nil {
				return err
			}
			if !nestedRC.userFound() {
				// if not found in ANY of them, the shared breaker should open
				breaker.Open()
			}
			userSetsPerChild.Set(idx, nestedRC.users)
			return nil
		})
	}

	if err := grp.Wait(); err != nil {
		return err
	}
	if breaker.IsOpen() {
		return nil // if the breaker opened, at least one group is missing the user
	}
	smallestUSIdx := 0
	usPerNode := userSetsPerChild.AsMap()
	// Finding the smallest of the usersets reduces the lookups when finding the intersections of the children
	// smallestUSIdx is used to store the index the contains the smallest of the usersets
	for idx, us := range usPerNode {
		if idx == 0 {
			continue
		}
		if len(usPerNode[smallestUSIdx].AsSlice()) > len(us.AsSlice()) {
			smallestUSIdx = idx
		}
	}
	// Avoid processing the same one twice
	smallestUS := usPerNode[smallestUSIdx].AsSlice()
	delete(usPerNode, smallestUSIdx)
	for _, user := range smallestUS {
		sit := rc.tracer.CreateIntersectionTracer()
		sit.AppendTrace(user.r)
		missing := false
		for _, set := range usPerNode {
			rt, ok := set.Get(user.u)
			if !ok {
				missing = true
				break
			}
			sit.AppendTrace(rt)
		}
		if !missing {
			rc.users.Add(rc.tracer.AppendIntersection(sit), user.u)
		}
	}

	return nil
}

func (query *CheckQuery) resolveDifference(
	ctx context.Context,
	rc *resolutionContext,
	node *openfgapb.Userset_Difference,
	typesys *typesystem.TypeSystem,
) error {
	sets := []*openfgapb.Userset{node.Difference.GetBase(), node.Difference.GetSubtract()}
	usPerNode := newUserSets()
	grp, ctx := errgroup.WithContext(ctx)
	breaker := &circuitBreaker{breakerState: false}
	for idx, set := range sets {
		idx, set := idx, set
		tracer := rc.tracer.AppendIndex(idx)
		nestedRC := newResolutionContext(rc.store, rc.modelID, rc.tk, rc.contextualTuples, tracer, rc.metadata, breaker)
		grp.Go(func() error {
			err := query.resolveNode(ctx, nestedRC, set, typesys)
			if err != nil {
				return err
			}
			if idx == 0 && !nestedRC.userFound() {
				// if not found in base, no point on resolving subtract completely
				breaker.Open()
			}
			usPerNode.Set(idx, nestedRC.users)
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	base, _ := usPerNode.Get(0)
	difference, _ := usPerNode.Get(1)
	base.DeleteFrom(difference)
	rc.users.AddFrom(base)
	return nil
}

func (query *CheckQuery) resolveTupleToUserset(
	ctx context.Context,
	rc *resolutionContext,
	node *openfgapb.Userset_TupleToUserset,
	typesys *typesystem.TypeSystem,
) error {
	relation := node.TupleToUserset.GetTupleset().GetRelation()
	if relation == "" {
		relation = rc.tk.GetRelation()
	}

	findTK := tupleUtils.NewTupleKey(rc.tk.GetObject(), relation, "")

	tracer := rc.tracer.AppendTupleToUserset().AppendString(tupleUtils.ToObjectRelationString(findTK.GetObject(), relation))
	iter, err := rc.read(ctx, query.datastore, findTK)
	if err != nil {
		return serverErrors.HandleError("", err)
	}

	done := make(chan struct{})
	defer close(done)

	var wg sync.WaitGroup
	c := make(chan *chanResolveResult)

	for {
		tuple, err := iter.Next()
		if err != nil {
			if err == storage.ErrIteratorDone {
				break
			}
			return serverErrors.HandleError("", err)
		}

		if rc.shouldShortCircuit() {
			break // the user was resolved already, avoid launching extra lookups
		}

		userObj, userRel := tupleUtils.SplitObjectRelation(tuple.GetUser())

		if userObj == Wildcard {
			objectType, _ := tupleUtils.SplitObject(rc.tk.GetObject())

			query.logger.WarnWithContext(
				ctx,
				fmt.Sprintf("unexpected wildcard evaluated on tupleset relation '%s'", relation),
				zap.String("store_id", rc.store),
				zap.String("authorization_model_id", rc.modelID),
				zap.String("object_type", objectType),
			)

			return serverErrors.InvalidTuple(
				fmt.Sprintf("unexpected wildcard evaluated on relation '%s#%s'", objectType, relation),
				tupleUtils.NewTupleKey(rc.tk.GetObject(), relation, Wildcard),
			)
		}

		if !tupleUtils.IsValidObject(userObj) {
			continue // TupleToUserset tuplesets should be of the form 'objectType:id' or 'objectType:id#relation' but are not guaranteed to be because it is neither a user or userset
		}

		usersetRel := node.TupleToUserset.GetComputedUserset().GetRelation()

		// userRel may be empty, and in this case we set it to usersetRel.
		if userRel == "" {
			userRel = usersetRel
		}
		// We only proceed in the case that userRel == usersetRel (=node.TupleToUserset.GetComputedUserset().GetRelation()).
		if userRel != usersetRel {
			continue
		}

		tupleKey := &openfgapb.TupleKey{
			// user from previous lookup
			Object:   userObj,
			Relation: userRel,
			// original tk user
			User: rc.tk.GetUser(),
		}
		tracer := tracer.AppendString(tupleUtils.ToObjectRelationString(userObj, userRel))
		nestedRC := rc.fork(tupleKey, tracer, false)

		wg.Add(1)
		go func(c chan<- *chanResolveResult) {
			defer wg.Done()

			userset, err := query.getTypeDefinitionRelationUsersets(ctx, nestedRC)
			if err == nil {
				err = query.resolveNode(ctx, nestedRC, userset, typesys)
			}

			select {
			case c <- &chanResolveResult{err: err, found: nestedRC.userFound()}:
			case <-done:
			}
		}(c)
	}

	// If any `break` was triggered, immediately release any possible resources held by the iterator.
	iter.Stop()

	go func(c chan *chanResolveResult) {
		wg.Wait()
		close(c)
	}(c)

	for res := range c {
		if res.found {
			return nil
		}
		if res.err != nil {
			err = res.err
		}
	}

	return err
}
