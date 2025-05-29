package reverseexpand

import (
	"context"
	"errors"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"slices"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	weightedGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/condition"
	"github.com/openfga/openfga/internal/condition/eval"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/tuple"
)

func (c *ReverseExpandQuery) LoopOverWeightedEdges(
	ctx context.Context,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	needsCheck bool,
	req *ReverseExpandRequest,
	resolutionMetadata *ResolutionMetadata,
	resultChan chan<- *ReverseExpandResult,
	sourceUserObj string,
) error {
	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	var errs error

	for _, edge := range edges {
		// TODO: i think the getEdgesFromWeightedGraph func handles this for us, shouldn't need this
		// intersectionOrExclusionInPreviousEdges := intersectionOrExclusionInPreviousEdges || innerLoopEdge.TargetReferenceInvolvesIntersectionOrExclusion
		//innerLoopEdge := edge
		r := &ReverseExpandRequest{
			Consistency:      req.Consistency,
			Context:          req.Context,
			ContextualTuples: req.ContextualTuples,
			ObjectType:       req.ObjectType,
			Relation:         req.Relation,
			StoreID:          req.StoreID,
			User:             req.User,

			weightedEdge:        edge,
			weightedEdgeTypeRel: edge.GetTo().GetUniqueLabel(),
			edge:                req.edge,
		}
		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			//fmt.Printf("JUSTIN DIRECT edge from %s to %s\n", edge.GetFrom().GetUniqueLabel(), edge.GetTo().GetUniqueLabel())
			//r.weightedEdge = edge
			pool.Go(func(ctx context.Context) error {
				return c.reverseExpandDirectWeighted(ctx, r, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.ComputedEdge:
			//fmt.Printf("JUSTIN Computed edge from %s to %s\n", edge.GetFrom().GetUniqueLabel(), edge.GetTo().GetUniqueLabel())
			// follow the computed_userset edge, no new goroutine needed since it's not I/O intensive
			to := edge.GetTo().GetUniqueLabel()

			// turn "document#viewer" into "viewer"
			rel := getRelationFromLabel(to)
			r.User = &UserRefObjectRelation{
				ObjectRelation: &openfgav1.ObjectRelation{
					Object:   sourceUserObj,
					Relation: rel,
				},
			}
			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.TTUEdge:
			//fmt.Printf("JUSTIN TTU EDGE")
			pool.Go(func(ctx context.Context) error {
				return c.reverseExpandTupleToUsersetWeighted(ctx, r, resultChan, needsCheck, resolutionMetadata)
			})
		case weightedGraph.RewriteEdge:
			//fmt.Printf("JUSTIN Rewrite edge from %s to %s\n", edge.GetFrom().GetUniqueLabel(), edge.GetTo().GetUniqueLabel())
			err := c.dispatch(ctx, r, resultChan, needsCheck, resolutionMetadata)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
			// Rewrite edge from directs-employee#alg_combined to exclusion:01JVZ4NBP9WMQ67GSMPNBXNGZR
			// this'll need a dispatch with a new key, instead of "type#rel" it'll be "exclusion:01JVWQXTZYP578BTN93PR9JTQK"
			// or intersection
		default:
			fmt.Printf("Unknown edge type %d\n", edge.GetEdgeType())
			panic("unsupported edge type")
		}
	}

	return errors.Join(errs, pool.Wait())
	// Can we lift this to the caller? I don't want to have to pass in a span also this method signature is big
	// if errs != nil {
	//	telemetry.TraceError(span, errs)
	//	return errs
	//}
}

func (c *ReverseExpandQuery) reverseExpandDirectWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandDirect", trace.WithAttributes(
		// attribute.String("edge", req.edge.String()),
		attribute.String("source.user", req.User.String()),
	))
	var err error
	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
		}
		span.End()
	}()

	err = c.readTuplesAndExecuteWeighted(ctx, req, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
	return err
}

func (c *ReverseExpandQuery) reverseExpandTupleToUsersetWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	ctx, span := tracer.Start(ctx, "reverseExpandTupleToUsersetWeighted", trace.WithAttributes(
		attribute.String("edge.from", req.weightedEdge.GetFrom().GetUniqueLabel()),
		attribute.String("edge.to", req.weightedEdge.GetFrom().GetUniqueLabel()),
		attribute.String("source.user", req.User.String()),
	))
	var err error
	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
		}
		span.End()
	}()

	err = c.readTuplesAndExecuteWeighted(ctx, req, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
	return err
}

func (c *ReverseExpandQuery) readTuplesAndExecuteWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	ctx, span := tracer.Start(ctx, "readTuplesAndExecuteWeighted")
	defer span.End()

	var userFilter []*openfgav1.ObjectRelation
	var relationFilter string

	userFilter, relationFilter, err := c.buildQueryFiltersWeighted(ctx, req)
	if err != nil {
		return err
	}

	// find all tuples of the form req.edge.TargetReference.Type:...#relationFilter@userFilter
	iter, err := c.datastore.ReadStartingWithUser(ctx, req.StoreID, storage.ReadStartingWithUserFilter{
		ObjectType: getTypeFromLabel(req.weightedEdge.GetFrom().GetLabel()), // e.g. directs-employee
		//ObjectType: req.edge.TargetReference.GetType(),
		Relation:   relationFilter, // other-rel
		UserFilter: userFilter,     // .Object = employee#alg_combined_1
	}, storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.Consistency,
		},
	})
	if err != nil {
		return err
	}
	fmt.Printf("JUSTIN UserFilter: %s\n", userFilter)
	fmt.Printf("JUSTIN RelationFilter: %s\n", relationFilter)

	// filter out invalid tuples yielded by the database iterator
	filteredIter := storage.NewFilteredTupleKeyIterator(
		storage.NewTupleKeyIteratorFromTupleIterator(iter),
		validation.FilterInvalidTuples(c.typesystem),
	)
	defer filteredIter.Stop()

	pool := concurrency.NewPool(ctx, int(c.resolveNodeBreadthLimit))

	var errs error

LoopOnIterator:
	for {
		tk, err := filteredIter.Next(ctx)
		fmt.Printf("JUSTIN TUPLE KEY: %s\n", tk.String())
		if err != nil {
			if errors.Is(err, storage.ErrIteratorDone) {
				break
			}
			errs = errors.Join(errs, err)
			break LoopOnIterator
		}

		condEvalResult, err := eval.EvaluateTupleCondition(ctx, tk, c.typesystem, req.Context)
		if err != nil {
			errs = errors.Join(errs, err)
			continue
		}

		if !condEvalResult.ConditionMet {
			if len(condEvalResult.MissingParameters) > 0 {
				errs = errors.Join(errs, condition.NewEvaluationError(
					tk.GetCondition().GetName(),
					fmt.Errorf("tuple '%s' is missing context parameters '%v'",
						tuple.TupleKeyToString(tk),
						condEvalResult.MissingParameters),
				))
			}

			continue
		}

		foundObject := tk.GetObject()
		var newRelation string

		switch req.weightedEdge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			newRelation = tk.GetRelation()
		case weightedGraph.TTUEdge:
			newRelation = req.weightedEdge.GetTo().GetLabel() // TODO : validate this?
		default:
			panic("unsupported edge type")
		}

		pool.Go(func(ctx context.Context) error {
			return c.dispatch(ctx, &ReverseExpandRequest{
				StoreID:    req.StoreID,
				ObjectType: req.ObjectType,
				Relation:   req.Relation,
				User: &UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   foundObject,
						Relation: newRelation,
					},
					Condition: tk.GetCondition(),
				},
				ContextualTuples: req.ContextualTuples,
				Context:          req.Context,
				edge:             req.edge,
				Consistency:      req.Consistency,
				// TODO : verify this
				weightedEdge:        req.weightedEdge,
				weightedEdgeTypeRel: req.weightedEdgeTypeRel,
			}, resultChan, intersectionOrExclusionInPreviousEdges, resolutionMetadata)
		})
	}

	errs = errors.Join(errs, pool.Wait())
	if errs != nil {
		telemetry.TraceError(span, errs)
		return errs
	}

	return nil
}

func (c *ReverseExpandQuery) buildQueryFiltersWeighted(
	ctx context.Context,
	req *ReverseExpandRequest,
) ([]*openfgav1.ObjectRelation, string, error) {
	var userFilter []*openfgav1.ObjectRelation
	var relationFilter string
	// type assertion to determine whether this is internal graph or weighted graph?
	// OR do we use a different key, req.edge vs req.weightedEdge?
	// or are there just completely different code paths
	switch req.weightedEdge.GetEdgeType() {
	case weightedGraph.DirectEdge:
		// the .From() for a direct edge will have a type#rel e.g. directs-employee#other_rel
		from := req.weightedEdge.GetFrom().GetLabel()
		relationFilter = getRelationFromLabel(from) // directs-employee#other_rel -> other_rel

		targetUserObjectType := req.weightedEdge.GetTo().GetLabel() // "employee"

		publiclyAssignable := slices.Contains(req.weightedEdge.GetFrom().GetWildcards(), targetUserObjectType)

		if publiclyAssignable {
			// e.g. 'user:*'
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: tuple.TypedPublicWildcard(targetUserObjectType),
			})
		}

		// e.g. 'user:bob'
		if val, ok := req.User.(*UserRefObject); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: tuple.BuildObject(val.Object.GetType(), val.Object.GetId()),
			})
		}

		// e.g. 'group:eng#member'
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			userFilter = append(userFilter, val.ObjectRelation)
		}
	case weightedGraph.TTUEdge:
		relationFilter = req.edge.TuplesetRelation
		// a TTU edge can only have a userset as a source node
		// e.g. 'group:eng#member'
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: val.ObjectRelation.GetObject(),
			})
			fmt.Printf("TTU EDGE GetObject(): %s", val.ObjectRelation.GetObject())
		} else {
			panic("unexpected source for reverse expansion of tuple to userset")
		}
	// TODO: are there any other cases?
	default:
		panic("unsupported edge type")
	}

	return userFilter, relationFilter, nil
}

func hasPathTo(dest string) func(*weightedGraph.WeightedAuthorizationModelEdge) bool {
	return func(edge *weightedGraph.WeightedAuthorizationModelEdge) bool {
		_, ok := edge.GetWeight(dest)
		return ok
	}
}

func cheapestEdgeTo(dst string) func(*weightedGraph.WeightedAuthorizationModelEdge, *weightedGraph.WeightedAuthorizationModelEdge) *weightedGraph.WeightedAuthorizationModelEdge {
	return func(lowest, current *weightedGraph.WeightedAuthorizationModelEdge) *weightedGraph.WeightedAuthorizationModelEdge {
		if lowest == nil {
			return current
		}

		a, ok := lowest.GetWeight(dst)
		if !ok {
			return current
		}

		b, ok := current.GetWeight(dst)
		if !ok {
			return lowest
		}

		if b < a {
			return current
		}
		return lowest
	}
}

func reduce[S ~[]E, E any, A any](s S, initializer A, f func(A, E) A) A {
	i := initializer
	for _, item := range s {
		i = f(i, item)
	}
	return i
}

func filter[S ~[]E, E any](s S, f func(E) bool) []E {
	var filteredItems []E
	for _, item := range s {
		if f(item) {
			filteredItems = append(filteredItems, item)
		}
	}
	return filteredItems
}

// expects a "type#rel".
func getTypeFromLabel(label string) string {
	userObject, _ := tuple.SplitObjectRelation(label)
	return userObject
}

// expects a "type#rel".
func getRelationFromLabel(label string) string {
	_, rel := tuple.SplitObjectRelation(label)
	return rel
}
