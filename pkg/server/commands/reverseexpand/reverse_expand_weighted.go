package reverseexpand

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

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

type reverseExpandParams struct {
	ctx                                    context.Context
	req                                    *ReverseExpandRequest
	resultChan                             chan<- *ReverseExpandResult
	intersectionOrExclusionInPreviousEdges bool
	resolutionMetadata                     *ResolutionMetadata
}

func newReverseExpandWeightedParams(
	ctx context.Context,
	req *ReverseExpandRequest,
	resultChan chan<- *ReverseExpandResult,
	intersectionOrExclusionInPreviousEdges bool,
	resolutionMetadata *ResolutionMetadata,
) *reverseExpandParams {
	return &reverseExpandParams{
		ctx:                                    ctx,
		req:                                    req,
		resultChan:                             resultChan,
		intersectionOrExclusionInPreviousEdges: intersectionOrExclusionInPreviousEdges,
		resolutionMetadata:                     resolutionMetadata,
	}
}

type copyOptions func(input *reverseExpandParams)

func (in *reverseExpandParams) copy(opts ...copyOptions) *reverseExpandParams {
	result := &reverseExpandParams{
		ctx:                                    in.ctx,
		req:                                    in.req.copy(),
		resultChan:                             in.resultChan,
		intersectionOrExclusionInPreviousEdges: in.intersectionOrExclusionInPreviousEdges,
		resolutionMetadata:                     in.resolutionMetadata,
	}
	for _, opt := range opts {
		opt(result)
	}
	return result
}

func (in *reverseExpandParams) copyWithCtx(ctx context.Context) *reverseExpandParams {
	return in.copy(withCtx(ctx))
}

func withEdge(edge *weightedGraph.WeightedAuthorizationModelEdge) copyOptions {
	return func(input *reverseExpandParams) {
		input.req.weightedEdge = edge
	}
}

func withEdgeTypeRel(weightedEdgeTypeRel string) copyOptions {
	return func(input *reverseExpandParams) {
		input.req.weightedEdgeTypeRel = weightedEdgeTypeRel
	}
}

func withCtx(ctx context.Context) copyOptions {
	return func(input *reverseExpandParams) {
		input.ctx = ctx
	}
}

func withUser(user IsUserRef) copyOptions {
	return func(in *reverseExpandParams) {
		in.req = in.req.copyWithUser(user)
	}
}

// create a query for expand -> go down the tree.
func (c *ReverseExpandQuery) expand(p *reverseExpandParams) error {
	return c.dispatch(
		p.ctx,
		p.req,
		p.resultChan,
		p.intersectionOrExclusionInPreviousEdges,
		p.resolutionMetadata,
	)
}

// create a query for reverse expand -> go up three

func (c *ReverseExpandQuery) loopOverWeightedEdges(
	p *reverseExpandParams,
	edges []*weightedGraph.WeightedAuthorizationModelEdge,
	sourceUserObj string,
) error {
	pool := concurrency.NewPool(p.ctx, int(c.resolveNodeBreadthLimit))

	var errs error

	for _, edge := range edges {
		params := p.copy(
			withEdge(edge),
			withEdgeTypeRel(edge.GetTo().GetUniqueLabel()),
		)
		switch edge.GetEdgeType() {
		case weightedGraph.DirectEdge:
			pool.Go(func(ctx context.Context) error {
				return c.reverseExpandDirectWeighted(params)
			})
		case weightedGraph.ComputedEdge:
			c.logger.Debug("LOWE: weightedGraph.ComputedEdge -> dispatch",
				zap.String("from", edge.GetFrom().GetUniqueLabel()),
				zap.String("to", edge.GetTo().GetUniqueLabel()),
			)
			toLabel := edge.GetTo().GetUniqueLabel()

			// turn "document#viewer" into "viewer"
			rel := tuple.GetRelation(toLabel)

			params = params.copy(
				withUser(&UserRefObjectRelation{
					ObjectRelation: &openfgav1.ObjectRelation{
						Object:   sourceUserObj,
						Relation: rel,
					},
				}),
			)

			err := c.expand(params)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		case weightedGraph.TTUEdge:
			c.logger.Debug("LOWE: weightedGraph.TTUEdge -> reverseExpandTupleToUsersetWeighted", zap.Any("user", p.req.User))
			pool.Go(func(ctx context.Context) error {
				return c.reverseExpandTupleToUsersetWeighted(params)
			})
			// -> go down the tree
		case weightedGraph.RewriteEdge:
			c.logger.Debug("LOWE: weightedGraph.RewriteEdge -> dispatch", zap.String("to", edge.GetTo().GetUniqueLabel()))
			err := c.expand(params)
			if err != nil {
				errs = errors.Join(errs, err)
				return errs
			}
		default:
			panic("unsupported edge type")
		}
	}

	return errors.Join(errs, pool.Wait())
}

func (c *ReverseExpandQuery) reverseExpandDirectWeighted(p *reverseExpandParams) error {
	ctx, span := tracer.Start(p.ctx, "reverseExpandDirect", trace.WithAttributes(
		// attribute.String("edge", req.edge.String()),
		attribute.String("source.user", p.req.User.String()),
	))
	var err error
	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
		}
		span.End()
	}()

	p.ctx = ctx

	// Do we want to separate buildQueryFilters more, and feed it in below?
	err = c.readTuplesAndExecuteWeighted(p)
	return err
}

func (c *ReverseExpandQuery) reverseExpandTupleToUsersetWeighted(p *reverseExpandParams) error {
	ctx, span := tracer.Start(p.ctx, "reverseExpandTupleToUsersetWeighted", trace.WithAttributes(
		attribute.String("edge.from", p.req.weightedEdge.GetFrom().GetUniqueLabel()),
		attribute.String("edge.to", p.req.weightedEdge.GetFrom().GetUniqueLabel()),
		attribute.String("source.user", p.req.User.String()),
	))
	var err error
	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
		}
		span.End()
	}()

	p.ctx = ctx

	err = c.readTuplesAndExecuteWeighted(p)
	return err
}

func (c *ReverseExpandQuery) readTuplesAndExecuteWeighted(p *reverseExpandParams) error {
	ctx := p.ctx
	if ctx.Err() != nil {
		return ctx.Err()
	}

	ctx, span := tracer.Start(ctx, "readTuplesAndExecuteWeighted")
	defer span.End()

	req := p.req
	userFilter, relationFilter := c.buildQueryFiltersWeighted(req)

	// find all tuples of the form req.edge.TargetReference.Type:...#relationFilter@userFilter
	iter, err := c.datastore.ReadStartingWithUser(ctx, req.StoreID, storage.ReadStartingWithUserFilter{
		ObjectType: getTypeFromLabel(req.weightedEdge.GetFrom().GetLabel()), // e.g. directs-employee
		Relation:   relationFilter,                                          // other-rel
		UserFilter: userFilter,                                              // .Object = employee#alg_combined_1
	}, storage.ReadStartingWithUserOptions{
		Consistency: storage.ConsistencyOptions{
			Preference: req.Consistency,
		},
	})
	if err != nil {
		return err
	}

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
			err := c.trySendCandidate(ctx, intersectionOrExclusionInPreviousEdges, foundObject, resultChan)
			errs = errors.Join(errs, err)

			continue

		case weightedGraph.TTUEdge:
			newRelation = req.weightedEdge.GetTo().GetLabel() // TODO: future branch
		default:
			panic("unsupported edge type")
		}

		params := p.copy(withUser(&UserRefObjectRelation{
			ObjectRelation: &openfgav1.ObjectRelation{
				Object:   foundObject,
				Relation: newRelation,
			},
			Condition: tk.GetCondition(),
		}))

		// Do we still need this additional dispatch?
		pool.Go(func(ctx context.Context) error {
			return c.expand(params)
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
	req *ReverseExpandRequest,
) ([]*openfgav1.ObjectRelation, string) {
	var userFilter []*openfgav1.ObjectRelation
	var relationFilter string

	// Should this actually be looking at the node we're heading towards?
	switch req.weightedEdge.GetEdgeType() {
	case weightedGraph.DirectEdge:
		// the .From() for a direct edge will have a type#rel e.g. directs-employee#other_rel
		fromLabel := req.weightedEdge.GetFrom().GetLabel()
		relationFilter = tuple.GetRelation(fromLabel) // directs-employee#other_rel -> other_rel

		// TODO : special handling if the TO node is a userset
		toNode := req.weightedEdge.GetTo()

		// e.g. 'user:*'
		if toNode.GetNodeType() == weightedGraph.SpecificTypeWildcard {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: toNode.GetLabel(), // e.g. "employee:*"
			})
		}

		// e.g. 'user:bob'
		if val, ok := req.User.(*UserRefObject); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: tuple.BuildObject(val.Object.GetType(), val.Object.GetId()),
			})
		}

		// e.g. 'group:eng#member'
		// TODO : so is it if the TO node is direct to a userset?
		// which would be a DirectEdge TO node with type weightedGraph.SpecificTypeAndRelation
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			if toNode.GetNodeType() == weightedGraph.SpecificTypeAndRelation {
				userFilter = append(userFilter, val.ObjectRelation)
			} else if toNode.GetNodeType() == weightedGraph.SpecificType {
				userFilter = append(userFilter, &openfgav1.ObjectRelation{
					Object: val.ObjectRelation.GetObject(),
				})
			}
		}
	case weightedGraph.TTUEdge:
		relationFilter = req.edge.TuplesetRelation // This needs to be updated
		// a TTU edge can only have a userset as a source node
		// e.g. 'group:eng#member'
		if val, ok := req.User.(*UserRefObjectRelation); ok {
			userFilter = append(userFilter, &openfgav1.ObjectRelation{
				Object: val.ObjectRelation.GetObject(),
			})
		} else {
			panic("unexpected source for reverse expansion of tuple to userset")
		}
	default:
		panic("unsupported edge type")
	}

	return userFilter, relationFilter
}

// expects a "type#rel".
func getTypeFromLabel(label string) string {
	userObject, _ := tuple.SplitObjectRelation(label)
	return userObject
}
