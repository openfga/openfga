package check

import (
	"context"
	"database/sql"
	"errors"
	"sort"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/tuple"
)

var (
	ErrSQLUnsupported = errors.New("datastore does not support SQL algorithms")
)

var _ GroupStrategy = &SQLStrategy{}

type SQLStrategy struct {
	datastore storage.RelationshipTupleReader
	model     *modelgraph.AuthorizationModelGraph
}

func NewSQL(model *modelgraph.AuthorizationModelGraph, datastore storage.RelationshipTupleReader) *SQLStrategy {
	return &SQLStrategy{
		model:     model,
		datastore: datastore,
	}
}

func (s *SQLStrategy) Union(ctx context.Context, req *Request, edge *GroupEdge) (*Response, error) {
	return nil, nil
}

func (s *SQLStrategy) Intersection(ctx context.Context, req *Request, edge *GroupEdge) (*Response, error) {
	return nil, nil
}

func (s *SQLStrategy) Exclusion(ctx context.Context, req *Request, edge *GroupEdge) (*Response, error) {
	return nil, nil
}

// branchOutcome is what we know about a branch while folding a boolean subtree:
// branchTrue / branchFalse are definite, branchNeedsQuery means the answer cannot be
// determined without a database read.
type branchOutcome uint8

const (
	branchNeedsQuery branchOutcome = iota
	branchTrue
	branchFalse
)

// leaf is a weight-1 terminal (direct assignment) reached while walking the subtree: a
// stored `relation` value, whether it matches the public wildcard subject `type:*` rather
// than the exact request subject, and the set of condition names the edge admits.
type leaf struct {
	relation   string
	wildcard   bool
	conditions []string
}

// gatheredRow is one row of the conditioned gather query, decoded into the fields needed
// to match it back to a leaf and to run the tuple's condition.
type gatheredRow struct {
	relation   string
	subjectID  string
	subjectRel string
	condName   string
	condCtx    *structpb.Struct
}

// weight1 answers whether the request's user has the requested relation to the object,
// evaluating the whole group of weight-1 edges (combined by operation) in a single SQL
// round-trip. Without conditions, it issues an existence query (SELECT 1 ... HAVING <tree>);
// with conditions it gathers candidate tuples and evaluates their conditions in-app.
func (s *SQLStrategy) weight1(ctx context.Context, req *Request, builder adapter.Builder, edges []*graph.WeightedAuthorizationModelEdge, operation string) (*Response, error) {
	w := &walker{
		s:         s,
		req:       req,
		builder:   builder,
		table:     builder.Tuple("t"),
		userType:  req.GetUserType(),
		wildcard:  req.IsTypedWildcard(),
		relations: map[string]struct{}{},
	}

	// Decompose the request subject "type:id[#relation]" (or "type:*") into its parts.
	w.subjType, w.subjID, w.subjRel = tuple.ToUserParts(req.GetTupleKey().GetUser())

	obj := req.GetTupleKey().GetObject()
	w.objectType, w.objectID = tuple.SplitObject(obj)

	// Contextual tuples can satisfy (or, via exclusion, deny) the whole subtree without a
	// database read; evaluate them first and short-circuit if the answer is already determined.
	outcome, err := w.evalTree(edges, operation, w.evalContextLeaf)
	if err != nil {
		return nil, err
	}
	switch outcome {
	case branchTrue:
		return &Response{Allowed: true}, nil
	case branchFalse:
		return &Response{Allowed: false}, nil
	default: // branchNeedsQuery: fall through to the database query below.
	}

	if w.conditioned {
		return w.evalConditioned(ctx, edges, operation)
	}
	return w.evalUnconditioned(ctx, edges, operation)
}

// walker carries the per-request state shared by the subtree traversals.
type walker struct {
	s       *SQLStrategy
	req     *Request
	builder adapter.Builder
	table   adapter.Tuple

	userType string
	wildcard bool

	subjType string
	subjID   string
	subjRel  string

	objectType string
	objectID   string

	// relations accumulates the distinct stored `relation` values referenced by unpruned
	// leaves, forming the WHERE `relation IN (...)` filter.
	relations map[string]struct{}
	// conditioned is set when any reachable leaf admits a named condition, selecting the
	// gather path over the existence path.
	conditioned     bool
	hasWildcardLeaf bool
}

// leafFrom derives a leaf descriptor from a terminal direct edge and records its relation
// and condition/wildcard properties on the walker.
func (w *walker) leafFrom(edge *graph.WeightedAuthorizationModelEdge) leaf {
	l := leaf{
		relation:   tuple.GetRelation(edge.GetRelationDefinition()),
		wildcard:   edge.GetTo().GetNodeType() == graph.SpecificTypeWildcard,
		conditions: edge.GetConditions(),
	}
	w.relations[l.relation] = struct{}{}
	if l.wildcard {
		w.hasWildcardLeaf = true
	}
	for _, c := range l.conditions {
		if c != graph.NoCond {
			w.conditioned = true
			break
		}
	}
	return l
}

// pruned reports whether an edge has no path to the request's user type, mirroring
// FlattenNode: such an edge contributes nothing (an unsatisfiable branch).
func (w *walker) pruned(edge *graph.WeightedAuthorizationModelEdge) bool {
	if _, ok := w.s.model.GetEdgeWeight(edge, w.userType); !ok {
		return true
	}
	if w.wildcard && !contains(edge.GetWildcards(), w.userType) {
		return true
	}
	return false
}

// evalTree reduces the subtree formed by edges under operation to a branchOutcome, delegating
// each leaf to evalLeaf. It is used both for the contextual-tuple short-circuit and for the
// final combination of gathered (conditioned) results.
func (w *walker) evalTree(edges []*graph.WeightedAuthorizationModelEdge, operation string, evalLeaf func(leaf) (branchOutcome, error)) (branchOutcome, error) {
	switch operation {
	case graph.UnionOperator:
		result := branchFalse
		for _, edge := range edges {
			outcome, err := w.evalEdge(edge, evalLeaf)
			if err != nil {
				return 0, err
			}
			switch outcome {
			case branchTrue:
				return branchTrue, nil
			case branchNeedsQuery:
				result = branchNeedsQuery
			default: // branchFalse contributes nothing to a union.
			}
		}
		return result, nil
	case graph.IntersectionOperator:
		result := branchTrue
		for _, edge := range edges {
			outcome, err := w.evalEdge(edge, evalLeaf)
			if err != nil {
				return 0, err
			}
			switch outcome {
			case branchFalse:
				return branchFalse, nil
			case branchNeedsQuery:
				result = branchNeedsQuery
			default: // branchTrue contributes nothing to an intersection.
			}
		}
		return result, nil
	case graph.ExclusionOperator:
		if w.wildcard {
			return 0, ErrWildcardInvalidRequest
		}
		if len(edges) != 2 {
			return 0, ErrPanicRequest
		}
		base, err := w.evalEdge(edges[0], evalLeaf)
		if err != nil {
			return 0, err
		}
		if base == branchFalse {
			return branchFalse, nil
		}
		subtract, err := w.evalEdge(edges[1], evalLeaf)
		if err != nil {
			return 0, err
		}
		if subtract == branchTrue {
			return branchFalse, nil
		}
		// base AND NOT subtract: definite only when both are definite.
		if base == branchTrue && subtract == branchFalse {
			return branchTrue, nil
		}
		return branchNeedsQuery, nil
	default:
		return 0, ErrPanicRequest
	}
}

// evalEdge evaluates a single edge: a pruned branch is false, a terminal is evaluated as a
// leaf, and a rewrite/computed/logical edge recurses into its target node.
func (w *walker) evalEdge(edge *graph.WeightedAuthorizationModelEdge, evalLeaf func(leaf) (branchOutcome, error)) (branchOutcome, error) {
	if w.pruned(edge) {
		return branchFalse, nil
	}
	switch edge.GetEdgeType() {
	case graph.DirectEdge:
		// A direct edge always terminates at a type node, which is a weight-1 leaf.
		return evalLeaf(w.leafFrom(edge))
	case graph.ComputedEdge, graph.DirectLogicalEdge, graph.TTULogicalEdge, graph.RewriteEdge:
		return w.evalNode(edge.GetTo(), evalLeaf)
	default:
		return 0, ErrPanicRequest
	}
}

// evalNode evaluates all edges out of a node. An operator node combines by its label; any
// other node (a bare relation) combines its definition branches by union.
func (w *walker) evalNode(node *graph.WeightedAuthorizationModelNode, evalLeaf func(leaf) (branchOutcome, error)) (branchOutcome, error) {
	edges, ok := w.s.model.GetEdgesFromNode(node)
	if !ok {
		return 0, ErrPanicRequest
	}
	operation := graph.UnionOperator
	if node.GetNodeType() == graph.OperatorNode {
		operation = node.GetLabel()
	}
	return w.evalTree(edges, operation, evalLeaf)
}

// evalContextLeaf resolves a leaf against the request's contextual tuples only: branchTrue if a
// contextual tuple satisfies it (its condition passing), otherwise branchNeedsQuery — absence
// from the contextual set does not prove absence in the database.
func (w *walker) evalContextLeaf(l leaf) (branchOutcome, error) {
	var candidates []*openfgav1.TupleKey
	if l.wildcard {
		cts, ok := w.req.GetContextualTuplesByObjectID(w.req.GetTupleKey().GetObject(), l.relation, w.userType)
		if ok {
			for _, ct := range cts {
				if tuple.IsTypedWildcard(ct.GetUser()) {
					candidates = append(candidates, ct)
				}
			}
		}
	} else {
		cts, ok := w.req.GetContextualTuplesByUserID(w.req.GetTupleKey().GetUser(), l.relation, w.objectType)
		if ok {
			for _, ct := range cts {
				if ct.GetObject() == w.req.GetTupleKey().GetObject() {
					candidates = append(candidates, ct)
				}
			}
		}
	}
	for _, ct := range candidates {
		// TODO: what if there are no conditions
		ok, err := evaluateCondition(context.Background(), w.s.model, l.conditions, ct, w.req.GetContext())
		if err != nil {
			return 0, err
		}
		if ok {
			return branchTrue, nil
		}
	}
	return branchNeedsQuery, nil
}

// evalUnconditioned answers the no-condition case with a single existence query: SELECT 1 over
// the referenced relations, grouped by object, with the boolean subtree rendered into HAVING.
func (w *walker) evalUnconditioned(ctx context.Context, edges []*graph.WeightedAuthorizationModelEdge, operation string) (*Response, error) {
	res, err := w.render(edges, operation)
	if err != nil {
		return nil, err
	}
	switch res.state {
	case branchTrue:
		return &Response{Allowed: true}, nil
	case branchFalse:
		return &Response{Allowed: false}, nil
	default: // branchNeedsQuery: fall through to the existence query below.
	}

	// Only unconditioned tuples may satisfy a leaf on the existence path; a conditioned model
	// would have routed to the gather path, so this narrowing is uniform across every leaf and
	// lives here in the shared WHERE rather than in each COUNT(CASE).
	where := append(w.whereShared(),
		w.table.ObjectRelation().In(w.relationLits()...),
		w.table.Condition().IsNull().Or(w.table.Condition().Eq(w.builder.Lit(""))),
	)
	rows, err := w.builder.Select(w.builder.Lit(1)).
		From(w.table).
		Where(where...).
		GroupBy(w.table.ObjectID()).
		Having(res.pred).
		Limit(1).
		Execute(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	allowed := rows.Next()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return &Response{Allowed: allowed}, nil
}

// evalConditioned answers the conditioned case: it reads the candidate tuples' attribution and
// condition columns, evaluates each row's condition in-app, then evaluates the subtree with
// the per-leaf results.
func (w *walker) evalConditioned(ctx context.Context, edges []*graph.WeightedAuthorizationModelEdge, operation string) (*Response, error) {
	// The relation/wildcard/condition state was already populated by the contextual-tuple
	// pass in weight1, so the WHERE filter can be built directly.
	if len(w.relations) == 0 {
		return &Response{Allowed: false}, nil
	}
	rows, err := w.builder.Select(
		w.table.ObjectRelation(),
		w.table.SubjectID(),
		w.table.SubjectRelation(),
		w.table.Condition(),
		w.table.ConditionContext(),
	).
		From(w.table).
		Where(append(w.whereShared(), w.table.ObjectRelation().In(w.relationLits()...))...).
		Execute(ctx)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var gathered []gatheredRow
	for rows.Next() {
		var (
			relation, subjectID, subjectRel string
			condName                        sql.NullString
			condCtx                         []byte
		)
		if err := rows.Scan(&relation, &subjectID, &subjectRel, &condName, &condCtx); err != nil {
			return nil, err
		}
		g := gatheredRow{relation: relation, subjectID: subjectID, subjectRel: subjectRel, condName: condName.String}
		if len(condCtx) > 0 {
			g.condCtx = &structpb.Struct{}
			if err := proto.Unmarshal(condCtx, g.condCtx); err != nil {
				return nil, err
			}
		}
		gathered = append(gathered, g)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	evalLeaf := func(l leaf) (branchOutcome, error) {
		ok, err := w.leafSatisfied(l, gathered)
		if err != nil {
			return 0, err
		}
		if ok {
			return branchTrue, nil
		}
		return branchFalse, nil
	}
	outcome, err := w.evalTree(edges, operation, evalLeaf)
	if err != nil {
		return nil, err
	}
	return &Response{Allowed: outcome == branchTrue}, nil
}

// leafSatisfied reports whether a contextual tuple or a gathered database row matches the
// leaf and passes the leaf's condition.
func (w *walker) leafSatisfied(l leaf, gathered []gatheredRow) (bool, error) {
	if outcome, err := w.evalContextLeaf(l); err != nil {
		return false, err
	} else if outcome == branchTrue {
		return true, nil
	}

	for _, g := range gathered {
		if g.relation != l.relation {
			continue
		}
		if l.wildcard {
			if g.subjectID != tuple.Wildcard {
				continue
			}
		} else if g.subjectID != w.subjID || g.subjectRel != w.subjRel {
			continue
		}

		user := tuple.BuildObject(w.subjType, g.subjectID)
		if g.subjectRel != "" {
			user = tuple.ToObjectRelationString(user, g.subjectRel)
		}
		tk := &openfgav1.TupleKey{
			Object:    w.req.GetTupleKey().GetObject(),
			Relation:  l.relation,
			User:      user,
			Condition: tuple.NewRelationshipCondition(g.condName, g.condCtx),
		}
		ok, err := evaluateCondition(context.Background(), w.s.model, l.conditions, tk, w.req.GetContext())
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// residual is the outcome of rendering a subtree to SQL: a definite state, or an
// unknown state carrying the predicate that must hold for the branch to be satisfied.
type residual struct {
	state branchOutcome
	pred  adapter.Predicate
}

// render lowers the subtree to a HAVING predicate. Leaves satisfied by contextual tuples
// fold to constant-true and drop out; the rest render to COUNT(CASE ...) Existence checks
// combined by the operators.
func (w *walker) render(edges []*graph.WeightedAuthorizationModelEdge, operation string) (residual, error) {
	switch operation {
	case graph.UnionOperator:
		var preds []adapter.Predicate
		for _, edge := range edges {
			r, err := w.renderEdge(edge)
			if err != nil {
				return residual{}, err
			}
			switch r.state {
			case branchTrue:
				return residual{state: branchTrue}, nil
			case branchNeedsQuery:
				preds = append(preds, r.pred)
			default: // branchFalse contributes nothing to a union.
			}
		}
		return combinePreds(preds, branchFalse, func(a, b adapter.Predicate) adapter.Predicate { return a.Or(b) }), nil
	case graph.IntersectionOperator:
		var preds []adapter.Predicate
		for _, edge := range edges {
			r, err := w.renderEdge(edge)
			if err != nil {
				return residual{}, err
			}
			switch r.state {
			case branchFalse:
				return residual{state: branchFalse}, nil
			case branchNeedsQuery:
				preds = append(preds, r.pred)
			default: // branchTrue contributes nothing to an intersection.
			}
		}
		return combinePreds(preds, branchTrue, func(a, b adapter.Predicate) adapter.Predicate { return a.And(b) }), nil
	case graph.ExclusionOperator:
		if w.wildcard {
			return residual{}, ErrWildcardInvalidRequest
		}
		if len(edges) != 2 {
			return residual{}, ErrPanicRequest
		}
		base, err := w.renderEdge(edges[0])
		if err != nil {
			return residual{}, err
		}
		if base.state == branchFalse {
			return residual{state: branchFalse}, nil
		}
		subtract, err := w.renderEdge(edges[1])
		if err != nil {
			return residual{}, err
		}
		if subtract.state == branchTrue {
			return residual{state: branchFalse}, nil
		}
		if subtract.state == branchFalse {
			return base, nil
		}
		// subtract is unknown; base is true or unknown.
		if base.state == branchTrue {
			return residual{state: branchNeedsQuery, pred: subtract.pred.Not()}, nil
		}
		return residual{state: branchNeedsQuery, pred: base.pred.And(subtract.pred.Not())}, nil
	default:
		return residual{}, ErrPanicRequest
	}
}

func (w *walker) renderEdge(edge *graph.WeightedAuthorizationModelEdge) (residual, error) {
	if w.pruned(edge) {
		return residual{state: branchFalse}, nil
	}
	switch edge.GetEdgeType() {
	case graph.DirectEdge:
		// A direct edge always terminates at a type node, which is a weight-1 leaf.
		return w.renderLeaf(w.leafFrom(edge)), nil
	case graph.ComputedEdge, graph.DirectLogicalEdge, graph.TTULogicalEdge, graph.RewriteEdge:
		return w.renderNode(edge.GetTo())
	default:
		return residual{}, ErrPanicRequest
	}
}

func (w *walker) renderNode(node *graph.WeightedAuthorizationModelNode) (residual, error) {
	edges, ok := w.s.model.GetEdgesFromNode(node)
	if !ok {
		return residual{}, ErrPanicRequest
	}
	operation := graph.UnionOperator
	if node.GetNodeType() == graph.OperatorNode {
		operation = node.GetLabel()
	}
	return w.render(edges, operation)
}

// renderLeaf renders a leaf: constant-true if a contextual tuple already satisfies it,
// otherwise an existence aggregate counting rows for the leaf's relation. The subject and
// unconditioned-tuple narrowing is uniform across every leaf on the existence path, so it
// lives once in the shared WHERE (see evalUnconditioned) rather than in each count's FILTER
// clause.
func (w *walker) renderLeaf(l leaf) residual {
	if outcome, _ := w.evalContextLeaf(l); outcome == branchTrue {
		return residual{state: branchTrue}
	}
	match := w.table.ObjectRelation().Eq(w.builder.Lit(l.relation))
	count := w.builder.Aggregate(adapter.AggCount, w.builder.Lit(1)).Filter(match)
	return residual{state: branchNeedsQuery, pred: count.Gt(w.builder.Lit(0))}
}

// whereShared returns the predicates common to every weight-1 query: the store, the
// object, and a narrowing of the subject to the exact request subject (plus the public
// wildcard when a wildcard leaf is present).
func (w *walker) whereShared() []adapter.Predicate {
	preds := []adapter.Predicate{
		w.table.Store().Eq(w.builder.Lit(w.req.GetStoreID())),
		w.table.ObjectType().Eq(w.builder.Lit(w.objectType)),
		w.table.ObjectID().Eq(w.builder.Lit(w.objectID)),
		w.table.SubjectType().Eq(w.builder.Lit(w.subjType)),
		w.table.SubjectRelation().Eq(w.builder.Lit(w.subjRel)),
	}
	if w.hasWildcardLeaf && w.subjID != tuple.Wildcard {
		preds = append(preds, w.table.SubjectID().In(w.builder.Lit(w.subjID), w.builder.Lit(tuple.Wildcard)))
	} else {
		preds = append(preds, w.table.SubjectID().Eq(w.builder.Lit(w.subjID)))
	}
	return preds
}

// relationLits renders the accumulated relation set as a sorted list of literals for the
// WHERE `relation IN (...)` filter (sorted for deterministic SQL).
func (w *walker) relationLits() []adapter.Expression {
	names := make([]string, 0, len(w.relations))
	for r := range w.relations {
		names = append(names, r)
	}
	sort.Strings(names)
	lits := make([]adapter.Expression, len(names))
	for i, r := range names {
		lits[i] = w.builder.Lit(r)
	}
	return lits
}

// combinePreds folds preds with join; an empty list yields the given identity state.
func combinePreds(preds []adapter.Predicate, empty branchOutcome, join func(a, b adapter.Predicate) adapter.Predicate) residual {
	if len(preds) == 0 {
		return residual{state: empty}
	}
	acc := preds[0]
	for _, p := range preds[1:] {
		acc = join(acc, p)
	}
	return residual{state: branchNeedsQuery, pred: acc}
}

func contains(s []string, v string) bool {
	for _, e := range s {
		if e == v {
			return true
		}
	}
	return false
}
