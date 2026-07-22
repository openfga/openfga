package check

import (
	"context"
	"database/sql"
	"slices"
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
	return s.weight1(ctx, req, s.datastore.Builder(req.GetConsistency()), edge.edges, graph.UnionOperator)
}

func (s *SQLStrategy) Intersection(ctx context.Context, req *Request, edge *GroupEdge) (*Response, error) {
	return s.weight1(ctx, req, s.datastore.Builder(req.GetConsistency()), edge.edges, graph.IntersectionOperator)
}

func (s *SQLStrategy) Exclusion(ctx context.Context, req *Request, edge *GroupEdge) (*Response, error) {
	return s.weight1(ctx, req, s.datastore.Builder(req.GetConsistency()), edge.edges, graph.ExclusionOperator)
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

// residual is the outcome of folding a subtree: a definite state, or an unknown state
// (branchNeedsQuery) optionally carrying the SQL predicate that must hold for the branch to
// be satisfied. Reducers that cannot express a predicate (contextual and gathered-row
// evaluation) leave pred nil; the render reducer sets it.
type residual struct {
	state branchOutcome
	pred  adapter.Predicate
}

// leafReducer reduces a single weight-1 leaf to a residual. The same subtree is folded with
// three different reducers: contextual-tuple evaluation (short-circuit pass), SQL predicate
// rendering (existence path), and gathered-row matching (conditioned path).
type leafReducer func(ctx context.Context, l leaf) (residual, error)

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

	// Populate the relation set and the conditioned/wildcard flags up front by visiting every
	// leaf, so the WHERE filter is complete regardless of how the folds below short-circuit.
	if err := w.collect(edges); err != nil {
		return nil, err
	}

	// Contextual tuples can satisfy (or, via exclusion, deny) the whole subtree without a
	// database read; evaluate them first and short-circuit if the answer is already determined.
	res, err := w.fold(ctx, edges, operation, w.reduceContextLeaf)
	if err != nil {
		return nil, err
	}
	switch res.state {
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
	// leaves, forming the WHERE `relation IN (...)` filter. Populated by collect.
	relations map[string]struct{}
	// conditioned is set when any reachable leaf admits a named condition, selecting the
	// gather path over the existence path. Populated by collect.
	conditioned bool
	// hasWildcardLeaf is set when any reachable leaf matches the public wildcard subject.
	// Populated by collect.
	hasWildcardLeaf bool
}

// leafFrom derives a leaf descriptor from a terminal direct edge. It is pure: the filter
// state a leaf contributes (relations/conditioned/hasWildcardLeaf) is accumulated separately
// by collect, so this can be called freely from any fold without side effects.
func (w *walker) leafFrom(edge *graph.WeightedAuthorizationModelEdge) leaf {
	return leaf{
		relation:   tuple.GetRelation(edge.GetRelationDefinition()),
		wildcard:   edge.GetTo().GetNodeType() == graph.SpecificTypeWildcard,
		conditions: edge.GetConditions(),
	}
}

// collect walks every unpruned leaf in the subtree — without the short-circuiting that fold
// applies — and records the filter state each leaf contributes. Doing this in a dedicated
// full pass (rather than as a side effect of fold) keeps the relation set complete no matter
// how the later folds short-circuit.
func (w *walker) collect(edges []*graph.WeightedAuthorizationModelEdge) error {
	for _, edge := range edges {
		if w.pruned(edge) {
			continue
		}
		switch edge.GetEdgeType() {
		case graph.DirectEdge:
			l := w.leafFrom(edge)
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
		case graph.ComputedEdge, graph.DirectLogicalEdge, graph.TTULogicalEdge, graph.RewriteEdge:
			children, ok := w.s.model.GetEdgesFromNode(edge.GetTo())
			if !ok {
				return ErrPanicRequest
			}
			if err := w.collect(children); err != nil {
				return err
			}
		default:
			return ErrPanicRequest
		}
	}
	return nil
}

// pruned reports whether an edge has no path to the request's user type, mirroring
// FlattenNode: such an edge contributes nothing (an unsatisfiable branch).
func (w *walker) pruned(edge *graph.WeightedAuthorizationModelEdge) bool {
	if _, ok := w.s.model.GetEdgeWeight(edge, w.userType); !ok {
		return true
	}
	if w.wildcard && !slices.Contains(edge.GetWildcards(), w.userType) {
		return true
	}
	return false
}

// fold reduces the subtree formed by edges under operation to a residual, delegating each
// leaf to reduce. State computation (the short-circuiting boolean algebra) is shared by every
// caller; reducers that yield a predicate (the render path) additionally have those predicates
// combined into residual.pred, while reducers that yield only a definite/unknown state (the
// contextual and gathered-result paths) leave pred nil.
func (w *walker) fold(ctx context.Context, edges []*graph.WeightedAuthorizationModelEdge, operation string, reduce leafReducer) (residual, error) {
	switch operation {
	case graph.UnionOperator:
		var preds []adapter.Predicate
		for _, edge := range edges {
			r, err := w.foldEdge(ctx, edge, reduce)
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
		return combinePreds(preds, branchFalse, orPred), nil
	case graph.IntersectionOperator:
		var preds []adapter.Predicate
		for _, edge := range edges {
			r, err := w.foldEdge(ctx, edge, reduce)
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
		return combinePreds(preds, branchTrue, andPred), nil
	case graph.ExclusionOperator:
		if w.wildcard {
			return residual{}, ErrWildcardInvalidRequest
		}
		if len(edges) != 2 {
			return residual{}, ErrPanicRequest
		}
		base, err := w.foldEdge(ctx, edges[0], reduce)
		if err != nil {
			return residual{}, err
		}
		if base.state == branchFalse {
			return residual{state: branchFalse}, nil
		}
		subtract, err := w.foldEdge(ctx, edges[1], reduce)
		if err != nil {
			return residual{}, err
		}
		if subtract.state == branchTrue {
			return residual{state: branchFalse}, nil
		}
		if subtract.state == branchFalse {
			// base AND NOT false == base (definite-true or the base predicate).
			return base, nil
		}
		// subtract is unknown; base is true or unknown. Both must be expressed in the predicate.
		if base.state == branchTrue {
			return residual{state: branchNeedsQuery, pred: notPred(subtract.pred)}, nil
		}
		return residual{state: branchNeedsQuery, pred: andPred(base.pred, notPred(subtract.pred))}, nil
	default:
		return residual{}, ErrPanicRequest
	}
}

// foldEdge folds a single edge: a pruned branch is false, a terminal is reduced as a leaf,
// and a rewrite/computed/logical edge recurses into its target node.
func (w *walker) foldEdge(ctx context.Context, edge *graph.WeightedAuthorizationModelEdge, reduce leafReducer) (residual, error) {
	if w.pruned(edge) {
		return residual{state: branchFalse}, nil
	}
	switch edge.GetEdgeType() {
	case graph.DirectEdge:
		// A direct edge always terminates at a type node, which is a weight-1 leaf.
		return reduce(ctx, w.leafFrom(edge))
	case graph.ComputedEdge, graph.DirectLogicalEdge, graph.TTULogicalEdge, graph.RewriteEdge:
		return w.foldNode(ctx, edge.GetTo(), reduce)
	default:
		return residual{}, ErrPanicRequest
	}
}

// foldNode folds all edges out of a node. An operator node combines by its label; any other
// node (a bare relation) combines its definition branches by union.
func (w *walker) foldNode(ctx context.Context, node *graph.WeightedAuthorizationModelNode, reduce leafReducer) (residual, error) {
	edges, ok := w.s.model.GetEdgesFromNode(node)
	if !ok {
		return residual{}, ErrPanicRequest
	}
	operation := graph.UnionOperator
	if node.GetNodeType() == graph.OperatorNode {
		operation = node.GetLabel()
	}
	return w.fold(ctx, edges, operation, reduce)
}

// reduceContextLeaf resolves a leaf against the request's contextual tuples only: branchTrue if
// a contextual tuple satisfies it (its condition passing), otherwise branchNeedsQuery — absence
// from the contextual set does not prove absence in the database.
func (w *walker) reduceContextLeaf(ctx context.Context, l leaf) (residual, error) {
	ok, err := w.contextLeafSatisfied(ctx, l)
	if err != nil {
		return residual{}, err
	}
	if ok {
		return residual{state: branchTrue}, nil
	}
	return residual{state: branchNeedsQuery}, nil
}

// contextLeafSatisfied reports whether a contextual tuple satisfies the leaf (matching the
// request subject/object and passing the leaf's condition). An unconditioned leaf carries the
// NoCond ("") sentinel in l.conditions and an unconditioned tuple has an empty condition name,
// so evaluateCondition matches and passes it without any per-condition special-casing here.
func (w *walker) contextLeafSatisfied(ctx context.Context, l leaf) (bool, error) {
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
		ok, err := evaluateCondition(ctx, w.s.model, l.conditions, ct, w.req.GetContext())
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// evalUnconditioned answers the no-condition case with a single existence query: SELECT 1 over
// the referenced relations, with the boolean subtree rendered into HAVING. Because object_id is
// pinned to a single value by the shared WHERE, the aggregates form one implicit group, so no
// GROUP BY is needed.
func (w *walker) evalUnconditioned(ctx context.Context, edges []*graph.WeightedAuthorizationModelEdge, operation string) (*Response, error) {
	res, err := w.fold(ctx, edges, operation, w.reduceRenderLeaf)
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
	// lives here in the shared WHERE rather than in each COUNT(CASE). The empty-condition
	// sentinel is a fixed, trusted constant, so it is emitted with Lit rather than Bind.
	where := append(w.whereShared(),
		w.table.ObjectRelation().In(w.relationBinds()...),
		w.table.Condition().IsNull().Or(w.table.Condition().Eq(w.builder.Lit(""))),
	)
	stmt := w.builder.Select(w.builder.Lit(1)).
		From(w.table).
		Where(where...).
		Having(res.pred).
		Limit(1)
	rows, err := w.builder.Build(stmt).Execute(ctx)
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
// condition columns, evaluates each row's condition in-app, then folds the subtree with the
// per-leaf results.
func (w *walker) evalConditioned(ctx context.Context, edges []*graph.WeightedAuthorizationModelEdge, operation string) (*Response, error) {
	// The relation/wildcard/condition state was already populated by collect in weight1, so the
	// WHERE filter can be built directly.
	if len(w.relations) == 0 {
		return &Response{Allowed: false}, nil
	}
	stmt := w.builder.Select(
		w.table.ObjectRelation(),
		w.table.SubjectID(),
		w.table.SubjectRelation(),
		w.table.Condition(),
		w.table.ConditionContext(),
	).
		From(w.table).
		Where(append(w.whereShared(), w.table.ObjectRelation().In(w.relationBinds()...))...)
	rows, err := w.builder.Build(stmt).Execute(ctx)
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

	reduce := func(ctx context.Context, l leaf) (residual, error) {
		ok, err := w.leafSatisfied(ctx, l, gathered)
		if err != nil {
			return residual{}, err
		}
		if ok {
			return residual{state: branchTrue}, nil
		}
		return residual{state: branchFalse}, nil
	}
	res, err := w.fold(ctx, edges, operation, reduce)
	if err != nil {
		return nil, err
	}
	return &Response{Allowed: res.state == branchTrue}, nil
}

// leafSatisfied reports whether a contextual tuple or a gathered database row matches the
// leaf and passes the leaf's condition. The contextual re-check here is intentional: it is
// cheap (contextual tuples are in-memory) and lets a contextual tuple satisfy a leaf that has
// no matching database row.
func (w *walker) leafSatisfied(ctx context.Context, l leaf, gathered []gatheredRow) (bool, error) {
	if ok, err := w.contextLeafSatisfied(ctx, l); err != nil {
		return false, err
	} else if ok {
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
		ok, err := evaluateCondition(ctx, w.s.model, l.conditions, tk, w.req.GetContext())
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// reduceRenderLeaf renders a leaf on the existence path: constant-true if a contextual tuple
// already satisfies it, otherwise an existence aggregate counting rows for the leaf's relation.
// The subject and unconditioned-tuple narrowing is uniform across every leaf, so it lives once
// in the shared WHERE (see evalUnconditioned) rather than in each count's FILTER clause. The
// contextual re-check mirrors leafSatisfied and is intentional (see its doc).
func (w *walker) reduceRenderLeaf(ctx context.Context, l leaf) (residual, error) {
	ok, err := w.contextLeafSatisfied(ctx, l)
	if err != nil {
		return residual{}, err
	}
	if ok {
		return residual{state: branchTrue}, nil
	}
	match := w.table.ObjectRelation().Eq(w.builder.Bind(l.relation))
	count := w.builder.Aggregate(adapter.AggCount, w.builder.Lit(1)).Filter(match)
	return residual{state: branchNeedsQuery, pred: count.Gt(w.builder.Lit(0))}, nil
}

// whereShared returns the predicates common to every weight-1 query: the store, the
// object, and a narrowing of the subject to the exact request subject (plus the public
// wildcard when a wildcard leaf is present).
func (w *walker) whereShared() []adapter.Predicate {
	preds := []adapter.Predicate{
		w.table.Store().Eq(w.builder.Bind(w.req.GetStoreID())),
		w.table.ObjectType().Eq(w.builder.Bind(w.objectType)),
		w.table.ObjectID().Eq(w.builder.Bind(w.objectID)),
		w.table.SubjectType().Eq(w.builder.Bind(w.subjType)),
		w.table.SubjectRelation().Eq(w.builder.Bind(w.subjRel)),
	}
	if w.hasWildcardLeaf && w.subjID != tuple.Wildcard {
		preds = append(preds, w.table.SubjectID().In(w.builder.Bind(w.subjID), w.builder.Bind(tuple.Wildcard)))
	} else {
		preds = append(preds, w.table.SubjectID().Eq(w.builder.Bind(w.subjID)))
	}
	return preds
}

// relationBinds renders the accumulated relation set as a sorted list of bound parameters
// for the WHERE `relation IN (...)` filter (sorted for deterministic SQL).
func (w *walker) relationBinds() []adapter.Expression {
	names := make([]string, 0, len(w.relations))
	for r := range w.relations {
		names = append(names, r)
	}
	sort.Strings(names)
	binds := make([]adapter.Expression, len(names))
	for i, r := range names {
		binds[i] = w.builder.Bind(r)
	}
	return binds
}

// combinePreds folds preds with join; an empty list yields the given identity state. Reducers
// that carry no predicate (the contextual and gathered-result paths) contribute nil preds, in
// which case join returns nil and the result is a bare branchNeedsQuery.
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

// andPred, orPred, and notPred combine predicates that may be nil. Within a single fold every
// branchNeedsQuery residual is either all-nil (non-render reducers) or all-non-nil (the render
// reducer), so returning nil when any operand is nil is correct: the render path always yields
// a real predicate, while the other paths never read one.
func andPred(a, b adapter.Predicate) adapter.Predicate {
	if a == nil || b == nil {
		return nil
	}
	return a.And(b)
}

func orPred(a, b adapter.Predicate) adapter.Predicate {
	if a == nil || b == nil {
		return nil
	}
	return a.Or(b)
}

func notPred(a adapter.Predicate) adapter.Predicate {
	if a == nil {
		return nil
	}
	return a.Not()
}
