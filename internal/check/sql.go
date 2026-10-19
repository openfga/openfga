package check

import (
	"context"
	"database/sql"
	"slices"
	"sort"
	"strings"

	"github.com/openfga/openfga/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
	"github.com/openfga/openfga/pkg/tuple"
)

var _ GroupStrategy = (*SQLStrategy)(nil)

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
	return s.weight1(ctx, req, s.datastore.Querier(req.GetConsistency()), edge.Edges, graph.UnionOperator)
}

func (s *SQLStrategy) Intersection(ctx context.Context, req *Request, edge *GroupEdge) (*Response, error) {
	return s.weight1(ctx, req, s.datastore.Querier(req.GetConsistency()), edge.Edges, graph.IntersectionOperator)
}

func (s *SQLStrategy) Exclusion(ctx context.Context, req *Request, edge *GroupEdge) (*Response, error) {
	return s.weight1(ctx, req, s.datastore.Querier(req.GetConsistency()), edge.Edges, graph.ExclusionOperator)
}

// branchOutcome is what is known about a branch: branchTrue/branchFalse are definite,
// branchNeedsQuery means the answer requires a database read.
type branchOutcome uint8

const (
	branchNeedsQuery branchOutcome = iota
	branchTrue
	branchFalse
)

// leaf represents a terminal node in the weighted graph (direct assignment, weight-1):
// its stored `relation`, whether it matches the public wildcard subject `type:*` (rather
// than the exact request subject), and the set of condition names the edge admits.
type leaf struct {
	relation   string
	wildcard   bool
	conditions []string
}

// relKey identifies the rows a leaf reads: its stored relation and whether it is a wildcard for
// the subject type but the request is for a direct subject.
type relKey struct {
	relation string
	wildcard bool
}

// gatheredRow is one row of the conditioned gather query, holding the fields needed to
// match it to a leaf and to run the tuple's condition.
type gatheredRow struct {
	relation  string
	subjectID string
	condName  string
	condCtx   []byte

	// evaluated reports whether satisfied holds the row's condition result.
	evaluated bool
	satisfied bool
}

// residual is the outcome of evaluating a subtree: a definite state, or branchNeedsQuery
// optionally carrying the SQL predicate that must hold for the branch to be satisfied.
// evaluateLeaf sets pred (consumed on the unconditioned path); the gathered-row evaluator leaves
// it nil, since the gather path re-evaluates against the read rows rather than a predicate.
type residual struct {
	state branchOutcome
	pred  *query.Predicate
}

// leafEvaluator evaluates a single weight-1 leaf to a residual. The subtree is evaluated at most
// twice: once with evaluateLeaf, and again on the conditioned path with an evaluator that matches
// each leaf against the gathered rows.
type leafEvaluator func(ctx context.Context, l leaf) (residual, error)

// weight1 answers whether the request's user has the requested relation to the object,
// evaluating the whole group of weight-1 edges (combined by operation) in a single SQL
// round-trip. Without conditions, it issues an unconditioned query (SELECT 1 ... HAVING <tree>);
// with conditions it gathers candidate tuples and evaluates their conditions in-app.
func (s *SQLStrategy) weight1(ctx context.Context, req *Request, querier adapter.Querier, edges []*graph.WeightedAuthorizationModelEdge, operation string) (resp *Response, err error) {
	ctx, span := tracer.Start(ctx, "sql.weight1", trace.WithAttributes(
		attribute.String("tuple_key", req.GetTupleString()),
		attribute.String("operation", operation),
		attribute.Bool("allowed", false),
	))
	defer span.End()

	defer func() {
		if err != nil {
			telemetry.TraceError(span, err)
			return
		}
		if resp.GetAllowed() {
			span.SetAttributes(attribute.Bool("allowed", true))
		}
	}()

	w := &walker{
		s:           s,
		req:         req,
		querier:     querier,
		table:       query.NewTuple("t"),
		userType:    req.GetUserType(),
		wildcardReq: req.IsTypedWildcard(),
		relConds:    map[relKey]map[string]struct{}{},
	}

	if len(req.GetContextualTuples()) > 0 {
		w.hasCtxTuples = true
		w.ctxSatisfied = map[string]struct{}{}
	}

	w.subjType, w.subjID, w.subjRel = tuple.ToUserParts(req.GetTupleKey().GetUser())
	w.objectType, w.objectID = tuple.SplitObject(req.GetTupleKey().GetObject())

	// Determine if the request was for a direct subject but a wildcard for the subject type is
	// also reachable. In that case, we will need to query for both subject IDs and specify the
	// subject ID each relation filter should match.
	w.queriesDirectAndWildcardSubject = w.subjID != tuple.Wildcard && slices.ContainsFunc(edges, func(e *graph.WeightedAuthorizationModelEdge) bool {
		// An edge's wildcards cover every wildcard reachable through it
		return slices.Contains(e.GetWildcards(), w.userType)
	})

	// A single evaluation pass short-circuits on contextual tuples, accumulates the WHERE filter
	// state (relConds/conditioned), and renders the unconditioned predicate. Since determined
	// branches are pruned, only leaves that need a database read contribute to the filter,
	// keeping the query narrower than a full walk of the subtree would.
	res, err := w.evaluateSubtree(ctx, edges, operation, w.evaluateLeaf)
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.Int("relation_count", len(w.relConds)))

	switch res.state {
	case branchTrue:
		span.SetAttributes(
			attribute.String("query_path", "no_query"),
			attribute.Bool("allowed", true),
		)
		return &Response{Allowed: true}, nil
	case branchFalse:
		span.SetAttributes(attribute.String("query_path", "no_query"))
		return &Response{Allowed: false}, nil
	default: // branchNeedsQuery: a database read is required.
	}

	// A conditioned leaf survived, so conditions must be evaluated in-app: gather the candidate
	// rows and re-evaluate against them. The unconditioned predicate counts rows without checking
	// conditions, so it is discarded, but the accumulated filter state drives the gather query.
	if w.conditioned {
		span.SetAttributes(attribute.String("query_path", "conditioned"))
		return w.evalConditioned(ctx, edges, operation)
	}

	span.SetAttributes(attribute.String("query_path", "unconditioned"))
	return w.evalUnconditioned(ctx, res.pred)
}

// walker carries the per-request state shared by the subtree traversals.
type walker struct {
	s       *SQLStrategy
	req     *Request
	querier adapter.Querier
	table   query.Tuple

	userType    string
	wildcardReq bool

	subjType string
	subjID   string
	subjRel  string

	objectType string
	objectID   string

	// relConds maps each relKey referenced by a surviving leaf to the set of condition names it
	// admits (including NoCond). It is used to form the  WHERE `relation IN (...)` filter and,
	// on the conditioned path, the per-relation condition filter. Accumulated by evaluateLeaf.
	relConds map[relKey]map[string]struct{}
	// conditioned is set when a surviving leaf admits a named condition, selecting the gather
	// path over the unconditioned path. Accumulated by evaluateLeaf.
	conditioned bool
	// sawNonUnion is set when the walk reaches an intersection or exclusion. When this is false
	// (i.e., pure union), the HAVING clause in the unconditioned path can be omitted.
	sawNonUnion bool
	// queriesDirectAndWildcardSubject is whether the request is on a direct subject but a wildcard
	// for the subject type is also reachable. In this case, both the direct and the wildcard
	// subject IDs must be admitted in the WHERE, and each relation filter must specify which
	// subject ID it's for, to avoid matching a row for the wrong subject (e.g., a wildcard tuple
	// left over from an older model).
	queriesDirectAndWildcardSubject bool
	// hasCtxTuples is whether the request carries contextual tuples.
	hasCtxTuples bool
	// ctxSatisfied holds the leafKey of every leaf that a contextual tuple satisfied during the
	// first pass. The gather pass reads it instead of re-evaluating the same contextual tuples, so
	// their conditions are evaluated once per request rather than once per pass. Nil (and unused)
	// when hasCtxTuples is false.
	ctxSatisfied map[string]struct{}
}

// leafFrom derives a leaf descriptor from a terminal direct edge.
func (w *walker) leafFrom(edge *graph.WeightedAuthorizationModelEdge) leaf {
	return leaf{
		relation:   tuple.GetRelation(edge.GetRelationDefinition()),
		wildcard:   edge.GetTo().GetNodeType() == graph.SpecificTypeWildcard,
		conditions: edge.GetConditions(),
	}
}

// relKeyOf derives the relKey a leaf reads, distinguishing wildcard leaves only when w.splitSubjects == true.
func (w *walker) relKeyOf(l leaf) relKey {
	return relKey{relation: l.relation, wildcard: w.queriesDirectAndWildcardSubject && l.wildcard}
}

// relationFilter builds the predicate matching rows that belong to the relation key k, with any
// extra predicates in rest ANDed on. When subjects are split, it also pins the subject ID (the
// wildcard for wildcard keys, the request subject otherwise) so a row for the other subject can't
// match this relation.
func (w *walker) relationFilter(k relKey, rest ...query.Predicate) query.Predicate {
	rel := query.Eq(w.table.ObjectRelation(), query.Lit(k.relation))

	if w.queriesDirectAndWildcardSubject {
		// There is more than one subject ID, so we need to specify which one this relation is for.
		var subj query.Predicate
		if k.wildcard {
			subj = query.Eq(w.table.SubjectID(), query.Lit(tuple.Wildcard))
		} else {
			subj = query.Eq(w.table.SubjectID(), query.Bind(w.subjID))
		}
		rest = append([]query.Predicate{subj}, rest...)
	}

	return query.And(rel, rest...)
}

// accumulateLeafConditions adds l's condition names into w's relConds under l's relKey, and sets
// w.conditioned if any is a named condition.
func (w *walker) accumulateLeafConditions(l leaf) {
	k := w.relKeyOf(l)
	conds, ok := w.relConds[k]
	if !ok {
		conds = map[string]struct{}{}
		w.relConds[k] = conds
	}
	for _, c := range l.conditions {
		conds[c] = struct{}{}
		if c != graph.NoCond {
			w.conditioned = true
		}
	}
}

// pruned reports whether an edge has no path to the request's user type, mirroring
// FlattenNode: such an edge contributes nothing (an unsatisfiable branch).
func (w *walker) pruned(edge *graph.WeightedAuthorizationModelEdge) bool {
	if _, ok := w.s.model.GetEdgeWeight(edge, w.userType); !ok {
		return true
	}
	if w.wildcardReq && !slices.Contains(edge.GetWildcards(), w.userType) {
		return true
	}
	return false
}

// evaluateSubtree evaluates the subtree formed by edges under operation to a residual, delegating
// each leaf to evalLeaf. The short-circuiting boolean algebra is shared by both callers; evaluateLeaf
// additionally combines a predicate into residual.pred, while the gathered-result evaluator leaves
// pred nil.
func (w *walker) evaluateSubtree(ctx context.Context, edges []*graph.WeightedAuthorizationModelEdge, operation string, evalLeaf leafEvaluator) (residual, error) {
	switch operation {
	case graph.UnionOperator:
		var preds []*query.Predicate
		for _, edge := range edges {
			r, err := w.evaluateEdge(ctx, edge, evalLeaf)
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
		w.sawNonUnion = true
		var preds []*query.Predicate
		for _, edge := range edges {
			r, err := w.evaluateEdge(ctx, edge, evalLeaf)
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
		w.sawNonUnion = true
		if w.wildcardReq {
			return residual{}, ErrWildcardInvalidRequest
		}
		if len(edges) != 2 {
			return residual{}, ErrPanicRequest
		}
		base, err := w.evaluateEdge(ctx, edges[0], evalLeaf)
		if err != nil {
			return residual{}, err
		}
		if base.state == branchFalse {
			return residual{state: branchFalse}, nil
		}
		subtract, err := w.evaluateEdge(ctx, edges[1], evalLeaf)
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

// evaluateEdge evaluates a single edge: a pruned branch is false, a terminal is reduced as a leaf,
// and a rewrite/computed/logical edge recurses into its target node.
func (w *walker) evaluateEdge(ctx context.Context, edge *graph.WeightedAuthorizationModelEdge, evalLeaf leafEvaluator) (residual, error) {
	if w.pruned(edge) {
		return residual{state: branchFalse}, nil
	}
	switch edge.GetEdgeType() {
	case graph.DirectEdge:
		// A direct edge always terminates at a type node, which is a weight-1 leaf.
		return evalLeaf(ctx, w.leafFrom(edge))
	case graph.ComputedEdge, graph.DirectLogicalEdge, graph.TTULogicalEdge, graph.RewriteEdge:
		return w.evaluateNode(ctx, edge.GetTo(), evalLeaf)
	default:
		return residual{}, ErrPanicRequest
	}
}

// evaluateNode evaluates all edges out of a node. An operator node combines by its label; any
// other node (a bare relation) combines its definition branches by union.
func (w *walker) evaluateNode(ctx context.Context, node *graph.WeightedAuthorizationModelNode, evalLeaf leafEvaluator) (residual, error) {
	edges, ok := w.s.model.GetEdgesFromNode(node)
	if !ok {
		return residual{}, ErrPanicRequest
	}
	operation := graph.UnionOperator
	if node.GetNodeType() == graph.OperatorNode {
		operation = node.GetLabel()
	}
	return w.evaluateSubtree(ctx, edges, operation, evalLeaf)
}

// evaluateLeaf is the first-pass evaluator. A contextual tuple satisfying the leaf resolves it to
// branchTrue and accumulates no filter state, since it needs no row. Otherwise the leaf needs a
// database read: its filter state is recorded and it renders the unconditioned aggregate counting
// rows for its relation. Absence from the contextual set does not prove absence in the database,
// so a miss is branchNeedsQuery, never branchFalse.
func (w *walker) evaluateLeaf(ctx context.Context, l leaf) (residual, error) {
	ok, err := w.contextLeafSatisfied(ctx, l)
	if err != nil {
		return residual{}, err
	}
	if ok {
		// Record the verdict so the gather pass can reuse it instead of re-evaluating the same
		// contextual tuples (see leafSatisfied). Only reachable when hasCtxTuples, so ctxSatisfied
		// is non-nil.
		w.ctxSatisfied[leafKey(l)] = struct{}{}
		return residual{state: branchTrue}, nil
	}
	w.accumulateLeafConditions(l)
	// Return a predicate for existence of this leaf's relation, which is used in the unconditioned path.
	count := query.Count(query.Lit[int64](1), query.AggFilter(w.relationFilter(w.relKeyOf(l))))
	pred := query.Gt(count, query.Lit[int64](0))
	return residual{state: branchNeedsQuery, pred: &pred}, nil
}

// contextLeafSatisfied reports whether a contextual tuple satisfies the leaf (matching the
// request subject/object and passing the leaf's condition). An unconditioned leaf carries the
// NoCond ("") sentinel and an unconditioned tuple has an empty condition name, so
// evaluateCondition matches and passes it without special-casing here. Candidates are matched and
// their conditions evaluated in a single pass, so no intermediate slice is built; a request with
// no contextual tuples returns before building any lookup key.
func (w *walker) contextLeafSatisfied(ctx context.Context, l leaf) (bool, error) {
	if !w.hasCtxTuples {
		return false, nil
	}
	var cts []*openfgav1.TupleKey
	var ok bool
	if l.wildcard {
		cts, ok = w.req.GetContextualTuplesByObjectID(w.req.GetTupleKey().GetObject(), l.relation, w.userType)
	} else {
		cts, ok = w.req.GetContextualTuplesByUserID(w.req.GetTupleKey().GetUser(), l.relation, w.objectType)
	}
	if !ok {
		return false, nil
	}
	for _, ct := range cts {
		if l.wildcard {
			if !tuple.IsTypedWildcard(ct.GetUser()) {
				continue
			}
		} else if ct.GetObject() != w.req.GetTupleKey().GetObject() {
			continue
		}
		satisfied, err := evaluateCondition(ctx, w.s.model, l.conditions, ct, w.req.GetContext())
		if err != nil {
			return false, err
		}
		if satisfied {
			return true, nil
		}
	}
	return false, nil
}

// leafKey identifies a leaf by the fields that determine its contextual-tuple verdict: its
// relation, whether it matches the wildcard subject, and the condition names it admits. leafFrom
// derives leaves from the same edges on both passes, so a leaf produces the same key each time.
// Condition names are model identifiers and never contain a NUL byte, so NUL is a safe separator.
func leafKey(l leaf) string {
	var b strings.Builder
	b.WriteString(l.relation)
	b.WriteByte(0)
	if l.wildcard {
		b.WriteByte('1')
	} else {
		b.WriteByte('0')
	}
	for _, c := range l.conditions {
		b.WriteByte(0)
		b.WriteString(c)
	}
	return b.String()
}

// evalUnconditioned answers the no-condition case with a single SELECT 1 query over the
// surviving relations, with the boolean subtree supplied as pred in HAVING. In the case of a pure
// union (see sawNonUnion), the HAVING clause is omitted so that LIMIT 1 can stop the scan at the
// first matching row.
func (w *walker) evalUnconditioned(ctx context.Context, pred *query.Predicate) (*Response, error) {
	where := append(w.whereShared(), w.relationIn(), w.unconditioned())
	stmt := query.Select(query.Lit[int64](1)).
		From(w.table).
		Where(query.And(where[0], where[1:]...)).
		Limit(1)
	if pred != nil && (w.sawNonUnion || w.queriesDirectAndWildcardSubject) {
		stmt = stmt.Having(*pred)
	}
	rows, err := w.querier.Execute(ctx, stmt)
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
// condition columns, evaluates each row's condition in-app, then evaluates the subtree with the
// per-leaf results.
func (w *walker) evalConditioned(ctx context.Context, edges []*graph.WeightedAuthorizationModelEdge, operation string) (*Response, error) {
	// Defensive: this should never happen since reaching here implies a conditioned leaf survived
	// and relConds is non-empty.
	if len(w.relConds) == 0 {
		return &Response{Allowed: false}, nil
	}

	where := append(w.whereShared(), w.relationIn(), w.conditionPairing())
	stmt := query.Select(
		w.table.ObjectRelation(),
		w.table.SubjectID(),
		w.table.Condition(),
		w.table.ConditionContext(),
	).
		From(w.table).
		Where(query.And(where[0], where[1:]...))
	rows, err := w.querier.Execute(ctx, stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	gathered := make([]gatheredRow, 0, len(w.relConds))
	for rows.Next() {
		var (
			relation, subjectID string
			condName            sql.NullString
			condCtx             []byte
		)
		if err := rows.Scan(&relation, &subjectID, &condName, &condCtx); err != nil {
			return nil, err
		}
		gathered = append(gathered, gatheredRow{relation: relation, subjectID: subjectID, condName: condName.String, condCtx: condCtx})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	evalLeaf := func(ctx context.Context, l leaf) (residual, error) {
		ok, err := w.leafSatisfied(ctx, l, gathered)
		if err != nil {
			return residual{}, err
		}
		if ok {
			return residual{state: branchTrue}, nil
		}
		return residual{state: branchFalse}, nil
	}
	res, err := w.evaluateSubtree(ctx, edges, operation, evalLeaf)
	if err != nil {
		return nil, err
	}
	return &Response{Allowed: res.state == branchTrue}, nil
}

// leafSatisfied reports whether a contextual tuple or a gathered database row matches the leaf and
// passes its condition. The contextual verdict is not recomputed here: the first pass recorded
// every context-satisfied leaf in ctxSatisfied, and this pass visits a subset of those leaves (its
// extra short-circuits only prune further), so a set lookup answers the contextual case. This still
// lets a contextual tuple satisfy a leaf that has no matching database row.
func (w *walker) leafSatisfied(ctx context.Context, l leaf, gathered []gatheredRow) (bool, error) {
	if w.hasCtxTuples {
		if _, ok := w.ctxSatisfied[leafKey(l)]; ok {
			return true, nil
		}
	}

	for i := range gathered {
		g := &gathered[i]
		if g.relation != l.relation {
			continue
		}
		if l.wildcard {
			if g.subjectID != tuple.Wildcard {
				continue
			}
		} else if g.subjectID != w.subjID {
			continue
		}

		// The result may have been computed for an earlier leaf. Reusing it is sound because every
		// leaf that reaches a row comes from the same edge (a relation's conditions for one target
		// type are merged onto a single edge), so all of them admit the same conditions.
		if !g.evaluated {
			ok, err := w.evaluateRow(ctx, g, l)
			if err != nil {
				return false, err
			}
			g.evaluated, g.satisfied = true, ok
		}
		// Tuples are unique per (object, relation, user), and the shared WHERE plus the relation and
		// subject checks above fix all of these, so no other row can match this leaf.
		return g.satisfied, nil
	}
	return false, nil
}

// evaluateRow decodes the row's condition context and runs its condition against the request
// context, reporting whether the tuple the row represents is satisfied.
func (w *walker) evaluateRow(ctx context.Context, g *gatheredRow, l leaf) (bool, error) {
	var condCtx *structpb.Struct
	if len(g.condCtx) > 0 {
		condCtx = &structpb.Struct{}
		if err := proto.Unmarshal(g.condCtx, condCtx); err != nil {
			return false, err
		}
	}
	user := tuple.BuildObject(w.subjType, g.subjectID)
	if w.subjRel != "" {
		user = tuple.ToObjectRelationString(user, w.subjRel)
	}
	tk := &openfgav1.TupleKey{
		Object:    w.req.GetTupleKey().GetObject(),
		Relation:  g.relation,
		User:      user,
		Condition: tuple.NewRelationshipCondition(g.condName, condCtx),
	}
	return evaluateCondition(ctx, w.s.model, l.conditions, tk, w.req.GetContext())
}

// whereShared returns the predicates common to every weight-1 query: the store, the
// object, and a narrowing of the subject to the exact request subject (plus the public
// wildcard when subjects are split).
func (w *walker) whereShared() []query.Predicate {
	preds := []query.Predicate{
		query.Eq(w.table.Store(), query.Bind(w.req.GetStoreID())),
		query.Eq(w.table.ObjectType(), query.Bind(w.objectType)),
		query.Eq(w.table.ObjectID(), query.Bind(w.objectID)),
		query.Eq(w.table.SubjectType(), query.Bind(w.subjType)),
		query.Eq(w.table.SubjectRelation(), query.Bind(w.subjRel)),
	}
	if w.queriesDirectAndWildcardSubject {
		preds = append(preds, query.In(w.table.SubjectID(), query.Bind(w.subjID), query.Bind(tuple.Wildcard)))
	} else {
		preds = append(preds, query.Eq(w.table.SubjectID(), query.Bind(w.subjID)))
	}
	return preds
}

// unconditioned returns the predicate for rows carrying no condition.
func (w *walker) unconditioned() query.Predicate {
	return query.Or(
		query.IsNull(w.table.Condition()),
		query.Eq(w.table.Condition(), query.Lit(graph.NoCond)),
	)
}

// sortedKeys returns w's accumulated relKeys in deterministic order: by relation, with the exact
// subject's key before the wildcard's.
func (w *walker) sortedKeys() []relKey {
	keys := make([]relKey, 0, len(w.relConds))
	for k := range w.relConds {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b relKey) int {
		if c := strings.Compare(a.relation, b.relation); c != 0 {
			return c
		}
		// Keys are unique, so equal relations differ in wildcard.
		if b.wildcard {
			return -1
		}
		return 1
	})
	return keys
}

// relationIn renders the accumulated relation set as a sorted `relation IN (...)` filter for the
// WHERE of both the unconditioned and gather paths, collapsing to `relation = r` when a single
// relation survives. Relation names are inlined rather than bound since they come from the model
// and cannot contain special characters like spaces for SQL injection.
func (w *walker) relationIn() query.Predicate {
	keys := w.sortedKeys()
	rels := make([]string, 0, len(keys))
	for _, k := range keys {
		// A relation read by both an exact-subject and a wildcard leaf has adjacent keys.
		if len(rels) == 0 || rels[len(rels)-1] != k.relation {
			rels = append(rels, k.relation)
		}
	}
	if len(rels) == 1 {
		return query.Eq(w.table.ObjectRelation(), query.Lit(rels[0]))
	}
	lits := make([]query.Expr[string], len(rels))
	for i, r := range rels {
		lits[i] = query.Lit(r)
	}
	return query.In(w.table.ObjectRelation(), lits...)
}

// conditionPairing pairs each relation with the conditions the model admits for it. A single
// global `relation IN (...) AND condition IN (...)` would be wrong: it would admit a tuple whose
// relation and condition are each referenced but by different leaves (e.g. rel1 carrying a
// condition only rel2 admits). Instead, it emits a disjunction of per-relation clauses —
// `(relation = r AND <conds for r>) OR ...`, each also naming its subject when subjects are split
// (see relationFilter). Keys are sorted for deterministic SQL; evalConditioned guards against an
// empty relation set, so the returned predicate is never empty.
func (w *walker) conditionPairing() query.Predicate {
	var clauses []query.Predicate
	for _, k := range w.sortedKeys() {
		clauses = append(clauses, w.relationFilter(k, w.conditionPred(w.relConds[k])))
	}
	pred := clauses[0]
	for _, c := range clauses[1:] {
		pred = query.Or(pred, c)
	}
	return pred
}

// conditionPred renders the predicate admitting exactly the condition names in conds: the named
// conditions via `condition IN (...)` plus, when the NoCond sentinel is present, the
// unconditioned rows. Names are sorted for deterministic SQL and, coming from the model, are safe
// to inline rather than bind.
func (w *walker) conditionPred(conds map[string]struct{}) query.Predicate {
	condNames := make([]string, 0, len(conds))
	unconditioned := false
	for c := range conds {
		if c == graph.NoCond {
			unconditioned = true
			continue
		}
		condNames = append(condNames, c)
	}
	sort.Strings(condNames)

	// At most two preds: one condition pred (Eq or In, mutually exclusive) and the unconditioned pred.
	preds := make([]query.Predicate, 0, 2)
	if len(condNames) == 1 {
		preds = append(preds, query.Eq(w.table.Condition(), query.Lit(condNames[0])))
	} else if len(condNames) > 0 {
		lits := make([]query.Expr[string], len(condNames))
		for i, c := range condNames {
			lits[i] = query.Lit(c)
		}
		preds = append(preds, query.In(w.table.Condition(), lits...))
	}
	if unconditioned {
		preds = append(preds, w.unconditioned())
	}

	// preds is never empty: every edge's GetConditions is normalized to at least NoCond
	pred := preds[0]
	for _, p := range preds[1:] {
		pred = query.Or(pred, p)
	}
	return pred
}

// combinePreds combines preds with join; an empty list yields the given identity state. The
// gathered-result evaluator contributes nil preds, in which case join returns nil and the result is
// a bare branchNeedsQuery.
func combinePreds(preds []*query.Predicate, empty branchOutcome, join func(a, b *query.Predicate) *query.Predicate) residual {
	if len(preds) == 0 {
		return residual{state: empty}
	}
	acc := preds[0]
	for _, p := range preds[1:] {
		acc = join(acc, p)
	}
	return residual{state: branchNeedsQuery, pred: acc}
}

// andPred, orPred, and notPred combine predicates that may be nil. Within a single evaluation the
// operands are either all-nil (gathered-result evaluator) or all-non-nil (evaluateLeaf), so returning
// nil when any operand is nil is correct.
func andPred(a, b *query.Predicate) *query.Predicate {
	if a == nil || b == nil {
		return nil
	}
	p := query.And(*a, *b)
	return &p
}

func orPred(a, b *query.Predicate) *query.Predicate {
	if a == nil || b == nil {
		return nil
	}
	p := query.Or(*a, *b)
	return &p
}

func notPred(a *query.Predicate) *query.Predicate {
	if a == nil {
		return nil
	}
	p := query.Not(*a)
	return &p
}
