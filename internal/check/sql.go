package check

import (
	"context"
	"database/sql"
	"slices"
	"sort"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/internal/telemetry"
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

// branchState is a branch's value: decided without a read (branchTrue/branchFalse), or pending
// one (branchNeedsQuery).
type branchState uint8

const (
	branchNeedsQuery branchState = iota
	branchTrue
	branchFalse
)

// leaf is a weight-1 direct assignment. A wildcard leaf's edge targets `type:*`, so it matches
// only wildcard tuples.
type leaf struct {
	relation   string
	wildcard   bool
	conditions []string
}

// relKey identifies the rows a leaf reads (see relKeyOf).
type relKey struct {
	relation string
	wildcard bool
}

// gatheredRow is one row of the conditioned query.
type gatheredRow struct {
	relation  string
	subjectID string
	condName  string
	condCtx   []byte

	// evaluated and satisfied memoize the row's condition result across leaves.
	evaluated bool
	satisfied bool
}

// branchResult is the outcome of evaluating a subtree. When state is branchNeedsQuery, pred is the
// HAVING predicate for the unconditioned path; it is nil throughout the conditioned pass, which
// evaluates against the read rows instead, so the pred combinators just propagate nil.
type branchResult struct {
	state branchState
	pred  *query.Predicate
}

// leafEvaluator lets the same subtree walk serve both passes: evaluateLeaf first, then a
// gathered-row evaluator on the conditioned path.
type leafEvaluator func(ctx context.Context, l leaf) (branchResult, error)

// weight1 resolves a group of weight-1 edges in a single SQL round-trip. When conditions are
// encountered, the candidate rows are gathered and conditions are evaluated in-app.
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
		model:          s.model,
		req:            req,
		querier:        querier,
		table:          query.NewTuple("t"),
		subjNodeID:     req.GetUserType(),
		subjIsWildcard: req.IsTypedWildcard(),
		relConds:       map[relKey]map[string]struct{}{},
	}

	if len(req.GetContextualTuples()) > 0 {
		w.hasCtxTuples = true
		w.ctxVerdicts = map[string]bool{}
	}

	w.subjType, w.subjID, w.subjRel = tuple.ToUserParts(req.GetTupleKey().GetUser())
	w.objectType, w.objectID = tuple.SplitObject(req.GetTupleKey().GetObject())

	w.alsoQueriesWildcard = !w.subjIsWildcard && slices.ContainsFunc(edges, func(e *graph.WeightedAuthorizationModelEdge) bool {
		// An edge's wildcards cover every wildcard reachable through it.
		return slices.Contains(e.GetWildcards(), w.subjNodeID)
	})

	// Branches decided without a read (e.g. by contextual tuples) are short-circuited, so only
	// leaves that need a read contribute to the WHERE filter.
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

	// res.pred ignores conditions, so it's discarded; the filter state from the first pass still
	// drives the conditioned query.
	if w.conditioned {
		span.SetAttributes(attribute.String("query_path", "conditioned"))
		return w.evalConditioned(ctx, edges, operation)
	}

	span.SetAttributes(attribute.String("query_path", "unconditioned"))
	return w.evalUnconditioned(ctx, res.pred)
}

// walker carries the per-request state shared by the subtree traversals.
type walker struct {
	model   *modelgraph.AuthorizationModelGraph
	req     *Request
	querier adapter.Querier
	table   query.Tuple

	subjIsWildcard bool

	objectType string
	objectID   string
	// subjNodeID is the subject's weighted-graph node ID: "type", or "type#rel" for usersets.
	subjNodeID string
	subjType   string
	subjID     string
	subjRel    string

	// relConds holds, per relKey of a surviving leaf, the condition names it admits (incl. NoCond).
	relConds map[relKey]map[string]struct{}
	// conditioned is set when a surviving leaf admits a named condition, selecting that query path.
	conditioned bool
	// sawNonUnion is set on reaching an intersection or exclusion; a pure union needs no HAVING.
	sawNonUnion bool
	// alsoQueriesWildcard is true when the request is for a direct subject but a wildcard for its
	// type is also reachable. In this case, both subject IDs must be queried for, and each relation
	// filter must specify which subject ID it is for.
	alsoQueriesWildcard bool
	hasCtxTuples        bool
	// ctxVerdicts maps leafKey to the verdict of the contextual tuple matching that leaf. Nil when
	// !hasCtxTuples.
	ctxVerdicts map[string]bool
}

func (w *walker) leafFrom(edge *graph.WeightedAuthorizationModelEdge) leaf {
	return leaf{
		relation:   tuple.GetRelation(edge.GetRelationDefinition()),
		wildcard:   edge.GetTo().GetNodeType() == graph.SpecificTypeWildcard,
		conditions: edge.GetConditions(),
	}
}

// relKeyOf keys wildcard leaves separately only when alsoQueriesWildcard; otherwise the
// WHERE admits a single subject ID, so the relation alone identifies the rows.
func (w *walker) relKeyOf(l leaf) relKey {
	return relKey{relation: l.relation, wildcard: w.alsoQueriesWildcard && l.wildcard}
}

// relationFilter matches rows for k, ANDed with rest. It pins the subject ID when alsoQueriesWildcard,
// so a stale tuple (e.g., a wildcard left over from an older model) can't satisfy the wrong leaf.
func (w *walker) relationFilter(k relKey, rest ...query.Predicate) query.Predicate {
	rel := query.Eq(w.table.ObjectRelation(), query.Lit(k.relation))

	if w.alsoQueriesWildcard {
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

// pruned reports whether an edge can't reach the request's subject, mirroring FlattenNode.
func (w *walker) pruned(edge *graph.WeightedAuthorizationModelEdge) bool {
	if _, ok := w.model.GetEdgeWeight(edge, w.subjNodeID); !ok {
		return true
	}
	if w.subjIsWildcard && !slices.Contains(edge.GetWildcards(), w.subjNodeID) {
		return true
	}
	return false
}

// evaluateSubtree combines edges under operation with short-circuiting, delegating leaves to
// evalLeaf.
func (w *walker) evaluateSubtree(ctx context.Context, edges []*graph.WeightedAuthorizationModelEdge, operation string, evalLeaf leafEvaluator) (branchResult, error) {
	switch operation {
	case graph.UnionOperator:
		var preds []*query.Predicate
		for _, edge := range edges {
			r, err := w.evaluateEdge(ctx, edge, evalLeaf)
			if err != nil {
				return branchResult{}, err
			}
			switch r.state {
			case branchTrue:
				return branchResult{state: branchTrue}, nil
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
				return branchResult{}, err
			}
			switch r.state {
			case branchFalse:
				return branchResult{state: branchFalse}, nil
			case branchNeedsQuery:
				preds = append(preds, r.pred)
			default: // branchTrue contributes nothing to an intersection.
			}
		}
		return combinePreds(preds, branchTrue, andPred), nil
	case graph.ExclusionOperator:
		w.sawNonUnion = true
		if w.subjIsWildcard {
			return branchResult{}, ErrWildcardInvalidRequest
		}
		if len(edges) != 2 {
			return branchResult{}, ErrPanicRequest
		}
		if w.subjRel != "" {
			if _, ok := w.model.GetEdgeWeight(edges[1], w.subjNodeID); !ok {
				return branchResult{}, ErrUsersetInvalidRequest
			}
		}
		base, err := w.evaluateEdge(ctx, edges[0], evalLeaf)
		if err != nil {
			return branchResult{}, err
		}
		if base.state == branchFalse {
			return branchResult{state: branchFalse}, nil
		}
		subtract, err := w.evaluateEdge(ctx, edges[1], evalLeaf)
		if err != nil {
			return branchResult{}, err
		}
		if subtract.state == branchTrue {
			return branchResult{state: branchFalse}, nil
		}
		if subtract.state == branchFalse {
			return base, nil
		}
		if base.state == branchTrue {
			return branchResult{state: branchNeedsQuery, pred: notPred(subtract.pred)}, nil
		}
		return branchResult{state: branchNeedsQuery, pred: andPred(base.pred, notPred(subtract.pred))}, nil
	default:
		return branchResult{}, ErrPanicRequest
	}
}

func (w *walker) evaluateEdge(ctx context.Context, edge *graph.WeightedAuthorizationModelEdge, evalLeaf leafEvaluator) (branchResult, error) {
	if w.pruned(edge) {
		return branchResult{state: branchFalse}, nil
	}
	switch edge.GetEdgeType() {
	case graph.DirectEdge:
		// A direct edge always terminates at a type node, which is a weight-1 leaf.
		return evalLeaf(ctx, w.leafFrom(edge))
	case graph.ComputedEdge, graph.DirectLogicalEdge, graph.TTULogicalEdge, graph.RewriteEdge:
		return w.evaluateNode(ctx, edge.GetTo(), evalLeaf)
	default:
		return branchResult{}, ErrPanicRequest
	}
}

// evaluateNode combines a node's edges by its operator, or by union for a non-operator node.
func (w *walker) evaluateNode(ctx context.Context, node *graph.WeightedAuthorizationModelNode, evalLeaf leafEvaluator) (branchResult, error) {
	edges, ok := w.model.GetEdgesFromNode(node)
	if !ok {
		return branchResult{}, ErrPanicRequest
	}
	operation := graph.UnionOperator
	if node.GetNodeType() == graph.OperatorNode {
		operation = node.GetLabel()
	}
	return w.evaluateSubtree(ctx, edges, operation, evalLeaf)
}

// evaluateLeaf is the first-pass evaluator. A matching contextual tuple overrides stored tuples; a
// miss yields branchNeedsQuery, since the tuple may be stored.
func (w *walker) evaluateLeaf(ctx context.Context, l leaf) (branchResult, error) {
	satisfied, matched, err := w.contextLeafSatisfied(ctx, l)
	if err != nil {
		return branchResult{}, err
	}
	if matched {
		// matched implies hasCtxTuples, so ctxVerdicts is non-nil.
		w.ctxVerdicts[leafKey(l)] = satisfied
		if satisfied {
			return branchResult{state: branchTrue}, nil
		}
		return branchResult{state: branchFalse}, nil
	}
	w.accumulateLeafConditions(l)
	count := query.Count(query.Lit[int64](1), query.AggFilter(w.relationFilter(w.relKeyOf(l))))
	pred := query.Gt(count, query.Lit[int64](0))
	return branchResult{state: branchNeedsQuery, pred: &pred}, nil
}

// contextLeafSatisfied reports whether a contextual tuple matches the leaf and, if so, whether it
// passes the leaf's condition. Unconditioned leaves need no special case: NoCond ("") matches an
// unconditioned tuple's empty condition name in evaluateCondition.
func (w *walker) contextLeafSatisfied(ctx context.Context, l leaf) (satisfied, matched bool, err error) {
	if !w.hasCtxTuples {
		return false, false, nil
	}
	var cts []*openfgav1.TupleKey
	var ok bool
	if l.wildcard {
		cts, ok = w.req.GetContextualTuplesByObjectID(w.req.GetTupleKey().GetObject(), l.relation, w.subjNodeID)
	} else {
		cts, ok = w.req.GetContextualTuplesByUserID(w.req.GetTupleKey().GetUser(), l.relation, w.objectType)
	}
	if !ok {
		return false, false, nil
	}
	for _, ct := range cts {
		if l.wildcard {
			if !tuple.IsTypedWildcard(ct.GetUser()) {
				continue
			}
		} else if ct.GetObject() != w.req.GetTupleKey().GetObject() {
			continue
		}
		satisfied, err := evaluateCondition(ctx, w.model, l.conditions, ct, w.req.GetContext())
		if err != nil {
			return false, false, err
		}
		return satisfied, true, nil
	}
	return false, false, nil
}

// leafKey identifies a leaf across passes by every field that affects its contextual-tuple
// verdict. Model identifiers can't contain NUL, so it's a safe separator.
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

// evalUnconditioned evaluates pred in HAVING. HAVING is omitted for a pure union, where any WHERE
// match suffices, so LIMIT 1 can stop at the first row; it's kept when both subject IDs are
// queried, since only pred pins each relation to its subject ID.
func (w *walker) evalUnconditioned(ctx context.Context, pred *query.Predicate) (*Response, error) {
	where := append(w.whereShared(), w.relationIn(), w.unconditionedPred())
	stmt := query.Select(query.Lit[int64](1)).
		From(w.table).
		Where(query.And(where[0], where[1:]...)).
		Limit(1)
	if pred != nil && (w.sawNonUnion || w.alsoQueriesWildcard) {
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

// evalConditioned gathers the candidate rows and re-walks the subtree against them, evaluating
// conditions in-app.
func (w *walker) evalConditioned(ctx context.Context, edges []*graph.WeightedAuthorizationModelEdge, operation string) (*Response, error) {
	// Unreachable: w.conditioned implies a surviving leaf. Guards conditionPairing's clauses[0].
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

	evalLeaf := func(ctx context.Context, l leaf) (branchResult, error) {
		ok, err := w.conditionedLeafSatisfied(ctx, l, gathered)
		if err != nil {
			return branchResult{}, err
		}
		if ok {
			return branchResult{state: branchTrue}, nil
		}
		return branchResult{state: branchFalse}, nil
	}
	res, err := w.evaluateSubtree(ctx, edges, operation, evalLeaf)
	if err != nil {
		return nil, err
	}
	return &Response{Allowed: res.state == branchTrue}, nil
}

// conditionedLeafSatisfied reports whether a contextual tuple or a gathered row satisfies the leaf.
// This pass only visits leaves the first pass visited, so ctxVerdicts fully answers the
// contextual case.
func (w *walker) conditionedLeafSatisfied(ctx context.Context, l leaf, gathered []gatheredRow) (bool, error) {
	if w.hasCtxTuples {
		if satisfied, ok := w.ctxVerdicts[leafKey(l)]; ok {
			return satisfied, nil
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

		// Reusing an earlier leaf's result is sound: a relation's conditions for one target type are
		// merged onto a single edge, so every leaf reaching this row admits the same conditions.
		if !g.evaluated {
			ok, err := w.evaluateRow(ctx, g, l)
			if err != nil {
				return false, err
			}
			g.evaluated, g.satisfied = true, ok
		}
		// Tuples are unique per (object, relation, user), all fixed by now, so no other row can match.
		return g.satisfied, nil
	}
	return false, nil
}

// evaluateRow decodes the row's condition context lazily, only for rows a leaf reaches.
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
	return evaluateCondition(ctx, w.model, l.conditions, tk, w.req.GetContext())
}

func (w *walker) whereShared() []query.Predicate {
	preds := []query.Predicate{
		query.Eq(w.table.Store(), query.Bind(w.req.GetStoreID())),
		query.Eq(w.table.ObjectType(), query.Bind(w.objectType)),
		query.Eq(w.table.ObjectID(), query.Bind(w.objectID)),
		query.Eq(w.table.SubjectType(), query.Bind(w.subjType)),
		query.Eq(w.table.SubjectRelation(), query.Bind(w.subjRel)),
	}
	if w.alsoQueriesWildcard {
		preds = append(preds,
			query.In(w.table.SubjectID(), query.Bind(w.subjID), query.Bind(tuple.Wildcard)),
			query.In(w.table.SubjectKind(), query.Lit(string(tuple.User)), query.Lit(string(tuple.UserSet))),
		)
	} else {
		preds = append(preds,
			query.Eq(w.table.SubjectID(), query.Bind(w.subjID)),
			query.Eq(w.table.SubjectKind(), query.Lit(string(tuple.GetUserTypeFromUser(w.req.GetTupleKey().GetUser())))),
		)
	}
	return preds
}

func (w *walker) unconditionedPred() query.Predicate {
	return query.Or(
		query.IsNull(w.table.Condition()),
		query.Eq(w.table.Condition(), query.Lit(graph.NoCond)),
	)
}

// sortedKeys orders relKeys for deterministic SQL: by relation, exact subject before wildcard.
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

// relationIn renders the surviving relations as `relation IN (...)`. Names come from the model, not
// the request, so they're inlined; the renderer escapes literals.
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

// conditionPairing emits `(relation = r AND <conds for r>) OR ...`. Separate `relation IN` and
// `condition IN` filters would admit a tuple whose condition is only admitted by a different
// relation.
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

// conditionPred admits exactly the condition names in conds, with NoCond admitting unconditioned
// rows. Names are inlined as in relationIn.
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
		preds = append(preds, w.unconditionedPred())
	}

	// Never empty: GetConditions always includes at least NoCond.
	pred := preds[0]
	for _, p := range preds[1:] {
		pred = query.Or(pred, p)
	}
	return pred
}

// combinePreds folds preds with join; no preds yields identity.
func combinePreds(preds []*query.Predicate, identity branchState, join func(a, b *query.Predicate) *query.Predicate) branchResult {
	if len(preds) == 0 {
		return branchResult{state: identity}
	}
	acc := preds[0]
	for _, p := range preds[1:] {
		acc = join(acc, p)
	}
	return branchResult{state: branchNeedsQuery, pred: acc}
}

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
