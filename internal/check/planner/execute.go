package planner

import (
	"context"
	"database/sql"
	"fmt"
	"slices"

	"github.com/openfga/openfga/pkg/storage/adapter"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
)

// ExecuteOption configures a Plan.Execute call.
type ExecuteOption func(*executeOptions)

// executeOptions holds the resolved configuration for a Plan.Execute call.
type executeOptions struct {
	concurrencyLimit int
}

// WithConcurrencyLimit bounds the number of extra goroutines the execution may spawn. When
// the limit is reached, pending work runs inline on the current goroutine rather than
// blocking, so the bound can never deadlock — including when a leaf's execution itself fans
// out into more sub-plan executions. A limit <= 0 means unbounded.
func WithConcurrencyLimit(limit int) ExecuteOption {
	return func(o *executeOptions) { o.concurrencyLimit = limit }
}

// ConditionEvaluator decides whether an ABAC condition attached to a matched tuple is
// satisfied for the current request. Name is the stored condition name (empty for an
// unconditioned tuple, which always passes) and context is its encoded condition context.
// The caller supplies an evaluator wired to the request context and the model's CEL
// environment; CEL evaluation deliberately lives outside this package.
type ConditionEvaluator interface {
	Eval(ctx context.Context, name string, context []byte) (bool, error)
}

// Execute runs the plan's single query and returns whether the bound Check subject is
// granted the relation.
//
// A condition-free plan folds its whole set algebra in the database: the query returns a
// row iff the Check is granted, so Execute only checks for a row. A plan that mentions an
// ABAC condition gathers its candidate tuples in one scan; Execute then evaluates each
// matched condition with eval and folds the union / intersection / exclusion over the
// per-leaf results.
func (p *Plan) Execute(ctx context.Context, eval ConditionEvaluator, opts ...ExecuteOption) (bool, error) {
	options := executeOptions{concurrencyLimit: defaultConcurrencyLimit}
	for _, opt := range opts {
		opt(&options)
	}

	switch p.unit.kind {
	case unitFalse:
		// Unreachable subject type: trivially false, no query to run.
		return false, nil

	case unitHaving:
		// The database decided the set algebra; a returned row means granted.
		rows, err := p.unit.query.Execute(ctx)
		if err != nil {
			return false, fmt.Errorf("planner: executing check query: %w", err)
		}
		defer rows.Close()
		granted := rows.Next()
		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("planner: iterating check query: %w", err)
		}
		return granted, nil

	case unitGather:
		return p.executeGather(ctx, eval)

	case unitMulti:
		// One limiter is shared across the whole multi-query execution (and any sub-plan
		// fan-out it later drives), so the limit bounds total extra goroutines rather than
		// being reset per fan-out.
		lim, lctx := newLimiter(ctx, options.concurrencyLimit)
		return p.executeMulti(lctx, eval, lim)

	default:
		return false, fmt.Errorf("planner: unknown unit kind %d", p.unit.kind)
	}
}

// gatherRow is one candidate tuple from the gather scan, in the order buildGatherQuery
// projects: relation, subject id, condition name, condition context.
type gatherRow struct {
	relation  string
	subjectID string
	condName  string
	condCtx   []byte
}

// executeGather runs the gather scan, attributes each row to the leaves it satisfies
// (evaluating CEL on conditioned rows), then folds the operator tree over per-leaf
// satisfaction.
func (p *Plan) executeGather(ctx context.Context, eval ConditionEvaluator) (bool, error) {
	gathered, err := scanGather(ctx, p.unit.query)
	if err != nil {
		return false, err
	}

	satisfied := make(map[Node]bool, len(p.unit.leaves))
	condCache := make(map[keys.Key]bool)
	for _, leaf := range p.unit.leaves {
		ok, err := leafSatisfied(ctx, leaf, gathered, eval, condCache)
		if err != nil {
			return false, err
		}
		satisfied[leaf] = ok
	}

	return fold(p.Root, satisfied), nil
}

// leafSatisfied reports whether any gathered row grants leaf: the row must match the
// leaf's relation and subject, and either be unconditioned (when the leaf accepts
// unconditioned tuples) or carry an accepted condition whose CEL evaluation passes.
func leafSatisfied(ctx context.Context, leaf *QueryNode, rows []gatherRow, eval ConditionEvaluator, cache map[keys.Key]bool) (bool, error) {
	for _, r := range rows {
		if r.relation != leaf.Relation {
			continue
		}
		if !leafMatchesSubject(leaf, r.subjectID) {
			continue
		}

		if r.condName == "" {
			// Unconditioned tuple: grants iff the leaf accepts unconditioned subjects.
			if leafAcceptsCondition(leaf, "") {
				return true, nil
			}
			continue
		}

		// Conditioned tuple: the leaf must accept this condition and CEL must pass.
		if !leafAcceptsCondition(leaf, r.condName) {
			continue
		}
		ok, err := evalCondition(ctx, eval, leaf, r, cache)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
	}
	return false, nil
}

// leafMatchesSubject reports whether a row's subject id is the leaf's subject — the exact
// bound id, or the public-access wildcard for a non-userset subject. A wildcard-terminal
// leaf (SubjectID "*") matches only the wildcard tuple.
func leafMatchesSubject(leaf *QueryNode, subjectID string) bool {
	if subjectID == leaf.SubjectID {
		return true
	}
	return leaf.SubjectRelation == "" && leaf.SubjectID != wildcardID && subjectID == wildcardID
}

// leafAcceptsCondition reports whether name is among the conditions the leaf's edge
// accepts ("" is the unconditioned sentinel).
func leafAcceptsCondition(leaf *QueryNode, name string) bool {
	return slices.Contains(leaf.Conditions, name)
}

// evalCondition evaluates a conditioned row's CEL, caching by (name, context) so a
// condition shared across leaves or rows is evaluated once.
func evalCondition(ctx context.Context, eval ConditionEvaluator, leaf *QueryNode, r gatherRow, cache map[keys.Key]bool) (bool, error) {
	if eval == nil {
		return false, fmt.Errorf("planner: tuple for %q carries condition %q but no evaluator was provided", leaf.Label, r.condName)
	}
	return evalNamedCondition(ctx, eval, r.condName, r.condCtx, cache)
}

// evalNamedCondition evaluates a condition by name and encoded context, caching by
// (name, context) so a condition shared across rows is evaluated once. An empty name is an
// unconditioned tuple, which always passes.
func evalNamedCondition(ctx context.Context, eval ConditionEvaluator, name string, condCtx []byte, cache map[keys.Key]bool) (bool, error) {
	if name == "" {
		return true, nil
	}
	if eval == nil {
		return false, fmt.Errorf("planner: tuple carries condition %q but no evaluator was provided", name)
	}
	builder := keys.GetBuilder()
	defer builder.Close()
	builder.EncodeString(name)
	builder.EncodeBytes(condCtx)
	key := builder.Key()
	if got, ok := cache[key]; ok {
		return got, nil
	}
	ok, err := eval.Eval(ctx, name, condCtx)
	if err != nil {
		return false, fmt.Errorf("planner: evaluating condition %q: %w", name, err)
	}
	cache[key] = ok
	return ok, nil
}

// leafOutcome is one leaf query's result, delivered over the outcomes channel: its fold-key
// node, its boolean grant, and any error. Errors and successes both flow through the channel
// (not the errgroup) so executeMulti can fold incrementally and short-circuit.
type leafOutcome struct {
	node Node
	ok   bool
	err  error
}

// executeMulti runs the plan's leaf queries under the shared limiter, folding the operator
// tree incrementally so it returns — and terminates the remaining queries — as soon as the
// result is determined. It is the execution form for any plan containing a weight-2 join:
// weight-1 leaves and weight-2 joins each run as their own query, concurrently up to the limit
// and inline beyond it.
//
// Short-circuiting works at every level of the tree, not just the root: once a sub-operation
// settles (e.g. a union yields true, or an intersection yields false), the queries feeding
// only that sub-operation are cancelled immediately, even while the root is still undecided.
// The bound is preserved (lim.run never blocks) and no goroutine outlives Execute: on return,
// the shared context is cancelled and every scheduled goroutine is reaped via lim.wait.
func (p *Plan) executeMulti(ctx context.Context, eval ConditionEvaluator, lim *limiter) (bool, error) {
	multi := p.unit.multi
	outcomes := make(chan leafOutcome, len(multi)) // buffered: unread sends never block

	// units is the set of fold-key nodes (one query each); decide and pendingNeeded must treat
	// them opaquely rather than recursing into a merged weight-1 region's leaves.
	units := make(map[Node]struct{}, len(multi))
	for _, lq := range multi {
		units[lq.node] = struct{}{}
	}

	satisfied := make(map[Node]bool, len(multi))
	cancels := make(map[Node]context.CancelFunc, len(multi)) // per-unit cancel
	cancelled := make(map[Node]struct{})                     // units we cancelled on purpose
	var firstErr error
	decided, scheduled, collected := false, 0, 0
	var result bool

	// cancelDeadUnits cancels every scheduled-but-unfinished unit that can no longer affect the
	// result, so a settled sub-operation stops its queries even while the root is undecided.
	cancelDeadUnits := func() {
		need := make(map[Node]struct{})
		pendingNeeded(p.unit.multiRoot, satisfied, units, need)
		for node, cancel := range cancels {
			if _, live := need[node]; live {
				continue
			}
			if _, done := satisfied[node]; done {
				continue
			}
			if _, already := cancelled[node]; already {
				continue
			}
			cancelled[node] = struct{}{}
			cancel() // terminates that unit's in-flight query
		}
	}

	consume := func(o leafOutcome) {
		collected++
		if _, deliberate := cancelled[o.node]; deliberate {
			return // we cancelled this unit; its ctx.Err() is not a real error or result
		}
		if o.err != nil {
			if firstErr == nil {
				firstErr = o.err // first error; swallowed if a decision is later reached
			}
			return
		}
		satisfied[o.node] = o.ok
		if v, ok := decide(p.unit.multiRoot, satisfied, units); ok {
			decided, result = true, v
		} else {
			cancelDeadUnits() // settle sub-operations: cancel units no longer needed
		}
	}

	drainReady := func() { // consume finished leaves without blocking
		for !decided {
			select {
			case o := <-outcomes:
				consume(o)
			default:
				return
			}
		}
	}

	// Schedule leaves until all are scheduled or the outcome is decided. Draining ready
	// outcomes before each schedule lets an already-finished decisive leaf stop us before we
	// start (and possibly run inline) more work.
	for i := 0; i < len(multi) && !decided; i++ {
		drainReady()
		if decided {
			break
		}
		lq := multi[i]
		leafCtx, cancel := context.WithCancel(ctx)
		cancels[lq.node] = cancel
		_ = lim.run(func() error {
			ok, err := p.runLeaf(leafCtx, eval, lq, lim)
			outcomes <- leafOutcome{node: lq.node, ok: ok, err: err}
			return nil // outcomes flow through the channel, not the errgroup
		})
		scheduled++
	}

	// Block for the rest until decided or every scheduled leaf has reported.
	for !decided && collected < scheduled {
		consume(<-outcomes)
	}

	// Terminate any remaining in-flight queries and reap, so no goroutine outlives Execute.
	lim.stop()
	_ = lim.wait()

	switch {
	case decided:
		return result, nil // a definitive result swallows any leaf error
	case firstErr != nil:
		return false, firstErr // undecided only because an errored leaf left the fold unknown
	default:
		return fold(p.unit.multiRoot, satisfied), nil // all leaves known: fully determined
	}
}

// runLeaf resolves a single leaf query according to its kind: a boolean existence check, a
// single-leaf gather with CEL, or a self-join gather with CEL across the hops. The shared
// limiter is threaded in (currently unused) so a future leaf that fans out into sub-plan
// executions resolves them under the same deadlock-free bound rather than a fresh one.
func (p *Plan) runLeaf(ctx context.Context, eval ConditionEvaluator, lq leafQuery, _ *limiter) (bool, error) {
	switch lq.kind {
	case leafBool:
		return rowExists(ctx, lq.query)
	case leafGather:
		if lq.subLeaves != nil {
			// A merged conditioned weight-1 region: gather its rows once, attribute them to the
			// region's leaves, then fold the region subtree.
			return p.executeRegionGather(ctx, eval, lq)
		}
		leaf, ok := lq.node.(*QueryNode)
		if !ok {
			return false, fmt.Errorf("planner: gather leaf is %T, want *QueryNode", lq.node)
		}
		return p.executeLeafGather(ctx, eval, leaf, lq.query)
	case leafJoinGather:
		join, ok := lq.node.(*JoinNode)
		if !ok {
			return false, fmt.Errorf("planner: join-gather leaf is %T, want *JoinNode", lq.node)
		}
		return executeJoinGather(ctx, eval, join, lq.query)
	default:
		return false, fmt.Errorf("planner: unknown leaf kind %d", lq.kind)
	}
}

// rowExists runs a boolean existence query and reports whether it returned a row.
func rowExists(ctx context.Context, query adapter.Query) (bool, error) {
	rows, err := query.Execute(ctx)
	if err != nil {
		return false, fmt.Errorf("planner: executing leaf query: %w", err)
	}
	defer rows.Close()
	granted := rows.Next()
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("planner: iterating leaf query: %w", err)
	}
	return granted, nil
}

// executeLeafGather runs a conditioned weight-1 leaf's single-leaf gather scan and reports
// whether any gathered row satisfies it, evaluating CEL on conditioned rows.
func (p *Plan) executeLeafGather(ctx context.Context, eval ConditionEvaluator, leaf *QueryNode, query adapter.Query) (bool, error) {
	gathered, err := scanGather(ctx, query)
	if err != nil {
		return false, err
	}
	return leafSatisfied(ctx, leaf, gathered, eval, make(map[keys.Key]bool))
}

// executeRegionGather runs a merged conditioned weight-1 region's single gather scan, attributes
// the rows to each of the region's leaves (evaluating CEL on conditioned rows), then folds the
// region subtree (lq.node) over the per-leaf results. It is the in-process analogue of a
// top-level unitGather, but for a region nested beside weight-2 joins in a unitMulti plan.
func (p *Plan) executeRegionGather(ctx context.Context, eval ConditionEvaluator, lq leafQuery) (bool, error) {
	gathered, err := scanGather(ctx, lq.query)
	if err != nil {
		return false, err
	}

	satisfied := make(map[Node]bool, len(lq.subLeaves))
	condCache := make(map[keys.Key]bool)
	for _, leaf := range lq.subLeaves {
		ok, err := leafSatisfied(ctx, leaf, gathered, eval, condCache)
		if err != nil {
			return false, err
		}
		satisfied[leaf] = ok
	}
	return fold(lq.node, satisfied), nil
}

// scanGather runs a gather scan and reads its rows into gatherRow values, in the column
// order buildGatherQuery projects: relation, subject id, condition name, condition context.
func scanGather(ctx context.Context, query adapter.Query) ([]gatherRow, error) {
	rows, err := query.Execute(ctx)
	if err != nil {
		return nil, fmt.Errorf("planner: executing check query: %w", err)
	}
	defer rows.Close()

	gathered := make([]gatherRow, 0)
	for rows.Next() {
		var relation, subjectID, name sql.NullString
		var condCtx []byte
		if err := rows.Scan(&relation, &subjectID, &name, &condCtx); err != nil {
			return nil, fmt.Errorf("planner: scanning check query: %w", err)
		}
		gathered = append(gathered, gatherRow{
			relation:  relation.String,
			subjectID: subjectID.String,
			condName:  name.String,
			condCtx:   condCtx,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("planner: iterating check query: %w", err)
	}
	return gathered, nil
}

// executeJoinGather runs a conditioned weight-2 self-join scan and reports whether any
// intermediate object satisfies the hop. The scan returns every joined (hop-1, hop-2) tuple
// pair that survived the SQL prune; the executor groups them by intermediate object and, for
// each object, folds the hop-2 subtree over the rows whose hop-1 and hop-2 conditions pass
// CEL. The hop resolves iff some intermediate object's fold is true — so a hop-2
// intersection is satisfied only when a single object carries every operand.
func executeJoinGather(ctx context.Context, eval ConditionEvaluator, join *JoinNode, query adapter.Query) (bool, error) {
	rows, err := query.Execute(ctx)
	if err != nil {
		return false, fmt.Errorf("planner: executing join query: %w", err)
	}
	defer rows.Close()

	// Group the gather rows by intermediate object, keeping insertion order so the fold is
	// deterministic. Each object's rows are the hop-2 candidate tuples for that object.
	order := make([]string, 0)
	byObject := make(map[string][]gatherRow)
	hop1Pass := make(map[string]bool) // intermediate id → hop-1 condition satisfied for this object
	cache := make(map[keys.Key]bool)

	for rows.Next() {
		var intermediateID, hop1Cond, hop2Rel, subjectID, hop2Cond sql.NullString
		var hop1CondCtx, hop2CondCtx []byte
		if err := rows.Scan(&intermediateID, &hop1Cond, &hop1CondCtx, &hop2Rel, &subjectID, &hop2Cond, &hop2CondCtx); err != nil {
			return false, fmt.Errorf("planner: scanning join query: %w", err)
		}
		id := intermediateID.String
		if _, seen := byObject[id]; !seen {
			order = append(order, id)
		}

		// The hop-1 condition is a property of the (bound object → intermediate object) link,
		// so it gates the whole object. Evaluate it once per object; a failing link removes the
		// object from consideration entirely.
		if !hop1Pass[id] {
			ok, err := evalNamedCondition(ctx, eval, hop1Cond.String, hop1CondCtx, cache)
			if err != nil {
				return false, err
			}
			if ok {
				hop1Pass[id] = true
			} else {
				continue
			}
		}

		// Evaluate the hop-2 condition; only rows whose grant actually holds contribute to the
		// object's hop-2 fold.
		hop2OK, err := evalNamedCondition(ctx, eval, hop2Cond.String, hop2CondCtx, cache)
		if err != nil {
			return false, err
		}
		if !hop2OK {
			continue
		}
		byObject[id] = append(byObject[id], gatherRow{
			relation:  hop2Rel.String,
			subjectID: subjectID.String,
			condName:  hop2Cond.String,
			condCtx:   hop2CondCtx,
		})
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("planner: iterating join query: %w", err)
	}

	leaves := collectLeaves(join.Hop2)
	for _, id := range order {
		if !hop1Pass[id] {
			continue
		}
		if hop2SubtreeSatisfied(join.Hop2, leaves, byObject[id]) {
			return true, nil
		}
	}
	return false, nil
}

// hop2SubtreeSatisfied folds the hop-2 subtree for one intermediate object: it marks which
// leaves a granting row satisfies (the rows are already CEL-passed and condition-accepting),
// then folds the operator tree. This is the in-process analogue of buildJoinBoolQuery's
// per-object HAVING fold.
func hop2SubtreeSatisfied(hop2 Node, leaves []*QueryNode, rows []gatherRow) bool {
	satisfied := make(map[Node]bool, len(leaves))
	for _, leaf := range leaves {
		satisfied[leaf] = leafMatchedByRow(leaf, rows)
	}
	return fold(hop2, satisfied)
}

// leafMatchedByRow reports whether any of an object's granting rows is attributable to the
// leaf: same relation, a condition the leaf accepts, and a matching subject. CEL was already
// applied when the rows were gathered, so a matching row is a genuine grant; the condition
// check still distinguishes two leaves that share a relation under different conditions.
func leafMatchedByRow(leaf *QueryNode, rows []gatherRow) bool {
	for _, r := range rows {
		if r.relation != leaf.Relation {
			continue
		}
		if !leafAcceptsCondition(leaf, r.condName) {
			continue
		}
		if leafMatchesSubject(leaf, r.subjectID) {
			return true
		}
	}
	return false
}

// collectLeaves returns every QueryNode in the tree (pre-order).
func collectLeaves(n Node) []*QueryNode {
	switch node := n.(type) {
	case *QueryNode:
		return []*QueryNode{node}
	case *CombineNode:
		var out []*QueryNode
		for _, c := range node.Children {
			out = append(out, collectLeaves(c)...)
		}
		return out
	default:
		return nil
	}
}

// fold reduces the plan tree to a boolean using the satisfaction results, keyed by Node. A key
// may be a leaf (QueryNode or JoinNode) or, in a unitMulti plan, the root of a merged weight-1
// region whose boolean was computed by a single query: when the node itself has a precomputed
// result, fold returns it directly rather than recursing, since the region's children were never
// queried individually. This is what lets executeMulti fold the rewritten tree (multiRoot) whose
// leaves are the compiled units.
func fold(n Node, satisfied map[Node]bool) bool {
	if v, ok := satisfied[n]; ok {
		return v
	}
	switch node := n.(type) {
	case *QueryNode:
		return satisfied[node]
	case *JoinNode:
		return satisfied[node]
	case *CombineNode:
		switch node.Op {
		case CombineUnion:
			// An empty union (unreachable subject type) is false.
			for _, c := range node.Children {
				if fold(c, satisfied) {
					return true
				}
			}
			return false
		case CombineIntersect:
			if len(node.Children) == 0 {
				return false
			}
			for _, c := range node.Children {
				if !fold(c, satisfied) {
					return false
				}
			}
			return true
		case CombineExcept:
			// base BUT NOT subtract; built with exactly two children.
			if len(node.Children) != 2 {
				return false
			}
			return fold(node.Children[0], satisfied) && !fold(node.Children[1], satisfied)
		default:
			return false
		}
	default:
		return false
	}
}

// decide is the three-valued companion to fold: it folds the operator tree from the unit
// results known so far and reports (value, true) when the outcome is already determined
// regardless of the still-unknown units, or (_, false) when more results are needed. A node
// present in satisfied returns its value directly; a unit absent from satisfied is unknown.
//
// The units argument is the set of fold-key nodes — one query each — which decide must treat
// opaquely: a merged weight-1 region is itself a *CombineNode unit whose children are never
// reported individually, so decide must not recurse into it (fold avoids this only because every
// unit is already in satisfied by fold-time). Recursion descends only through the structural
// operators that are not units. With every unit known, decide agrees with fold, so
// short-circuiting can only return sooner — never disagree.
func decide(n Node, satisfied map[Node]bool, units map[Node]struct{}) (val bool, known bool) {
	if v, ok := satisfied[n]; ok {
		return v, true
	}
	if _, isUnit := units[n]; isUnit {
		// An unreported unit (leaf or merged-region root): its value is not yet known.
		return false, false
	}
	node, ok := n.(*CombineNode)
	if !ok {
		return false, false
	}
	switch node.Op {
	case CombineUnion:
		// Any known-true child decides true; otherwise true only once every child is known false.
		allKnown := true
		for _, c := range node.Children {
			v, k := decide(c, satisfied, units)
			if k && v {
				return true, true
			}
			if !k {
				allKnown = false
			}
		}
		if allKnown {
			return false, true // empty or all-false union
		}
		return false, false
	case CombineIntersect:
		if len(node.Children) == 0 {
			return false, true // empty intersection is false, matching fold
		}
		// Any known-false child decides false; otherwise true only once every child is known true.
		allKnown := true
		for _, c := range node.Children {
			v, k := decide(c, satisfied, units)
			if k && !v {
				return false, true
			}
			if !k {
				allKnown = false
			}
		}
		if allKnown {
			return true, true
		}
		return false, false
	case CombineExcept:
		// base BUT NOT subtract; built with exactly two children.
		if len(node.Children) != 2 {
			return false, true // malformed, matching fold's false
		}
		baseV, baseK := decide(node.Children[0], satisfied, units)
		if baseK && !baseV {
			return false, true // base false decides false regardless of subtract
		}
		subV, subK := decide(node.Children[1], satisfied, units)
		if subK && subV {
			return false, true // subtract true decides false regardless of base
		}
		if baseK && subK {
			return baseV && !subV, true
		}
		return false, false
	default:
		return false, true
	}
}

// pendingNeeded adds to need every still-unknown fold-key node (a leaf, or a merged weight-1
// region root) whose value can still change the overall result, given what is known. A subtree
// that decide already settles contributes nothing — its unknown leaves are dead and may be
// cancelled — and the children of an already-settled operator are pruned entirely. This is the
// liveness companion to decide that lets executeMulti cancel a settled sub-operation's queries
// even while the root is undecided.
func pendingNeeded(n Node, satisfied map[Node]bool, units map[Node]struct{}, need map[Node]struct{}) {
	if _, ok := satisfied[n]; ok {
		return // this node's value is known; nothing under it is pending
	}
	if _, isUnit := units[n]; isUnit {
		need[n] = struct{}{} // an unreported unit that is still live
		return
	}
	node, ok := n.(*CombineNode)
	if !ok {
		return
	}
	if _, done := decide(node, satisfied, units); done {
		return // operator already settled: none of its unknown children matter
	}
	for _, c := range node.Children {
		pendingNeeded(c, satisfied, units, need)
	}
}
