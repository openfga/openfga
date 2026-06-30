package planner

import (
	"slices"

	"github.com/openfga/openfga/pkg/storage/adapter"
)

// wildcardID is the object id of a public-access (type-bound wildcard) tuple, e.g. the
// "*" in user:*.
const wildcardID = "*"

// bound is the constant Check coordinates shared by every leaf of a plan: the store, the
// bound object, and the bound subject. Only the per-leaf relation, accepted conditions,
// and (for wildcard terminals) subject id vary across the tree.
type bound struct {
	store           string
	objectType      string
	objectID        string
	subjectType     string
	subjectID       string
	subjectRelation string
}

// sharedWherePredicates are the WHERE conditions common to both query shapes: the store,
// the bound object, the bound subject type / relation, and a superset bound on subject id
// (the exact id, plus the public-access wildcard for a non-userset subject). The
// per-leaf relation and condition filters are applied above this, in HAVING FILTER atoms
// or in the gather disjuncts.
func sharedWherePredicates(b adapter.Builder, t adapter.Tuple, bnd bound) []adapter.Predicate {
	preds := []adapter.Predicate{
		t.Store().Eq(b.Lit(bnd.store)),
		t.ObjectType().Eq(b.Lit(bnd.objectType)),
		t.ObjectID().Eq(b.Lit(bnd.objectID)),
	}
	return append(preds, subjectBound(b, t, bnd)...)
}

// subjectBound is the subject portion of sharedWherePredicates for an arbitrary tuple
// alias: the bound subject's type and relation, and a superset bound on its id (the exact
// id, plus the public-access wildcard for a non-userset subject). It is reused on the
// hop-2 side of a weight-2 self-join, where the object columns differ from the bound object
// but the subject is still the Check subject.
func subjectBound(b adapter.Builder, t adapter.Tuple, bnd bound) []adapter.Predicate {
	subjectIDs := []adapter.Expression{b.Lit(bnd.subjectID)}
	if bnd.subjectRelation == "" {
		subjectIDs = append(subjectIDs, b.Lit(wildcardID))
	}
	return []adapter.Predicate{
		t.SubjectType().Eq(b.Lit(bnd.subjectType)),
		t.SubjectRelation().Eq(b.Lit(bnd.subjectRelation)),
		t.SubjectID().In(subjectIDs...),
	}
}

// unconditionedPred matches a tuple that carries no ABAC condition. An unconditioned
// tuple stores the condition name as NULL or the empty string, so both are admitted (this
// mirrors the executor's row scan, which treats an empty / NULL name as an unconditioned
// grant).
func unconditionedPred(b adapter.Builder, t adapter.Tuple) adapter.Predicate {
	return t.Condition().IsNull().Or(t.Condition().Eq(b.Lit("")))
}

// leafSubjectMatch is the per-leaf subject-id test: the leaf's exact subject id, plus the
// public-access wildcard for a non-userset subject. A wildcard terminal leaf (subject id
// "*") matches only the wildcard tuple.
func leafSubjectMatch(b adapter.Builder, t adapter.Tuple, leaf *QueryNode) adapter.Predicate {
	m := t.SubjectID().Eq(b.Lit(leaf.SubjectID))
	if leaf.SubjectRelation == "" && leaf.SubjectID != wildcardID {
		m = m.Or(t.SubjectID().Eq(b.Lit(wildcardID)))
	}
	return m
}

// falsePredicate is a constant-false predicate ("1 = 0"), used for an empty union (an
// unreachable subject type) nested inside a condition-free HAVING clause.
func falsePredicate(b adapter.Builder) adapter.Predicate {
	return b.Lit(1).Eq(b.Lit(0))
}

// leafCountAtom is the HAVING atom for a condition-free leaf: a count of the rows that
// match the leaf's relation and subject and carry no condition, compared ">= 1" — i.e.
// the leaf grants. The count is COUNT(CASE WHEN <match> THEN 1 END): the searched CASE has
// no ELSE, so a non-matching row yields NULL and COUNT skips it. This is equivalent to
// COUNT(*) FILTER (WHERE <match>) but uses only ANSI core constructs (CASE + COUNT) that
// every dialect supports, rather than the SQL:2003 FILTER clause MySQL/SQLite lack. The
// count runs over the single grand-total group (the query has no GROUP BY), so ">= 1"
// means at least one matching tuple exists; combineHaving negates this atom with NOT to
// express the "= 0" subtract side of an exclusion.
func leafCountAtom(b adapter.Builder, t adapter.Tuple, leaf *QueryNode) adapter.Predicate {
	match := t.ObjectRelation().Eq(b.Lit(leaf.Relation)).
		And(leafSubjectMatch(b, t, leaf)).
		And(unconditionedPred(b, t))
	counted := b.Case().When(match, b.Lit(1))
	return b.Aggregate(adapter.AggCount, counted).Gte(b.Lit(1))
}

// combineHaving folds a condition-free subtree into a single HAVING predicate over the
// per-leaf count atoms, combining them with OR for a union, AND for an intersection, and
// "base AND NOT subtract" for an exclusion. An empty union is constant-false.
func combineHaving(b adapter.Builder, t adapter.Tuple, n Node) adapter.Predicate {
	switch node := n.(type) {
	case *QueryNode:
		return leafCountAtom(b, t, node)
	case *CombineNode:
		switch node.Op {
		case CombineUnion:
			if len(node.Children) == 0 {
				return falsePredicate(b)
			}
			preds := childHaving(b, t, node.Children)
			return preds[0].Or(preds[1:]...)
		case CombineIntersect:
			if len(node.Children) == 0 {
				// An empty intersection is false, matching fold and decide.
				return falsePredicate(b)
			}
			preds := childHaving(b, t, node.Children)
			return preds[0].And(preds[1:]...)
		case CombineExcept:
			// base BUT NOT subtract; built with exactly two children.
			base := combineHaving(b, t, node.Children[0])
			subtract := combineHaving(b, t, node.Children[1])
			return base.And(subtract.Not())
		}
	}
	return falsePredicate(b)
}

// childHaving maps combineHaving over a node's children.
func childHaving(b adapter.Builder, t adapter.Tuple, children []Node) []adapter.Predicate {
	preds := make([]adapter.Predicate, len(children))
	for i, c := range children {
		preds[i] = combineHaving(b, t, c)
	}
	return preds
}

// relationFilter bounds the scan to the relations the plan's leaves actually reference,
// rendering as an equality for a single relation or an IN list for several. The HAVING
// atoms already test t.relation per leaf, so this predicate changes no result, because a
// row for an unreferenced relation contributes NULL to every count atom regardless. It
// exists only to prune the scan: relation is a stored column, so a composite index keyed
// on the store, object, and relation columns can use it to narrow the range, whereas the
// SUBSTRING- and POSITION-wrapped subject predicates are non-sargable.
//
// The relation set is taken from every leaf, including the subtract side of an exclusion,
// since collectLeaves returns those, so a subtract leaf's relation is never dropped here,
// which would otherwise force its count to zero and silently disable the exclusion.
func relationFilter(b adapter.Builder, t adapter.Tuple, root Node) adapter.Predicate {
	seen := make(map[string]struct{})
	var relations []string
	for _, leaf := range collectLeaves(root) {
		if _, ok := seen[leaf.Relation]; ok {
			continue
		}
		seen[leaf.Relation] = struct{}{}
		relations = append(relations, leaf.Relation)
	}
	// Sorted for deterministic SQL regardless of leaf traversal order.
	slices.Sort(relations)

	if len(relations) == 1 {
		return t.ObjectRelation().Eq(b.Lit(relations[0]))
	}
	lits := make([]adapter.Expression, len(relations))
	for i, r := range relations {
		lits[i] = b.Lit(r)
	}
	return t.ObjectRelation().In(lits...)
}

// buildHavingQuery compiles a condition-free plan into one boolean query: the database
// folds the whole set algebra in HAVING, returning a single row when the Check is granted
// and no rows otherwise.
func buildHavingQuery(b adapter.Builder, bnd bound, root Node) adapter.Query {
	t := b.Tuple("t")
	where := append(sharedWherePredicates(b, t, bnd), relationFilter(b, t, root))
	return b.Select(b.Lit(1)).
		From(t).
		Where(where...).
		Having(combineHaving(b, t, root))
}

// conditionMembership matches a tuple whose condition is one the leaf accepts: the empty
// sentinel admits unconditioned tuples, a named condition admits that name.
func conditionMembership(b adapter.Builder, t adapter.Tuple, conditions []string) adapter.Predicate {
	var pred adapter.Predicate
	for _, c := range conditions {
		var p adapter.Predicate
		if c == "" {
			p = unconditionedPred(b, t)
		} else {
			p = t.Condition().Eq(b.Lit(c))
		}
		if pred == nil {
			pred = p
		} else {
			pred = pred.Or(p)
		}
	}
	if pred == nil {
		return falsePredicate(b)
	}
	return pred
}

// leafDisjunct gathers the candidate tuples for one leaf: its relation plus any condition
// it accepts. Subject matching is left to the shared WHERE bound and to the executor's
// per-row attribution, which distinguishes a wildcard terminal from an exact-id leaf.
func leafDisjunct(b adapter.Builder, t adapter.Tuple, leaf *QueryNode) adapter.Predicate {
	return t.ObjectRelation().Eq(b.Lit(leaf.Relation)).
		And(conditionMembership(b, t, leaf.Conditions))
}

// buildGatherQuery compiles a plan that mentions an ABAC condition into one scan that
// pulls every candidate tuple for the tree's leaves, projecting the relation, subject id,
// and condition columns the executor needs to attribute rows to leaves and evaluate CEL.
func buildGatherQuery(b adapter.Builder, bnd bound, leaves []*QueryNode) adapter.Query {
	t := b.Tuple("t")

	disjuncts := make([]adapter.Predicate, len(leaves))
	for i, leaf := range leaves {
		disjuncts[i] = leafDisjunct(b, t, leaf)
	}
	operandMatch := disjuncts[0].Or(disjuncts[1:]...)

	where := append(sharedWherePredicates(b, t, bnd), operandMatch)
	return b.Select(t.ObjectRelation(), t.SubjectID(), t.Condition(), t.ConditionContext()).
		From(t).
		Where(where...)
}

// leafKindFor selects how a standalone weight-1 leaf resolves: a boolean existence query
// when condition-free, or a single-tuple gather for in-process CEL when conditioned.
func leafKindFor(leaf *QueryNode) leafKind {
	if leaf.ConditionFree() {
		return leafBool
	}
	return leafGather
}

// buildLeafQuery compiles a standalone weight-1 leaf to the query its kind needs: a boolean
// existence query, or the existing single-leaf gather scan for the conditioned case.
func buildLeafQuery(b adapter.Builder, bnd bound, leaf *QueryNode) adapter.Query {
	if leaf.ConditionFree() {
		return buildLeafBoolQuery(b, bnd, leaf)
	}
	return buildGatherQuery(b, bnd, []*QueryNode{leaf})
}

// joinLeafKindFor selects how a weight-2 join resolves: a boolean self-join when both hops
// are condition-free, or a self-join gather for in-process CEL when either hop is
// conditioned.
func joinLeafKindFor(j *JoinNode) leafKind {
	if j.ConditionFree() {
		return leafBool
	}
	return leafJoinGather
}

// buildJoinNodeQuery compiles a weight-2 join to the query its kind needs: a boolean
// self-join when condition-free, or a self-join gather when either hop is conditioned.
func buildJoinNodeQuery(b adapter.Builder, bnd bound, j *JoinNode) adapter.Query {
	if j.ConditionFree() {
		return buildJoinBoolQuery(b, bnd, j)
	}
	return buildJoinGatherQuery(b, bnd, j)
}

// buildLeafBoolQuery compiles a single weight-1 leaf into its own boolean existence query,
// used when a plan mixes weight-1 leaves with weight-2 joins and so cannot fold the whole
// tree into one statement. It returns a row iff a stored tuple grants the leaf, mirroring
// one leafCountAtom but as a standalone scan.
func buildLeafBoolQuery(b adapter.Builder, bnd bound, leaf *QueryNode) adapter.Query {
	t := b.Tuple("t")
	match := t.ObjectRelation().Eq(b.Lit(leaf.Relation)).
		And(leafSubjectMatch(b, t, leaf)).
		And(conditionMembership(b, t, leaf.Conditions))
	where := append(sharedWherePredicates(b, t, bnd), match)
	return b.Select(b.Lit(1)).From(t).Where(where...)
}

// hop1Predicates are the t1 (bound-object) side of a weight-2 self-join: the store, the
// bound object, the hop-1 relation, and the intermediate the hop lands on (its type and the
// hop-1 subject relation — "" for a TTU object, the inner relation for a userset). The
// hop-1 subject id is deliberately unbound: it is the intermediate object, joined to t2.
func hop1Predicates(b adapter.Builder, t1 adapter.Tuple, bnd bound, j *JoinNode) []adapter.Predicate {
	return []adapter.Predicate{
		t1.Store().Eq(b.Lit(bnd.store)),
		t1.ObjectType().Eq(b.Lit(bnd.objectType)),
		t1.ObjectID().Eq(b.Lit(bnd.objectID)),
		t1.ObjectRelation().Eq(b.Lit(j.Hop1Relation)),
		t1.SubjectType().Eq(b.Lit(j.IntermediateType)),
		t1.SubjectRelation().Eq(b.Lit(j.Hop1SubjectRelation)),
	}
}

// hop2BasePredicates are the t2 (intermediate-object) side of a weight-2 self-join common to
// both shapes: the store, the intermediate type, and the bound subject. The per-relation and
// per-condition filtering differs by shape (a relation prune for the boolean fold, a per-leaf
// disjunct for the gather), so the caller appends it. The join (built by the caller) ties
// t2.object_id to t1.subject_id.
func hop2BasePredicates(b adapter.Builder, t2 adapter.Tuple, bnd bound, j *JoinNode) []adapter.Predicate {
	preds := []adapter.Predicate{
		t2.Store().Eq(b.Lit(bnd.store)),
		t2.ObjectType().Eq(b.Lit(j.IntermediateType)),
	}
	return append(preds, subjectBound(b, t2, bnd)...)
}

// buildJoinBoolQuery compiles a condition-free weight-2 hop into one self-join existence
// query: t1 names the intermediate objects reachable from the bound object via the hop-1
// relation, and t2 holds the bound subject's grants on those objects. The hop-2 set algebra
// folds in HAVING over the per-leaf count atoms, exactly as a top-level weight-1 relation
// would (combineHaving), but GROUP BY t2.object_id evaluates it per intermediate object: a
// hop-2 intersection requires one object that satisfies every operand, not operands spread
// across different objects. Any qualifying group yields a row, so the hop resolves iff the
// query returns a row. Both hops require unconditioned tuples.
func buildJoinBoolQuery(b adapter.Builder, bnd bound, j *JoinNode) adapter.Query {
	t1 := b.Tuple("t1")
	t2 := b.Tuple("t2")

	where := hop1Predicates(b, t1, bnd, j)
	where = append(where, unconditionedPred(b, t1))
	where = append(where, hop2BasePredicates(b, t2, bnd, j)...)
	where = append(where, relationFilter(b, t2, j.Hop2))

	join := b.Join(adapter.InnerJoin, t2).On(t1.SubjectID().Eq(t2.ObjectID()))
	return b.Select(b.Lit(1)).
		From(t1).
		JoinClause(join).
		Where(where...).
		GroupBy(t2.ObjectID()).
		Having(combineHaving(b, t2, j.Hop2))
}

// buildJoinGatherQuery compiles a conditioned weight-2 hop into one self-join scan that
// gathers the joined rows for in-process CEL. The unconditioned side still prunes in SQL (its
// membership predicate stays in WHERE), so only joined rows survive; the executor groups the
// rows by intermediate object and folds the hop-2 subtree per object after evaluating CEL.
// The projection is, in order: the intermediate object id (the group key), the hop-1
// condition name and context, then the hop-2 relation, subject id, condition name, and
// context the executor attributes to a hop-2 leaf.
func buildJoinGatherQuery(b adapter.Builder, bnd bound, j *JoinNode) adapter.Query {
	t1 := b.Tuple("t1")
	t2 := b.Tuple("t2")

	leaves := collectLeaves(j.Hop2)
	disjuncts := make([]adapter.Predicate, len(leaves))
	for i, leaf := range leaves {
		disjuncts[i] = leafDisjunct(b, t2, leaf)
	}

	where := hop1Predicates(b, t1, bnd, j)
	where = append(where, conditionMembership(b, t1, j.Hop1Conditions))
	where = append(where, hop2BasePredicates(b, t2, bnd, j)...)
	where = append(where, disjuncts[0].Or(disjuncts[1:]...))

	join := b.Join(adapter.InnerJoin, t2).On(t1.SubjectID().Eq(t2.ObjectID()))
	return b.Select(
		t2.ObjectID(),
		t1.Condition(), t1.ConditionContext(),
		t2.ObjectRelation(), t2.SubjectID(), t2.Condition(), t2.ConditionContext(),
	).
		From(t1).
		JoinClause(join).
		Where(where...)
}
