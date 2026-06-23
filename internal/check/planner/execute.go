package planner

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
)

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
func (p *Plan) Execute(ctx context.Context, eval ConditionEvaluator) (bool, error) {
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
	rows, err := p.unit.query.Execute(ctx)
	if err != nil {
		return false, fmt.Errorf("planner: executing check query: %w", err)
	}
	defer rows.Close()

	gathered := make([]gatherRow, 0)
	for rows.Next() {
		var relation, subjectID sql.NullString
		var name sql.NullString
		var condCtx []byte
		if err := rows.Scan(&relation, &subjectID, &name, &condCtx); err != nil {
			return false, fmt.Errorf("planner: scanning check query: %w", err)
		}
		gathered = append(gathered, gatherRow{
			relation:  relation.String,
			subjectID: subjectID.String,
			condName:  name.String,
			condCtx:   condCtx,
		})
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("planner: iterating check query: %w", err)
	}

	satisfied := make(map[*QueryNode]bool, len(p.unit.leaves))
	condCache := make(map[string]bool)
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
func leafSatisfied(ctx context.Context, leaf *QueryNode, rows []gatherRow, eval ConditionEvaluator, cache map[string]bool) (bool, error) {
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
func evalCondition(ctx context.Context, eval ConditionEvaluator, leaf *QueryNode, r gatherRow, cache map[string]bool) (bool, error) {
	if eval == nil {
		return false, fmt.Errorf("planner: tuple for %q carries condition %q but no evaluator was provided", leaf.Label, r.condName)
	}
	key := r.condName + "\x00" + string(r.condCtx)
	if got, ok := cache[key]; ok {
		return got, nil
	}
	ok, err := eval.Eval(ctx, r.condName, r.condCtx)
	if err != nil {
		return false, fmt.Errorf("planner: evaluating condition %q for %q: %w", r.condName, leaf.Label, err)
	}
	cache[key] = ok
	return ok, nil
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

// fold reduces the plan tree to a boolean using the per-leaf satisfaction results.
func fold(n Node, satisfied map[*QueryNode]bool) bool {
	switch node := n.(type) {
	case *QueryNode:
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
