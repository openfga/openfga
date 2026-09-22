// Package ast_test builds trees through exported types and package query, as an adapter would.
package ast_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
	"github.com/openfga/openfga/pkg/storage/adapter/query"
)

// col is a valid ColNode used as filler wherever a test cares about some other field.
func col() ast.ColNode { return ast.ColNode{Alias: "t", Name: ast.ColObjectID} }

// pred is a valid Predicate, used the same way.
func pred() ast.Predicate { return ast.ConstPredNode{Value: true} }

// assertValid fails if a well-formed tree is rejected.
func assertValid(t *testing.T, n ast.Node) {
	t.Helper()
	if err := n.Validate(); err != nil {
		t.Errorf("%T: want valid, got error:\n%v", n, err)
	}
}

// assertInvalid fails unless the tree is rejected with the expected sentinel and a message
// mentioning the field path.
func assertInvalid(t *testing.T, n ast.Node, want error, path string) {
	t.Helper()
	err := n.Validate()
	if err == nil {
		t.Fatalf("%T: want error %v, got nil", n, want)
	}
	if !errors.Is(err, want) {
		t.Errorf("%T: want errors.Is(err, %v), got:\n%v", n, want, err)
	}
	if !strings.Contains(err.Error(), path) {
		t.Errorf("%T: want message mentioning %q, got:\n%v", n, path, err)
	}
}

// TestBuilderProducesValidTrees checks that every statement package query can build validates clean.
func TestBuilderProducesValidTrees(t *testing.T) {
	a, b := query.NewTuple("a"), query.NewTuple("b")

	stmts := map[string]*query.Statement{
		"minimal": query.Select(a.ObjectID()).From(a),

		"count star": query.Select(query.Count(query.Star)).From(a),

		"count distinct column": query.Select(
			query.Count(a.SubjectID(), query.AggDistinct),
		).From(a),

		"where and": query.Select(a.ObjectID()).From(a).Where(query.And(
			query.Eq(a.Store(), query.Bind("s")),
			query.Eq(a.ObjectType(), query.Lit("document")),
		)),

		// A single-part LogicalNode is legal.
		"where single-part and": query.Select(a.ObjectID()).From(a).
			Where(query.And(query.Eq(a.Store(), query.Bind("s")))),

		"join": query.Select(a.ObjectID()).From(a).
			Join(b, query.Eq(a.SubjectID(), b.ObjectID())),

		"cross join": query.Select(a.ObjectID()).From(a).CrossJoin(b),

		"in": query.Select(a.ObjectID()).From(a).
			Where(query.In(a.ObjectType(), query.Lit("document"), query.Bind("folder"))),

		"quantified over bound set": query.Select(a.ObjectID()).From(a).Where(
			query.Quantified(a.ObjectType(), ast.OpEq, ast.Any,
				query.BindAll([]string{"document", "folder"})),
		),

		// The empty bound set is a documented arity exception.
		"quantified over empty bound set": query.Select(a.ObjectID()).From(a).Where(
			query.Quantified(a.ObjectType(), ast.OpEq, ast.Any, query.BindAll([]string{})),
		),

		"exists": query.Select(a.ObjectID()).From(a).
			Where(query.Exists(query.Select(b.ObjectID()).From(b))),

		"distinct": query.Select(a.ObjectID()).From(a).Distinct(),

		// Having with no GroupBy is explicitly permitted.
		"having without group by": query.Select(query.Count(query.Star)).From(a).
			Having(query.Gt(query.Count(query.Star), query.Lit(int64(1)))),

		"group by having": query.Select(a.ObjectType()).From(a).
			GroupBy(a.ObjectType()).
			Having(query.Gt(query.Count(query.Star), query.Lit(int64(1)))),

		"searched case": query.Select(query.As(query.Case[string]().
			When(query.Eq(a.ObjectType(), query.Lit("document")), query.Lit("doc")).
			Else(query.Lit("other")).End(), "kind")).From(a),

		"simple case": query.Select(query.CaseOf[string, string](a.ObjectType()).
			When(query.Lit("document"), query.Lit("doc")).End()).From(a),

		"json object": query.Select(query.JSONObject(
			query.Pair(query.Lit("id"), a.ObjectID()),
		)).From(a),

		"json array": query.Select(query.JSONArray(a.ObjectID(), a.ObjectType())).From(a),

		// The empty JSON array is legal.
		"empty json array": query.Select(query.JSONArray()).From(a),

		// A COUNT with FILTER and DISTINCT exercises the aggregate modifiers.
		"count distinct filtered": query.Select(query.Count(a.SubjectID(),
			query.AggDistinct,
			query.AggFilter(query.Eq(a.ObjectType(), query.Lit("document"))),
		)).From(a).GroupBy(a.Store()),

		"cast": query.Select(query.Cast[string](a.ConditionContext(), ast.TypeVarchar)).From(a),

		"coalesce": query.Select(
			query.Coalesce(a.Condition(), query.Lit("")),
		).From(a),

		"lower like": query.Select(a.ObjectID()).From(a).
			Where(query.Like(query.Lower(a.ObjectID()), query.Lit("a%"))),

		"between": query.Select(a.ObjectID()).From(a).
			Where(query.Between(a.ObjectID(), query.Lit("a"), query.Lit("z"))),

		"is null": query.Select(a.ObjectID()).From(a).Where(query.IsNull(a.Condition())),

		"not": query.Select(a.ObjectID()).From(a).
			Where(query.Not(query.Eq(a.ObjectType(), query.Lit("document")))),

		"scalar subquery": query.Select(query.ScalarExpr[string](
			query.Select(b.ObjectID()).From(b),
		)).From(a),

		"order and limit": query.Select(a.ObjectID()).From(a).
			OrderBy(query.Desc(a.ObjectID())).
			Limit(10).Offset(5),

		"constant projection, no from": query.Select(query.Lit("x")),
	}

	for name, s := range stmts {
		t.Run(name, func(t *testing.T) {
			assertValid(t, &s.Select)
		})
	}
}

// TestRequiredFields covers the rule that an interface-typed field is required unless documented otherwise.
func TestRequiredFields(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		path string
	}{
		{"compare left", ast.CompareNode{Right: col()}, "Left"},
		{"compare right", ast.CompareNode{Left: col()}, "Right"},
		{"like left", ast.LikeNode{Pattern: col()}, "Left"},
		{"like pattern", ast.LikeNode{Left: col()}, "Pattern"},
		{"between lo", ast.BetweenNode{Inner: col(), Hi: col()}, "Lo"},
		{"is null inner", ast.IsNullNode{}, "Inner"},
		{"in left", ast.InNode{Elems: []ast.ScalarValue{col()}}, "Left"},
		{"not inner", ast.NotNode{}, "Inner"},
		{"quantified left", ast.QuantifiedNode{Set: ast.SetBindNode{}}, "Left"},
		{"quantified set", ast.QuantifiedNode{Left: col()}, "Set"},
		{"cast inner", ast.CastNode{Type: ast.TypeVarchar}, "Inner"},
		{"alias inner", ast.AliasNode{Alias: "x"}, "Inner"},
		{"case simple base", ast.CaseSimpleNode{
			Branches: []ast.SimpleBranch{{When: col(), Then: col()}},
		}, "Base"},
		{"searched branch when", ast.CaseSearchedNode{
			Branches: []ast.SearchedBranch{{Then: col()}},
		}, "Branches[0]: When"},
		{"json pair key", ast.JSONObjectNode{
			Pairs: []ast.JSONPairNode{{Value: col()}},
		}, "Pairs[0]: Key"},
		{"order term expr", &ast.Select{
			From:    []ast.Table{{Alias: "t"}},
			OrderBy: []ast.OrderTerm{{}},
		}, "OrderBy[0]: Expr"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertInvalid(t, tt.node, ast.ErrMissing, tt.path)
		})
	}
}

// TestNilStatementPointers checks a nil *Select is reported, not dereferenced: a typed nil pointer
// inside an interface is not a nil interface.
func TestNilStatementPointers(t *testing.T) {
	assertInvalid(t, ast.ExistsNode{}, ast.ErrMissing, "Stmt")
	assertInvalid(t, ast.SubqueryNode{}, ast.ErrMissing, "Stmt")

	// Reached through a field rather than at the root.
	assertInvalid(t, &ast.Select{
		From:  []ast.Table{{Alias: "t"}},
		Where: ast.ExistsNode{},
	}, ast.ErrMissing, "Where: Stmt")
}

// TestArity covers the minimum-length rules the slice types cannot express: WHERE (), x IN (),
// CASE END, COALESCE().
func TestArity(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		path string
	}{
		{"empty logical parts", ast.LogicalNode{Op: ast.And}, "Parts"},
		{"empty in elems", ast.InNode{Left: col()}, "Elems"},
		{"empty coalesce args", ast.FuncNode{Fn: ast.FuncCoalesce}, "Args"},
		{"empty json object pairs", ast.JSONObjectNode{}, "Pairs"},
		{"empty searched branches", ast.CaseSearchedNode{Else: col()}, "Branches"},
		{"empty simple branches", ast.CaseSimpleNode{Base: col(), Else: col()}, "Branches"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertInvalid(t, tt.node, ast.ErrArity, tt.path)
		})
	}
}

// TestPerFunctionArity covers the per-Fn argument-count rules: COALESCE one or more, LOWER/UPPER
// exactly one, JSON_ARRAY any number including zero, COUNT zero (COUNT(*)) or one.
func TestPerFunctionArity(t *testing.T) {
	one := []ast.ScalarValue{col()}
	two := []ast.ScalarValue{col(), col()}

	t.Run("valid", func(t *testing.T) {
		for _, n := range []ast.Node{
			ast.FuncNode{Fn: ast.FuncCoalesce, Args: one},
			ast.FuncNode{Fn: ast.FuncCoalesce, Args: two},
			ast.FuncNode{Fn: ast.FuncLower, Args: one},
			ast.FuncNode{Fn: ast.FuncJSONArray}, // the one empty-argument function.
			ast.FuncNode{Fn: ast.FuncJSONArray, Args: two},
			ast.AggNode{Fn: ast.AggCount},            // COUNT(*).
			ast.AggNode{Fn: ast.AggCount, Args: one}, // COUNT(x).
		} {
			assertValid(t, n)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		for _, n := range []ast.Node{
			ast.FuncNode{Fn: ast.FuncLower, Args: two}, // exactly one.
			ast.FuncNode{Fn: ast.FuncUpper},            // exactly one.
			ast.AggNode{Fn: ast.AggCount, Args: two},   // zero or one.
		} {
			assertInvalid(t, n, ast.ErrArity, "Args")
		}
	})
}

// TestCrossFieldRules covers rules between fields that are each individually well-formed, e.g.
// COUNT(DISTINCT *) is malformed though each field alone looks fine.
func TestCrossFieldRules(t *testing.T) {
	t.Run("count distinct star", func(t *testing.T) {
		assertInvalid(t, ast.AggNode{Fn: ast.AggCount, Distinct: true},
			ast.ErrCrossField, "Distinct")
	})

	t.Run("cross join with condition", func(t *testing.T) {
		assertInvalid(t, &ast.Select{
			From: []ast.Table{{Alias: "a"}},
			Joins: []ast.JoinClause{
				{Type: ast.JoinCross, Table: ast.Table{Alias: "b"}, On: pred()},
			},
		}, ast.ErrCrossField, "Joins[0]: On")
	})

	t.Run("inner join without condition", func(t *testing.T) {
		assertInvalid(t, &ast.Select{
			From:  []ast.Table{{Alias: "a"}},
			Joins: []ast.JoinClause{{Type: ast.JoinInner, Table: ast.Table{Alias: "b"}}},
		}, ast.ErrMissing, "Joins[0]: On")
	})

	t.Run("duplicate table alias", func(t *testing.T) {
		assertInvalid(t, &ast.Select{
			From: []ast.Table{{Alias: "a"}, {Alias: "a"}},
		}, ast.ErrCrossField, `"a"`)

		// Also across the FROM list and the joins.
		assertInvalid(t, &ast.Select{
			From: []ast.Table{{Alias: "a"}},
			Joins: []ast.JoinClause{
				{Type: ast.JoinInner, Table: ast.Table{Alias: "a"}, On: pred()},
			},
		}, ast.ErrCrossField, `"a"`)
	})
}

// TestEnumRanges checks that out-of-range enum values, including the Count sentinels, are rejected.
func TestEnumRanges(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
	}{
		{"column count sentinel", ast.ColNode{Alias: "t", Name: ast.ColCount}},
		{"cast type sentinel", ast.CastNode{Inner: col(), Type: ast.TypeCount}},
		{"scalar func sentinel", ast.FuncNode{Fn: ast.FuncCount, Args: []ast.ScalarValue{col()}}},
		{"agg func sentinel", ast.AggNode{Fn: ast.AggCount_}},
		{"op out of range", ast.CompareNode{Op: ast.Op(99), Left: col(), Right: col()}},
		{"logical op out of range", ast.LogicalNode{
			Op: ast.LogicalOp(99), Parts: []ast.Predicate{pred()},
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.node.Validate(); !errors.Is(err, ast.ErrEnum) {
				t.Errorf("%T: want ErrEnum, got:\n%v", tt.node, err)
			}
		})
	}
}

// TestValueRules covers what a bound or inlined slot may hold: a bool is rejected (truth values are
// the Predicate category) and an AST node in a value slot is rejected.
func TestValueRules(t *testing.T) {
	type flag bool // a named bool, which a concrete-type check would miss.

	t.Run("valid", func(t *testing.T) {
		for _, n := range []ast.Node{
			ast.BindNode{Value: "s"},
			ast.BindNode{Value: int64(1)},
			ast.BindNode{Value: nil}, // a bound NULL.
			ast.LitNode{Value: 1.5},
			ast.SetBindNode{}, // the arity exception.
			ast.SetBindNode{Elems: []any{"a", int64(2)}},
		} {
			assertValid(t, n)
		}
	})

	t.Run("bool", func(t *testing.T) {
		for _, n := range []ast.Node{
			ast.BindNode{Value: true},
			ast.LitNode{Value: false},
			ast.BindNode{Value: flag(true)},
			ast.SetBindNode{Elems: []any{"a", true}},
		} {
			assertInvalid(t, n, ast.ErrValue, "bool")
		}
	})

	t.Run("ast node as value", func(t *testing.T) {
		assertInvalid(t, ast.BindNode{Value: col()}, ast.ErrValue, "AST node")
		assertInvalid(t, ast.LitNode{Value: &ast.Select{}}, ast.ErrValue, "AST node")
	})
}

// TestAliasRules checks that only emptiness is validated; character set and quoting belong to the renderer.
func TestAliasRules(t *testing.T) {
	assertInvalid(t, ast.ColNode{Name: ast.ColObjectID}, ast.ErrAlias, "Alias")
	assertInvalid(t, ast.AliasNode{Inner: col()}, ast.ErrAlias, "Alias")
	assertInvalid(t, &ast.Select{From: []ast.Table{{}}}, ast.ErrAlias, "From[0]: Alias")
}

// TestReportsOneViolation checks fail-fast: a tree with four independent faults yields one finding.
func TestReportsOneViolation(t *testing.T) {
	bad := &ast.Select{
		Columns: []ast.Projection{ast.AliasNode{Alias: "x"}}, // Inner nil.
		From:    []ast.Table{{Alias: "a"}, {Alias: "a"}},     // duplicate alias.
		Where:   ast.LogicalNode{Op: ast.And},                // empty Parts.
		Having:  ast.CompareNode{Op: ast.OpEq, Left: col()},  // Right nil.
	}

	err := bad.Validate()
	if err == nil {
		t.Fatal("want error, got nil")
	}
	if got, want := len(strings.Split(err.Error(), "\n")), 1; got != want {
		t.Errorf("finding count: got %d, want %d:\n%v", got, want, err)
	}
	if errors.Is(err, ast.ErrArity) || errors.Is(err, ast.ErrMissing) {
		t.Errorf("a later fault leaked into the first finding: %v", err)
	}
}

// TestChecksInOrder pins the order Validate reaches faults: local rules before children, and among
// local rules the quietly-wrong ones before the loud ones. Each fixture carries more than one fault.
func TestChecksInOrder(t *testing.T) {
	t.Run("select reports its cross-field rules before any clause", func(t *testing.T) {
		// A duplicate alias silently returns wrong rows, so it is reached before the clause's nil operand.
		assertInvalid(t, &ast.Select{
			From:  []ast.Table{{Alias: "a"}, {Alias: "a"}},
			Where: ast.NotNode{},
		}, ast.ErrCrossField, "used twice")
	})

	t.Run("clauses are reached in source order", func(t *testing.T) {
		assertInvalid(t, &ast.Select{
			Columns: []ast.Projection{ast.AliasNode{Alias: "x"}}, // Inner nil.
			From:    []ast.Table{{Alias: "a"}},
			Where:   ast.NotNode{}, // also nil, but later.
		}, ast.ErrMissing, "Columns[0]: Inner")
	})

	t.Run("a bad Fn is reported before the arity it makes meaningless", func(t *testing.T) {
		// FuncCount is a sentinel, not a function, so an arity verdict would be meaningless.
		assertInvalid(t, ast.FuncNode{Fn: ast.FuncCount}, ast.ErrEnum, "Fn")
		assertInvalid(t, ast.AggNode{Fn: ast.AggCount_}, ast.ErrEnum, "Fn")
	})

	t.Run("a node reports its first bad element, not its last", func(t *testing.T) {
		assertInvalid(t, ast.LogicalNode{Op: ast.Or, Parts: []ast.Predicate{
			ast.NotNode{},                            // fault one.
			ast.CompareNode{Op: ast.OpEq, Left: nil}, // fault two.
		}}, ast.ErrMissing, "Parts[0]: Inner")
	})
}

// TestNestedErrorPathIsBuiltUp checks that the path prefix accumulates as the recursion returns.
func TestNestedErrorPathIsBuiltUp(t *testing.T) {
	deep := &ast.Select{
		From: []ast.Table{{Alias: "a"}},
		Where: ast.LogicalNode{Op: ast.And, Parts: []ast.Predicate{
			pred(),
			ast.NotNode{Inner: ast.CompareNode{Op: ast.OpEq, Left: col()}}, // Right nil.
		}},
	}

	const want = "Where: Parts[1]: Inner: Right: required field is nil"
	if err := deep.Validate(); err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("want a finding at %q, got:\n%v", want, err)
	}
}

// TestFindingIsFullyQualified checks the path is complete from the root, not just the frame the
// fault was found in.
func TestFindingIsFullyQualified(t *testing.T) {
	two := &ast.Select{
		From:  []ast.Table{{Alias: "a"}},
		Where: ast.NotNode{Inner: ast.CompareNode{Op: ast.OpEq}}, // BOTH operands nil.
	}

	err := two.Validate()
	if err == nil {
		t.Fatal("want error, got nil")
	}
	const want = "Where: Inner: Left: required field is nil"
	if got := err.Error(); got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// TestValidateRecursesThroughSubqueries checks one call at the root reaches every nested statement.
func TestValidateRecursesThroughSubqueries(t *testing.T) {
	inner := &ast.Select{From: []ast.Table{{Alias: "b"}}, Where: ast.NotNode{}} // Inner nil.

	t.Run("through exists", func(t *testing.T) {
		assertInvalid(t, &ast.Select{
			From:  []ast.Table{{Alias: "a"}},
			Where: ast.ExistsNode{Stmt: inner},
		}, ast.ErrMissing, "Where: Stmt: Where: Inner")
	})

	t.Run("through a projected subquery", func(t *testing.T) {
		assertInvalid(t, &ast.Select{
			Columns: []ast.Projection{ast.SubqueryNode{Stmt: inner}},
			From:    []ast.Table{{Alias: "a"}},
		}, ast.ErrMissing, "Columns[0]: Stmt: Where: Inner")
	})

	t.Run("through a quantified set operand", func(t *testing.T) {
		assertInvalid(t, &ast.Select{
			From: []ast.Table{{Alias: "a"}},
			Where: ast.QuantifiedNode{
				Left: col(), Op: ast.OpEq, Q: ast.Any,
				Set: ast.SubqueryNode{Stmt: inner},
			},
		}, ast.ErrMissing, "Where: Set: Stmt: Where: Inner")
	})
}

// TestZeroSelectIsValid checks an empty statement is valid: both an empty From and empty Columns are allowed.
func TestZeroSelectIsValid(t *testing.T) {
	assertValid(t, &ast.Select{})
}
