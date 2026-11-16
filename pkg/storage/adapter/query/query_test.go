package query

import (
	"reflect"
	"testing"
	"time"

	"github.com/openfga/openfga/pkg/storage/adapter/ast"
)

func assertNode(t *testing.T, got, want any) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("node mismatch:\n got: %#v\nwant: %#v", got, want)
	}
}

func assertPanics(t *testing.T, want string, f func()) {
	t.Helper()
	defer func() {
		t.Helper()
		if r := recover(); r != want {
			t.Errorf("want panic %q, got %v", want, r)
		}
	}()
	f()
}

func TestBuildersProduceNodes(t *testing.T) {
	a := NewTuple("a")
	objectID := ast.ColNode{Alias: "a", Name: ast.ColObjectID}
	subjectID := ast.ColNode{Alias: "a", Name: ast.ColSubjectID}
	x := Bind("x")
	bindX := ast.BindNode{Value: "x"}
	now := time.Unix(0, 0)
	stmt := Select(a.ObjectID()).From(a)
	isTrue := ast.ConstPredNode{Value: true}
	isFalse := ast.ConstPredNode{Value: false}

	tests := []struct {
		name string
		got  any
		want any
	}{
		{"Bind", Bind[int64](1).n, ast.BindNode{Value: int64(1)}},
		{"Bind time", Bind(now).n, ast.BindNode{Value: now}},
		{"Lit", Lit("doc").n, ast.LitNode{Value: "doc"}},
		{"BindAll", BindAll([]string{"a", "b"}).n, ast.SetBindNode{Elems: []any{"a", "b"}}},
		{"BindAll empty", BindAll([]string{}).n, ast.SetBindNode{Elems: []any{}}},
		{"True", True().n, isTrue},
		{"False", False().n, isFalse},

		{"Eq", Eq(a.ObjectID(), x).n, ast.CompareNode{Op: ast.OpEq, Left: objectID, Right: bindX}},
		{"Ne", Ne(a.ObjectID(), x).n, ast.CompareNode{Op: ast.OpNe, Left: objectID, Right: bindX}},
		{"Lt", Lt(a.ObjectID(), x).n, ast.CompareNode{Op: ast.OpLt, Left: objectID, Right: bindX}},
		{"Lte", Lte(a.ObjectID(), x).n, ast.CompareNode{Op: ast.OpLte, Left: objectID, Right: bindX}},
		{"Gt", Gt(a.ObjectID(), x).n, ast.CompareNode{Op: ast.OpGt, Left: objectID, Right: bindX}},
		{"Gte", Gte(a.ObjectID(), x).n, ast.CompareNode{Op: ast.OpGte, Left: objectID, Right: bindX}},
		{"Compare", Compare(OpLte, a.ObjectID(), x).n, ast.CompareNode{Op: ast.OpLte, Left: objectID, Right: bindX}},
		{"Like", Like(a.ObjectID(), x).n, ast.LikeNode{Left: objectID, Pattern: bindX}},
		{"Between", Between(a.ObjectID(), x, Bind("y")).n, ast.BetweenNode{Inner: objectID, Lo: bindX, Hi: ast.BindNode{Value: "y"}}},
		{"IsNull", IsNull(a.ObjectID()).n, ast.IsNullNode{Inner: objectID}},
		{"In", In(a.ObjectID(), x, Lit("y")).n, ast.InNode{Left: objectID, Elems: []ast.ScalarValue{bindX, ast.LitNode{Value: "y"}}}},
		{"In no values", In(a.ObjectID()).n, ast.InNode{Left: objectID, Elems: []ast.ScalarValue{}}},
		{
			"Quantified bound set",
			Quantified(a.ObjectID(), OpEq, Any, BindAll([]string{"x"})).n,
			ast.QuantifiedNode{Left: objectID, Op: ast.OpEq, Q: ast.Any, Set: ast.SetBindNode{Elems: []any{"x"}}},
		},
		{
			"Quantified subquery",
			Quantified(a.ObjectID(), OpNe, All, SetSubExpr[string](stmt)).n,
			ast.QuantifiedNode{Left: objectID, Op: ast.OpNe, Q: ast.All, Set: ast.SubqueryNode{Stmt: &stmt.Select}},
		},
		{"Exists", Exists(stmt).n, ast.ExistsNode{Stmt: &stmt.Select}},

		{"And single", And(True()).n, isTrue},
		{"And", And(True(), False()).n, ast.LogicalNode{Op: ast.And, Parts: []ast.Predicate{isTrue, isFalse}}},
		{"Or single", Or(False()).n, isFalse},
		{"Or", Or(True(), False(), True()).n, ast.LogicalNode{Op: ast.Or, Parts: []ast.Predicate{isTrue, isFalse, isTrue}}},
		{"Not", Not(True()).n, ast.NotNode{Inner: isTrue}},

		{"Order", Order(a.ObjectID(), Descending).t, ast.OrderTerm{Expr: objectID, Dir: ast.Descending}},
		{"Asc", Asc(a.ObjectID()).t, ast.OrderTerm{Expr: objectID, Dir: ast.Ascending}},
		{"Desc", Desc(a.ObjectID()).t, ast.OrderTerm{Expr: objectID, Dir: ast.Descending}},

		{"Coalesce", Coalesce(a.ObjectID(), x).n, ast.FuncNode{Fn: ast.FuncCoalesce, Args: []ast.ScalarValue{objectID, bindX}}},
		{"Lower", Lower(a.ObjectID()).n, ast.FuncNode{Fn: ast.FuncLower, Args: []ast.ScalarValue{objectID}}},
		{"Upper", Upper(a.ObjectID()).n, ast.FuncNode{Fn: ast.FuncUpper, Args: []ast.ScalarValue{objectID}}},
		{"Pair", Pair(Lit("k"), a.ObjectID()).n, ast.JSONPairNode{Key: ast.LitNode{Value: "k"}, Value: objectID}},
		{
			"JSONObject",
			JSONObject(Pair(Lit("k"), a.ObjectID()), Pair(Lit("n"), Lit[int64](1))).n,
			ast.JSONObjectNode{Pairs: []ast.JSONPairNode{
				{Key: ast.LitNode{Value: "k"}, Value: objectID},
				{Key: ast.LitNode{Value: "n"}, Value: ast.LitNode{Value: int64(1)}},
			}},
		},
		{
			"JSONArray",
			JSONArray(a.ObjectID(), Lit[int64](1)).n,
			ast.FuncNode{Fn: ast.FuncJSONArray, Args: []ast.ScalarValue{objectID, ast.LitNode{Value: int64(1)}}},
		},

		{"Count star", Count(Star).n, ast.AggNode{Fn: ast.AggCount}},
		{"Count column", Count(a.SubjectID()).n, ast.AggNode{Fn: ast.AggCount, Args: []ast.ScalarValue{subjectID}}},
		{
			"Count distinct filtered",
			Count(a.SubjectID(), AggDistinct, AggFilter(True())).n,
			ast.AggNode{Fn: ast.AggCount, Args: []ast.ScalarValue{subjectID}, Distinct: true, Filter: isTrue},
		},
		{"Count zero-value option", Count(Star, AggOption{}).n, ast.AggNode{Fn: ast.AggCount}},

		{
			"Case",
			Case[string]().When(True(), a.ObjectID()).When(False(), x).End().n,
			ast.CaseSearchedNode{Branches: []ast.SearchedBranch{{When: isTrue, Then: objectID}, {When: isFalse, Then: bindX}}},
		},
		{
			"Case else replaced",
			Case[string]().When(True(), a.ObjectID()).Else(Lit("first")).Else(x).End().n,
			ast.CaseSearchedNode{Branches: []ast.SearchedBranch{{When: isTrue, Then: objectID}}, Else: bindX},
		},
		{
			"CaseOf",
			CaseOf[int64](a.ObjectType()).When(Lit("folder"), Lit[int64](1)).End().n,
			ast.CaseSimpleNode{
				Base:     ast.ColNode{Alias: "a", Name: ast.ColObjectType},
				Branches: []ast.SimpleBranch{{When: ast.LitNode{Value: "folder"}, Then: ast.LitNode{Value: int64(1)}}},
			},
		},
		{
			"CaseOf else replaced",
			CaseOf[int64](a.ObjectType()).When(Lit("folder"), Lit[int64](1)).Else(Lit[int64](2)).Else(Lit[int64](0)).End().n,
			ast.CaseSimpleNode{
				Base:     ast.ColNode{Alias: "a", Name: ast.ColObjectType},
				Branches: []ast.SimpleBranch{{When: ast.LitNode{Value: "folder"}, Then: ast.LitNode{Value: int64(1)}}},
				Else:     ast.LitNode{Value: int64(0)},
			},
		},

		{"Cast", Cast[int64](a.ObjectID(), TypeBigint).n, ast.CastNode{Inner: objectID, Type: ast.TypeBigint}},
		{"ScalarExpr", ScalarExpr[string](stmt).n, ast.SubqueryNode{Stmt: &stmt.Select}},
		{"SetSubExpr", SetSubExpr[string](stmt).n, ast.SubqueryNode{Stmt: &stmt.Select}},
		{"As", As(a.ObjectID(), "id").n, ast.AliasNode{Inner: objectID, Alias: "id"}},
		{"As subquery", As(SetSubExpr[string](stmt), "ids").n, ast.AliasNode{Inner: ast.SubqueryNode{Stmt: &stmt.Select}, Alias: "ids"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assertNode(t, tt.got, tt.want)
		})
	}
}

func TestTupleColumns(t *testing.T) {
	a := NewTuple("a")
	cols := []struct {
		got  ast.ScalarValue
		want ast.Column
	}{
		{a.ObjectType().n, ast.ColObjectType},
		{a.ObjectID().n, ast.ColObjectID},
		{a.ObjectRelation().n, ast.ColObjectRelation},
		{a.SubjectType().n, ast.ColSubjectType},
		{a.SubjectID().n, ast.ColSubjectID},
		{a.SubjectRelation().n, ast.ColSubjectRelation},
		{a.Store().n, ast.ColStore},
		{a.Condition().n, ast.ColCondition},
		{a.ConditionContext().n, ast.ColConditionContext},
	}
	for _, c := range cols {
		assertNode(t, c.got, ast.ColNode{Alias: "a", Name: c.want})
	}

	assertNode(t, a.SubjectKindIn(KindUserset).n, ast.SubjectKindNode{Alias: "a", Kinds: ast.KindUserset})
}

func TestStatement(t *testing.T) {
	a, b, c, d := NewTuple("a"), NewTuple("b"), NewTuple("c"), NewTuple("d")
	objectID := ast.ColNode{Alias: "a", Name: ast.ColObjectID}
	subjectID := ast.ColNode{Alias: "b", Name: ast.ColSubjectID}
	isTrue := ast.ConstPredNode{Value: true}
	isFalse := ast.ConstPredNode{Value: false}

	got := Select(a.ObjectID()).
		Columns(As(Count(Star), "n")).
		Distinct().
		From(a).
		From(b).
		Join(c, True()).
		JoinOn(JoinLeftOuter, d, False()).
		CrossJoin(NewTuple("e")).
		Where(False()).
		Where(True()).
		GroupBy(a.ObjectID()).
		GroupBy(b.SubjectID()).
		Having(True()).
		Having(False()).
		OrderBy(Asc(a.ObjectID())).
		OrderBy(Desc(b.SubjectID())).
		Limit(5).
		Limit(10).
		Offset(1).
		Offset(2)

	limit, offset := uint64(10), uint64(2)
	assertNode(t, got.Select, ast.Select{
		Distinct: true,
		Columns:  []ast.Projection{objectID, ast.AliasNode{Inner: ast.AggNode{Fn: ast.AggCount}, Alias: "n"}},
		From:     []ast.Table{{Alias: "a"}, {Alias: "b"}},
		Joins: []ast.JoinClause{
			{Type: ast.JoinInner, Table: ast.Table{Alias: "c"}, On: isTrue},
			{Type: ast.JoinLeftOuter, Table: ast.Table{Alias: "d"}, On: isFalse},
			{Type: ast.JoinCross, Table: ast.Table{Alias: "e"}},
		},
		Where:   isTrue,
		GroupBy: []ast.ScalarValue{objectID, subjectID},
		Having:  isFalse,
		OrderBy: []ast.OrderTerm{{Expr: objectID, Dir: ast.Ascending}, {Expr: subjectID, Dir: ast.Descending}},
		Limit:   &limit,
		Offset:  &offset,
	})

	assertNode(t, Select().Select, ast.Select{})
}

func TestCaseEndWithoutBranchesPanics(t *testing.T) {
	const want = "query: CASE with no WHEN branches"
	assertPanics(t, want, func() { Case[string]().Else(Lit("x")).End() })
	assertPanics(t, want, func() { CaseOf[int64](Lit("x")).Else(Lit[int64](0)).End() })
}

// elem exists only to tie a set to its element type; it always returns the zero value.
func TestSetElemIsZero(t *testing.T) {
	var sets = []AnySet[int64]{
		BindAll([]int64{1}),
		SetSubExpr[int64](Select()),
	}
	for _, s := range sets {
		if got := s.elem(); got != 0 {
			t.Errorf("%T.elem() = %d, want 0", s, got)
		}
	}
}
