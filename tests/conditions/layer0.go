package conditions

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// WriteCase is one tuple and whether the server must accept it.
type WriteCase struct {
	ID       string
	User     string
	Relation string
	// Condition is the tuple's condition. Nil writes a tuple with no condition.
	Condition *openfgav1.RelationshipCondition
	// Accept is the correct outcome: true if the tuple must be accepted.
	Accept bool
	// Gap marks a tuple that is accepted today and must be rejected.
	Gap *Gap
	// WriteOnly skips the contextual-tuple form of the case.
	WriteOnly bool
}

const layer0Model = `model
  schema 1.1

type user

type group
  relations
    define member: [user]

type doc
  relations
    define cond: [user with $expression]
    define plain: [user]
    define cond_wild: [user:* with $expression]
    define cond_group: [group#member with $expression]
`

// rawCondition builds a $expression condition from an arbitrary context, so
// that malformed contexts can be written.
func rawCondition(fields map[string]any) *openfgav1.RelationshipCondition {
	return &openfgav1.RelationshipCondition{Name: InlineName, Context: toStruct(fields)}
}

var (
	scalarTypes = []string{"string", "int", "uint", "double", "bool", "duration", "timestamp", "ipaddress"}

	// Names that work as parameter names, even though several of them are also
	// CEL type or function names.
	validParameterNames = []string{
		"channel_id", "maxLength", "_x", "X1", "type", "size", "string", "int", "uint", "bool", "double",
		"list", "map", "timestamp", "duration", "has", "all", "matches", "expression", "parameters",
	}

	// Names that are CEL reserved words or are not identifiers.
	invalidParameterNames = []string{
		"in", "as", "if", "else", "for", "var", "function", "namespace", "package", "import", "let", "loop",
		"return", "void", "while", "break", "const", "continue", "true", "false", "null", "a-b", "ünï", "7up",
	}
)

// Layer0 builds the Write layer.
func Layer0() []WriteCase {
	var cases []WriteCase
	add := func(id string, accept bool, cond *openfgav1.RelationshipCondition) *WriteCase {
		cases = append(cases, WriteCase{ID: "L0/" + id, User: "user:u", Relation: "cond", Condition: cond, Accept: accept})
		return &cases[len(cases)-1]
	}

	// Well-formed tuples.
	add("valid/declared-parameter", true, InlineCondition(`channel_id == 'A'`, map[string]string{"channel_id": "string"}))
	add("valid/undeclared-parameter-is-string", true, InlineCondition(`channel_id == 'A'`, nil))
	add("valid/no-parameters", true, InlineCondition(`true`, nil))
	add("valid/two-parameters", true, InlineCondition(`a == 'x' && b < 5`, map[string]string{"a": "string", "b": "int"}))
	add("valid/declared-but-unused-parameter", true, InlineCondition(`true`, map[string]string{"unused": "string"}))
	add("valid/small-loop", true, InlineCondition(`ids.all(i, i < 100)`, map[string]string{"ids": "list<int>"}))

	// Parameter types.
	for _, typ := range scalarTypes {
		add("type/valid/"+typ, true, InlineCondition(`true`, map[string]string{"p": typ}))
		add("type/valid/list<"+typ+">", true, InlineCondition(`true`, map[string]string{"p": "list<" + typ + ">"}))
		add("type/valid/map<"+typ+">", true, InlineCondition(`true`, map[string]string{"p": "map<" + typ + ">"}))
	}
	for _, typ := range []string{"map", "list", "str", "String", "integer", "bytes", "map< string >", "map<string", "list<>", ""} {
		add(fmt.Sprintf("type/invalid/%q", typ), false, InlineCondition(`true`, map[string]string{"p": typ}))
	}
	add("type/invalid/not-a-string", false, rawCondition(map[string]any{"expression": "true", "parameters": map[string]any{"p": 5}}))
	for _, typ := range []string{"any", "map<any>", "list<any>", "list<map<any>>"} {
		add("type/any/"+typ, false, InlineCondition(`true`, map[string]string{"p": typ})).Gap = GapAnyTypes
	}

	// Parameter names.
	for _, name := range validParameterNames {
		add("name/valid/"+name, true, InlineCondition(name+` == 'v'`, map[string]string{name: "string"}))
		add("name/valid-undeclared/"+name, true, InlineCondition(name+` == 'v'`, nil))
	}
	for _, name := range invalidParameterNames {
		add("name/invalid/"+name, false, InlineCondition(name+` == 'v'`, map[string]string{name: "string"}))
	}

	// The expression itself.
	add("expression/missing", false, rawCondition(map[string]any{"parameters": map[string]any{"p": "string"}}))
	add("expression/empty", false, rawCondition(map[string]any{"expression": ""}))
	add("expression/not-a-string", false, rawCondition(map[string]any{"expression": 5}))
	add("expression/syntax-error", false, InlineCondition(`p ==`, nil))
	add("expression/not-boolean-string", false, InlineCondition(`'abc'`, nil))
	add("expression/not-boolean-number", false, InlineCondition(`1 + 1`, nil))
	add("expression/type-error-undeclared-is-string", false, InlineCondition(`p == 1235`, nil))
	add("expression/type-error-declared-int", false, InlineCondition(`p < 'x'`, map[string]string{"p": "int"}))
	add("expression/unknown-function", false, InlineCondition(`p.nope()`, nil))
	add("expression/deeply-nested", false, InlineCondition(strings.Repeat("(", 300)+"true"+strings.Repeat(")", 300), nil))
	add("expression/over-context-size-limit", false, InlineCondition(strings.Repeat(`p == 'x' || `, 3000)+"false", nil)).WriteOnly = true
	add("expression/over-cost-limit-for-any-input", false, InlineCondition(
		`[1,2,3,4,5,6,7,8,9,10].all(a, [1,2,3,4,5,6,7,8,9,10].all(b, [1,2,3,4,5,6,7,8,9,10].all(c, a + b + c > 0)))`, nil)).Gap = GapCostAtWrite

	// The shape of the context.
	add("context/nil", false, &openfgav1.RelationshipCondition{Name: InlineName})
	add("context/parameters-not-an-object", false, rawCondition(map[string]any{"expression": "true", "parameters": "p: string"}))
	add("context/extra-key", false, rawCondition(map[string]any{
		"expression": "p == allowed", "parameters": map[string]any{"p": "string", "allowed": "string"}, "allowed": "A",
	})).Gap = GapExtraContextKeys

	// The relation's type restriction.
	valid := InlineCondition(`true`, nil)
	cases = append(cases,
		WriteCase{ID: "L0/restriction/relation-without-expression", User: "user:u", Relation: "plain", Condition: valid, Accept: false},
		WriteCase{ID: "L0/restriction/no-condition-on-conditioned-relation", User: "user:u", Relation: "cond", Condition: nil, Accept: false},
		WriteCase{ID: "L0/restriction/named-condition-not-in-model", User: "user:u", Relation: "cond", Condition: NamedCondition("some_condition"), Accept: false},
		WriteCase{ID: "L0/restriction/wildcard-on-user-relation", User: "user:*", Relation: "cond", Condition: valid, Accept: false},
		WriteCase{ID: "L0/restriction/userset-on-user-relation", User: "group:g#member", Relation: "cond", Condition: valid, Accept: false},
		WriteCase{ID: "L0/restriction/user-on-wildcard-relation", User: "user:u", Relation: "cond_wild", Condition: valid, Accept: false},
		WriteCase{ID: "L0/restriction/wildcard-on-wildcard-relation", User: "user:*", Relation: "cond_wild", Condition: valid, Accept: true},
		WriteCase{ID: "L0/restriction/userset-on-userset-relation", User: "group:g#member", Relation: "cond_group", Condition: valid, Accept: true},
		WriteCase{ID: "L0/restriction/user-on-userset-relation", User: "user:u", Relation: "cond_group", Condition: valid, Accept: false},
	)

	return cases
}

// invalidTupleCode is the error code Check returns for a contextual tuple
// that does not validate.
const invalidTupleCode = 2027

// RunLayer0 writes every case as a stored tuple, and sends every case as a
// contextual tuple of a Check. Both paths must accept and reject the same tuples.
func RunLayer0(t *testing.T, client Client, scope string) *Outcome {
	t.Helper()
	ctx := context.Background()
	out := newOutcome()
	storeID, modelID := setupStore(t, client, "layer0", layer0Model)

	judge := func(id string, c *WriteCase, accepted bool) {
		switch {
		case c.Gap != nil && accepted:
			out.KnownGaps[c.Gap.Improvement]++
		case c.Gap != nil:
			out.Failures = append(out.Failures, fmt.Sprintf(
				"[GAP CLOSED] %s %s: now rejected as intended; remove the gap for improvement %d (%s)",
				scope, id, c.Gap.Improvement, c.Gap.Summary))
		case accepted == c.Accept:
			out.Passed++
		case accepted:
			out.Failures = append(out.Failures, fmt.Sprintf("[HIGH accepted-should-be-rejected] %s %s", scope, id))
		default:
			out.Failures = append(out.Failures, fmt.Sprintf("[MEDIUM rejected-should-be-accepted] %s %s", scope, id))
		}
	}

	cases := Layer0()
	for i := range cases {
		c := &cases[i]
		tk := Tuple(c.User, c.Relation, fmt.Sprintf("doc:%d", i), c.Condition)

		_, err := client.Write(ctx, &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Writes:               &openfgav1.WriteRequestWrites{TupleKeys: []*openfgav1.TupleKey{tk}},
		})
		judge(c.ID+"/write", c, err == nil)

		if c.WriteOnly {
			continue
		}
		ctxTuple := Tuple(c.User, c.Relation, fmt.Sprintf("doc:ctx%d", i), c.Condition)
		_, err = client.Check(ctx, &openfgav1.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			TupleKey:             &openfgav1.CheckRequestTupleKey{User: "user:u", Relation: c.Relation, Object: ctxTuple.GetObject()},
			ContextualTuples:     &openfgav1.ContextualTupleKeys{TupleKeys: []*openfgav1.TupleKey{ctxTuple}},
		})
		rejected := false
		if err != nil {
			s, ok := status.FromError(err)
			require.True(t, ok)
			rejected = int(s.Code()) == invalidTupleCode
		}
		judge(c.ID+"/contextual", c, !rejected)
	}
	return out
}
