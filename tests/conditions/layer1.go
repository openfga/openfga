package conditions

import (
	"errors"
	"fmt"
	"net/netip"
	"slices"
	"strings"
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// Expression is one CEL expression over a single parameter named p, together
// with the same logic written in Go. The Go function is the rule the server's
// result is compared with.
type Expression struct {
	// Type is the declared type of p. Empty means p is not declared, which the
	// server treats as string.
	Type string
	// Slug identifies the expression in cell IDs.
	Slug string
	// CEL is the expression text.
	CEL string
	// Eval gives the result for a value that has already been converted to
	// Type. It returns an error where CEL does, for example a missing map key.
	Eval func(v any) (bool, error)
}

var errNoSuchKey = errors.New("no such key")

func onString(f func(string) bool) func(any) (bool, error) {
	return func(v any) (bool, error) { return f(v.(string)), nil }
}

func onInt(f func(int64) bool) func(any) (bool, error) {
	return func(v any) (bool, error) { return f(v.(int64)), nil }
}

func onUint(f func(uint64) bool) func(any) (bool, error) {
	return func(v any) (bool, error) { return f(v.(uint64)), nil }
}

func onDouble(f func(float64) bool) func(any) (bool, error) {
	return func(v any) (bool, error) { return f(v.(float64)), nil }
}

func onBool(f func(bool) bool) func(any) (bool, error) {
	return func(v any) (bool, error) { return f(v.(bool)), nil }
}

func onDuration(f func(time.Duration) bool) func(any) (bool, error) {
	return func(v any) (bool, error) { return f(v.(time.Duration)), nil }
}

func onTime(f func(time.Time) bool) func(any) (bool, error) {
	return func(v any) (bool, error) { return f(v.(time.Time)), nil }
}

func onAddr(f func(netip.Addr) bool) func(any) (bool, error) {
	return func(v any) (bool, error) { return f(v.(netip.Addr)), nil }
}

func onList(f func([]any) bool) func(any) (bool, error) {
	return func(v any) (bool, error) { return f(v.([]any)), nil }
}

// onKey reads key k from a map parameter. CEL returns an error when the key
// is missing, and so does this.
func onKey(k string, f func(any) bool) func(any) (bool, error) {
	return func(v any) (bool, error) {
		item, ok := v.(map[string]any)[k]
		if !ok {
			return false, errNoSuchKey
		}
		return f(item), nil
	}
}

func hasKey(k string) func(any) (bool, error) {
	return func(v any) (bool, error) {
		_, ok := v.(map[string]any)[k]
		return ok, nil
	}
}

func all(items []any, f func(any) bool) bool {
	return !slices.ContainsFunc(items, func(item any) bool { return !f(item) })
}

var (
	year2026  = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	year2027  = time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	tenSlash8 = netip.MustParsePrefix("10.0.0.0/8")
)

// Expressions is the list of expressions Layer 1 runs. Each declared type has
// equality, inequality and, where the type supports them, ordering,
// membership and a function call.
var Expressions = []Expression{
	{Type: "string", Slug: "string-eq", CEL: `p == 'A'`, Eval: onString(func(s string) bool { return s == "A" })},
	{Type: "string", Slug: "string-ne", CEL: `p != 'A'`, Eval: onString(func(s string) bool { return s != "A" })},
	{Type: "string", Slug: "string-lt", CEL: `p < 'B'`, Eval: onString(func(s string) bool { return s < "B" })},
	{Type: "string", Slug: "string-in", CEL: `p in ['A', 'C']`, Eval: onString(func(s string) bool { return s == "A" || s == "C" })},
	{Type: "string", Slug: "string-startswith", CEL: `p.startsWith('A')`, Eval: onString(func(s string) bool { return strings.HasPrefix(s, "A") })},

	{Type: "", Slug: "undeclared-eq", CEL: `p == 'A'`, Eval: onString(func(s string) bool { return s == "A" })},
	{Type: "", Slug: "undeclared-ne", CEL: `p != 'A'`, Eval: onString(func(s string) bool { return s != "A" })},

	{Type: "int", Slug: "int-eq", CEL: `p == 7`, Eval: onInt(func(i int64) bool { return i == 7 })},
	{Type: "int", Slug: "int-ne", CEL: `p != 7`, Eval: onInt(func(i int64) bool { return i != 7 })},
	{Type: "int", Slug: "int-lt", CEL: `p < 8`, Eval: onInt(func(i int64) bool { return i < 8 })},
	{Type: "int", Slug: "int-in", CEL: `p in [7, 9]`, Eval: onInt(func(i int64) bool { return i == 7 || i == 9 })},
	{Type: "int", Slug: "int-eq-2pow53", CEL: `p == 9007199254740992`, Eval: onInt(func(i int64) bool { return i == 1<<53 })},

	{Type: "uint", Slug: "uint-eq", CEL: `p == 7u`, Eval: onUint(func(u uint64) bool { return u == 7 })},
	{Type: "uint", Slug: "uint-ne", CEL: `p != 7u`, Eval: onUint(func(u uint64) bool { return u != 7 })},
	{Type: "uint", Slug: "uint-lt", CEL: `p < 8u`, Eval: onUint(func(u uint64) bool { return u < 8 })},
	{Type: "uint", Slug: "uint-in", CEL: `p in [7u, 9u]`, Eval: onUint(func(u uint64) bool { return u == 7 || u == 9 })},

	{Type: "double", Slug: "double-eq", CEL: `p == 7.0`, Eval: onDouble(func(d float64) bool { return d == 7 })},
	{Type: "double", Slug: "double-ne", CEL: `p != 7.0`, Eval: onDouble(func(d float64) bool { return d != 7 })},
	{Type: "double", Slug: "double-lt", CEL: `p < 8.0`, Eval: onDouble(func(d float64) bool { return d < 8 })},

	{Type: "bool", Slug: "bool-self", CEL: `p`, Eval: onBool(func(b bool) bool { return b })},
	{Type: "bool", Slug: "bool-not", CEL: `!p`, Eval: onBool(func(b bool) bool { return !b })},
	{Type: "bool", Slug: "bool-eq", CEL: `p == true`, Eval: onBool(func(b bool) bool { return b })},

	{Type: "duration", Slug: "duration-eq", CEL: `p == duration('1h')`, Eval: onDuration(func(d time.Duration) bool { return d == time.Hour })},
	{Type: "duration", Slug: "duration-ne", CEL: `p != duration('1h')`, Eval: onDuration(func(d time.Duration) bool { return d != time.Hour })},
	{Type: "duration", Slug: "duration-lt", CEL: `p < duration('2h')`, Eval: onDuration(func(d time.Duration) bool { return d < 2*time.Hour })},

	{Type: "timestamp", Slug: "timestamp-eq", CEL: `p == timestamp('2026-01-01T00:00:00Z')`, Eval: onTime(func(ts time.Time) bool { return ts.Equal(year2026) })},
	{Type: "timestamp", Slug: "timestamp-lt", CEL: `p < timestamp('2027-01-01T00:00:00Z')`, Eval: onTime(func(ts time.Time) bool { return ts.Before(year2027) })},

	{Type: "ipaddress", Slug: "ipaddress-in-cidr", CEL: `p.in_cidr('10.0.0.0/8')`, Eval: onAddr(tenSlash8.Contains)},
	{Type: "ipaddress", Slug: "ipaddress-not-in-cidr", CEL: `!p.in_cidr('10.0.0.0/8')`, Eval: onAddr(func(a netip.Addr) bool { return !tenSlash8.Contains(a) })},

	{Type: "list<string>", Slug: "liststring-contains", CEL: `'A' in p`, Eval: onList(func(l []any) bool { return slices.Contains(l, any("A")) })},
	{Type: "list<string>", Slug: "liststring-size", CEL: `p.size() == 1`, Eval: onList(func(l []any) bool { return len(l) == 1 })},
	{Type: "list<string>", Slug: "liststring-all", CEL: `p.all(x, x == 'A')`, Eval: onList(func(l []any) bool { return all(l, func(x any) bool { return x == "A" }) })},
	{Type: "list<string>", Slug: "liststring-exists", CEL: `p.exists(x, x == 'A')`, Eval: onList(func(l []any) bool { return slices.Contains(l, any("A")) })},

	{Type: "list<int>", Slug: "listint-contains", CEL: `7 in p`, Eval: onList(func(l []any) bool { return slices.Contains(l, any(int64(7))) })},
	{Type: "list<int>", Slug: "listint-all", CEL: `p.all(x, x < 8)`, Eval: onList(func(l []any) bool { return all(l, func(x any) bool { return x.(int64) < 8 }) })},

	{Type: "map<string>", Slug: "mapstring-key-eq", CEL: `p.k == 'A'`, Eval: onKey("k", func(x any) bool { return x == "A" })},
	{Type: "map<string>", Slug: "mapstring-key-ne", CEL: `p.k != 'A'`, Eval: onKey("k", func(x any) bool { return x != "A" })},
	{Type: "map<string>", Slug: "mapstring-has-key", CEL: `'k' in p`, Eval: hasKey("k")},

	{Type: "map<int>", Slug: "mapint-key-eq", CEL: `p.k == 7`, Eval: onKey("k", func(x any) bool { return x == int64(7) })},
	{Type: "map<int>", Slug: "mapint-has-key", CEL: `'k' in p`, Eval: hasKey("k")},
}

// Layer1Result gives the result of one expression for one sent value.
func Layer1Result(e Expression, v SentValue, lenient bool) Result {
	if v.Absent {
		return Error
	}
	typ := e.Type
	if typ == "" {
		typ = "string"
	}
	converted, ok := ConvertValue(typ, v.JSON, lenient)
	if !ok {
		return Error
	}
	met, err := e.Eval(converted)
	switch {
	case err != nil:
		return Error
	case met:
		return Allowed
	default:
		return Denied
	}
}

// Layer1 builds the One tuple layer: every expression against every sent
// value, once where the conditioned tuple is a grant and once where it is a
// deny rule.
//
// Every expression gets its own object type, so a ListObjects or ListUsers
// request evaluates exactly one conditioned tuple.
//
// For KindNamed, cells where the target rule and today's rule differ are left
// out: the proposed changes to numeric conversion apply to $expression only.
//
// A model holds at most 25 conditions, so the expressions are split over
// several groups.
func Layer1(kind Kind) []*Group {
	const perGroup = 20
	var groups []*Group
	for start := 0; start < len(Expressions); start += perGroup {
		end := min(start+perGroup, len(Expressions))
		groups = append(groups, layer1Group(kind, start, end))
	}
	return groups
}

func layer1Group(kind Kind, start, end int) *Group {
	g := &Group{Name: fmt.Sprintf("layer1-%s-%d", kind, start)}

	var model, conditions strings.Builder
	model.WriteString("model\n  schema 1.1\n\ntype user\n")

	for i := start; i < end; i++ {
		e := Expressions[i]
		if kind == KindNamed && e.Type == "" {
			continue // an undeclared parameter only exists for $expression
		}

		objectType := fmt.Sprintf("t%d", i)
		object := objectType + ":1"
		condName := InlineName
		var cond *openfgav1.RelationshipCondition
		if kind == KindNamed {
			condName = fmt.Sprintf("c%d", i)
			cond = NamedCondition(condName)
			fmt.Fprintf(&conditions, "\ncondition %s(p: %s) {\n  %s\n}\n", condName, e.Type, e.CEL)
		} else {
			var params map[string]string
			if e.Type != "" {
				params = map[string]string{"p": e.Type}
			}
			cond = InlineCondition(e.CEL, params)
		}

		fmt.Fprintf(&model, `
type %s
  relations
    define grant: [user with %s]
    define base: [user]
    define blocked: [user with %s]
    define deny: base but not blocked
`, objectType, condName, condName)

		g.Tuples = append(g.Tuples,
			Tuple("user:u", "grant", object, cond),
			Tuple("user:u", "base", object, nil),
			Tuple("user:u", "blocked", object, cond),
		)

		for _, v := range SentValues {
			want := Layer1Result(e, v, false)
			today := Layer1Result(e, v, true)

			var gap *Gap
			if want != today {
				if kind == KindNamed {
					continue
				}
				gap = GapNumericStrings
				if _, isNumber := v.JSON.(float64); isNumber {
					gap = GapUnsafeIntegers
				}
			}

			var reqCtx map[string]any
			if !v.Absent {
				reqCtx = map[string]any{"p": v.JSON}
			}

			id := fmt.Sprintf("L1/%s/%s/%s", kind, e.Slug, v.Name)
			g.Cells = append(g.Cells,
				Cell{ID: id + "/grant", User: "user:u", Relation: "grant", Object: object, Context: reqCtx, Want: want, Gap: gap, Today: today},
				Cell{ID: id + "/deny", User: "user:u", Relation: "deny", Object: object, Context: reqCtx, Want: want.Not(), Gap: gap, Today: today.Not()},
			)
		}
	}

	g.ModelDSL = model.String() + conditions.String()
	return g
}
