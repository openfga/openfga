package conditions

import (
	"math"
	"math/big"
	"net/netip"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// SentValue is one kind of value a caller can put in the request context.
type SentValue struct {
	// Name identifies the value in cell IDs.
	Name string
	// Absent means the parameter is left out of the context.
	Absent bool
	// JSON is the value as it appears in a JSON request: a string, a float64
	// (every JSON number), a bool, nil, a []any or a map[string]any.
	JSON any
}

// SentValues is the list of values every Layer 1 expression is checked against.
// It covers matching and non-matching values of each type, the string forms of
// numbers, values at the edge of exact integer range, and values of the wrong
// shape.
var SentValues = []SentValue{
	{Name: "absent", Absent: true},
	{Name: "null", JSON: nil},

	{Name: `str:A`, JSON: "A"},
	{Name: `str:B`, JSON: "B"},
	{Name: `str:empty`, JSON: ""},
	{Name: `str:7`, JSON: "7"},
	{Name: `str:8`, JSON: "8"},
	{Name: `str:007`, JSON: "007"},
	{Name: `str:7.0`, JSON: "7.0"},
	{Name: `str:7e0`, JSON: "7e0"},
	{Name: `str:+7`, JSON: "+7"},
	{Name: `str:space7`, JSON: " 7"},
	{Name: `str:true`, JSON: "true"},
	{Name: `str:1h`, JSON: "1h"},
	{Name: `str:90m`, JSON: "90m"},
	{Name: `str:2026-01-01T00:00:00Z`, JSON: "2026-01-01T00:00:00Z"},
	{Name: `str:2026-01-01T01:00:00+01:00`, JSON: "2026-01-01T01:00:00+01:00"},
	{Name: `str:2026-01-01`, JSON: "2026-01-01"},
	{Name: `str:10.1.2.3`, JSON: "10.1.2.3"},
	{Name: `str:8.8.8.8`, JSON: "8.8.8.8"},
	{Name: `str:ffff-mapped-10.1.2.3`, JSON: "::ffff:10.1.2.3"},
	{Name: `str:9007199254740993`, JSON: "9007199254740993"},

	{Name: "num:7", JSON: float64(7)},
	{Name: "num:8", JSON: float64(8)},
	{Name: "num:7.5", JSON: 7.5},
	{Name: "num:-7", JSON: float64(-7)},
	{Name: "num:0", JSON: float64(0)},
	// 2^53 + 1. A JSON number cannot hold it; it arrives as 2^53.
	{Name: "num:9007199254740993", JSON: float64(9007199254740993)},

	{Name: "bool:true", JSON: true},
	{Name: "bool:false", JSON: false},

	{Name: "list:[A]", JSON: []any{"A"}},
	{Name: "list:[A,B]", JSON: []any{"A", "B"}},
	{Name: "list:[7]", JSON: []any{float64(7)}},
	{Name: "list:[7,8]", JSON: []any{float64(7), float64(8)}},
	{Name: "list:[A,7]", JSON: []any{"A", float64(7)}},
	{Name: "list:[str007]", JSON: []any{"007"}},
	{Name: "list:[]", JSON: []any{}},

	{Name: "map:{k:A}", JSON: map[string]any{"k": "A"}},
	{Name: "map:{k:B}", JSON: map[string]any{"k": "B"}},
	{Name: "map:{k:7}", JSON: map[string]any{"k": float64(7)}},
	{Name: "map:{k:str007}", JSON: map[string]any{"k": "007"}},
	{Name: "map:{j:A}", JSON: map[string]any{"j": "A"}},
	{Name: "map:{}", JSON: map[string]any{}},
}

// maxSafeInteger is 2^53. A JSON number at or above it may already have been
// rounded, so the target rule refuses it for integer parameters.
const maxSafeInteger = float64(1 << 53)

var (
	canonicalInt  = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)
	canonicalUint = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)
)

// ConvertValue applies the rule for turning a JSON value into a parameter of
// the declared type. It returns false when the value does not fit the type.
//
// With lenient false it applies the target rule: a value must have the
// declared type, with one exception, a canonical integer string for int and
// uint. With lenient true it applies the rule the server uses today for
// numeric types, which parses any decimal string and accepts rounded integers.
// The two rules are the same for every other type.
func ConvertValue(typ string, v any, lenient bool) (any, bool) {
	switch typ {
	case "string":
		s, ok := v.(string)
		return s, ok
	case "bool":
		b, ok := v.(bool)
		return b, ok
	case "int":
		return convertInt(v, lenient)
	case "uint":
		return convertUint(v, lenient)
	case "double":
		return convertDouble(v, lenient)
	case "duration":
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		d, err := time.ParseDuration(s)
		return d, err == nil
	case "timestamp":
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		ts, err := time.Parse(time.RFC3339, s)
		return ts, err == nil
	case "ipaddress":
		s, ok := v.(string)
		if !ok {
			return nil, false
		}
		addr, err := netip.ParseAddr(s)
		return addr.Unmap(), err == nil
	}

	if elem, ok := genericElement(typ, "list"); ok {
		items, ok := v.([]any)
		if !ok {
			return nil, false
		}
		out := make([]any, 0, len(items))
		for _, item := range items {
			converted, ok := ConvertValue(elem, item, lenient)
			if !ok {
				return nil, false
			}
			out = append(out, converted)
		}
		return out, true
	}

	if elem, ok := genericElement(typ, "map"); ok {
		items, ok := v.(map[string]any)
		if !ok {
			return nil, false
		}
		out := make(map[string]any, len(items))
		for key, item := range items {
			converted, ok := ConvertValue(elem, item, lenient)
			if !ok {
				return nil, false
			}
			out[key] = converted
		}
		return out, true
	}

	panic("conditions matrix: unknown parameter type " + typ)
}

func genericElement(typ, container string) (string, bool) {
	rest, ok := strings.CutPrefix(typ, container+"<")
	if !ok {
		return "", false
	}
	return strings.TrimSuffix(rest, ">"), true
}

func convertInt(v any, lenient bool) (any, bool) {
	switch x := v.(type) {
	case float64:
		if x != math.Trunc(x) {
			return nil, false
		}
		if !lenient && math.Abs(x) >= maxSafeInteger {
			return nil, false
		}
		return int64(x), true
	case string:
		if !lenient {
			if !canonicalInt.MatchString(x) {
				return nil, false
			}
			i, err := strconv.ParseInt(x, 10, 64)
			return i, err == nil
		}
		f, ok := parseDecimal(x)
		if !ok || !f.IsInt() {
			return nil, false
		}
		i, acc := f.Int64()
		return i, acc == big.Exact
	}
	return nil, false
}

func convertUint(v any, lenient bool) (any, bool) {
	switch x := v.(type) {
	case float64:
		if x != math.Trunc(x) || x < 0 {
			return nil, false
		}
		if !lenient && x >= maxSafeInteger {
			return nil, false
		}
		return uint64(x), true
	case string:
		if !lenient {
			if !canonicalUint.MatchString(x) {
				return nil, false
			}
			u, err := strconv.ParseUint(x, 10, 64)
			return u, err == nil
		}
		f, ok := parseDecimal(x)
		if !ok || !f.IsInt() || f.Sign() < 0 {
			return nil, false
		}
		u, acc := f.Uint64()
		return u, acc == big.Exact
	}
	return nil, false
}

func convertDouble(v any, lenient bool) (any, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case string:
		if !lenient {
			return nil, false
		}
		f, ok := parseDecimal(x)
		if !ok {
			return nil, false
		}
		d, acc := f.Float64()
		return d, acc == big.Exact && !math.IsInf(d, 0)
	}
	return nil, false
}

func parseDecimal(s string) (*big.Float, bool) {
	f, _, err := big.ParseFloat(s, 10, 64, 0)
	if err != nil || f.IsInf() {
		return nil, false
	}
	return f, true
}
