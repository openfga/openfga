package conditions

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// These tests check the rules the matrix compares the server with. They do
// not start a server.

func TestThreeValuedLogic(t *testing.T) {
	T, F, U := triTrue, triFalse, triUnknown

	// A false operand decides an intersection and a true operand decides a
	// union, whatever the other operand is.
	require.Equal(t, F, triAnd(F, U))
	require.Equal(t, F, triAnd(U, F))
	require.Equal(t, T, triOr(T, U))
	require.Equal(t, T, triOr(U, T))

	// Otherwise unknown stays unknown. It never becomes an allow.
	require.Equal(t, U, triAnd(T, U))
	require.Equal(t, U, triOr(F, U))
	require.Equal(t, U, triNot(U))

	require.Equal(t, T, triAnd(T, T))
	require.Equal(t, F, triOr(F, F))
	require.Equal(t, F, triNot(T))
	require.Equal(t, T, triNot(F))

	require.Equal(t, Allowed, T.result())
	require.Equal(t, Denied, F.result())
	require.Equal(t, Error, U.result())
	require.Equal(t, Error, Error.Not())
}

func TestConvertValue(t *testing.T) {
	tests := []struct {
		typ     string
		value   any
		target  bool // fits under the target rule
		lenient bool // fits under the rule the server uses today
	}{
		{"string", "A", true, true},
		{"string", float64(7), false, false},
		{"string", nil, false, false},

		{"int", float64(7), true, true},
		{"int", 7.5, false, false},
		{"int", "7", true, true},
		{"int", "-7", true, true},
		{"int", "007", false, true},
		{"int", "7.0", false, true},
		{"int", "7e0", false, true},
		{"int", "+7", false, true},
		{"int", " 7", false, false},
		{"int", "A", false, false},
		{"int", true, false, false},
		{"int", float64(1 << 53), false, true},
		{"int", float64(1<<53 - 1), true, true},
		{"int", "9007199254740993", true, true},

		{"uint", float64(7), true, true},
		{"uint", float64(-7), false, false},
		{"uint", "-7", false, false},
		{"uint", "007", false, true},

		{"double", 7.5, true, true},
		{"double", "7.5", false, true},
		{"double", "7", false, true},

		{"bool", true, true, true},
		{"bool", "true", false, false},
		{"bool", float64(1), false, false},

		{"duration", "1h", true, true},
		{"duration", float64(3600), false, false},
		{"timestamp", "2026-01-01T00:00:00Z", true, true},
		{"timestamp", "2026-01-01", false, false},
		{"ipaddress", "10.1.2.3", true, true},
		{"ipaddress", "10.1.2.3/8", false, false},

		{"list<string>", []any{"A", "B"}, true, true},
		{"list<string>", []any{"A", float64(7)}, false, false},
		{"list<string>", "A", false, false},
		{"list<int>", []any{float64(7)}, true, true},
		{"list<int>", []any{"007"}, false, true},
		{"map<string>", map[string]any{"k": "A"}, true, true},
		{"map<string>", map[string]any{"k": float64(7)}, false, false},
		{"map<int>", map[string]any{"k": "007"}, false, true},
	}
	for _, tc := range tests {
		_, ok := ConvertValue(tc.typ, tc.value, false)
		require.Equalf(t, tc.target, ok, "target rule: %s <- %#v", tc.typ, tc.value)
		_, ok = ConvertValue(tc.typ, tc.value, true)
		require.Equalf(t, tc.lenient, ok, "today's rule: %s <- %#v", tc.typ, tc.value)
	}
}

func TestJudge(t *testing.T) {
	gap := &Gap{Improvement: 1, Summary: "test gap"}

	t.Run("a result equal to the expected result passes", func(t *testing.T) {
		out := newOutcome()
		out.judge("s", &Cell{ID: "c", Want: Denied}, APICheck, Denied)
		require.Equal(t, 1, out.Passed)
		require.Empty(t, out.Failures)
	})

	t.Run("an allow that should not be is critical", func(t *testing.T) {
		out := newOutcome()
		out.judge("s", &Cell{ID: "c", Want: Error}, APICheck, Allowed)
		require.Len(t, out.Failures, 1)
		require.Contains(t, out.Failures[0], "CRITICAL")
	})

	t.Run("an error is accepted only for the APIs a provisional cell names", func(t *testing.T) {
		cell := &Cell{ID: "c", Want: Allowed, ErrorAlsoOK: []API{APIListUsers}, Provisional: "reason"}
		out := newOutcome()
		out.judge("s", cell, APIListUsers, Error)
		require.Equal(t, 1, out.Provisional["reason"])
		require.Empty(t, out.Failures)

		out.judge("s", cell, APICheck, Error)
		require.Len(t, out.Failures, 1)
		require.Contains(t, out.Failures[0], "MEDIUM")
	})

	t.Run("a provisional cell never accepts an allow that should not be", func(t *testing.T) {
		out := newOutcome()
		out.judge("s", &Cell{ID: "c", Want: Denied, ErrorAlsoOK: AllAPIs, Provisional: "reason"}, APICheck, Allowed)
		require.Len(t, out.Failures, 1)
		require.Contains(t, out.Failures[0], "CRITICAL")
	})

	t.Run("a known gap passes only while the server gives today's result", func(t *testing.T) {
		cell := &Cell{ID: "c", Want: Error, Gap: gap, Today: Allowed}

		out := newOutcome()
		out.judge("s", cell, APICheck, Allowed)
		require.Equal(t, 1, out.KnownGaps[1])
		require.Empty(t, out.Failures)

		out.judge("s", cell, APICheck, Error)
		require.Len(t, out.Failures, 1)
		require.Contains(t, out.Failures[0], "GAP CLOSED")

		out.judge("s", cell, APICheck, Denied)
		require.Len(t, out.Failures, 2)
		require.Contains(t, out.Failures[1], "known gap expects allowed today")
	})
}

func TestLayerSizes(t *testing.T) {
	// The generators decide the size of the matrix. These bounds catch a
	// generator that silently stops producing cases.
	cells := 0
	for _, g := range Layer1(KindExpression) {
		cells += len(g.Cells)
	}
	require.Equal(t, len(Expressions)*len(SentValues)*2, cells)

	require.Len(t, Layer2(KindExpression, secondNone).Cells, len(slots)*len(wrappers)*len(userOutcomes))
	require.Len(t, Layer2(KindExpression, secondBefore).Cells, 2*len(wrappers)*len(userOutcomes)*len(otherOutcomes))
	require.Greater(t, len(Layer0()), 100)
}
