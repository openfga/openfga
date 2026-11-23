package types

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNumericTypeConverterInt64(t *testing.T) {
	t.Run("out_of_range_positive_errors", func(t *testing.T) {
		if _, err := numericTypeConverterFunc[int64]("99999999999999999999"); err == nil {
			t.Fatal("expected error for value above math.MaxInt64, got nil")
		}
	})

	t.Run("out_of_range_negative_errors", func(t *testing.T) {
		if _, err := numericTypeConverterFunc[int64]("-99999999999999999999"); err == nil {
			t.Fatal("expected error for value below math.MinInt64, got nil")
		}
	})

	t.Run("huge_exponent_errors", func(t *testing.T) {
		if _, err := numericTypeConverterFunc[int64]("1e309"); err == nil {
			t.Fatal("expected error for 1e309, got nil")
		}
	})

	t.Run("in_range_positive_ok", func(t *testing.T) {
		got, err := numericTypeConverterFunc[int64]("42")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != int64(42) {
			t.Fatalf("expected int64(42), got %#v", got)
		}
	})

	t.Run("in_range_negative_ok", func(t *testing.T) {
		got, err := numericTypeConverterFunc[int64]("-7")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != int64(-7) {
			t.Fatalf("expected int64(-7), got %#v", got)
		}
	})
}

func TestNumericTypeConverterUint64(t *testing.T) {
	t.Run("above_maxint64_but_valid_uint64_ok", func(t *testing.T) {
		got, err := numericTypeConverterFunc[uint64]("10000000000000000000")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != uint64(10000000000000000000) {
			t.Fatalf("expected uint64(10000000000000000000), got %#v", got)
		}
	})

	t.Run("max_uint64_converts_exactly", func(t *testing.T) {
		got, err := numericTypeConverterFunc[uint64]("18446744073709551615")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != uint64(18446744073709551615) {
			t.Fatalf("expected uint64(18446744073709551615), got %#v", got)
		}
	})

	t.Run("above_maxuint64_errors", func(t *testing.T) {
		if _, err := numericTypeConverterFunc[uint64]("18446744073709551616"); err == nil {
			t.Fatal("expected error for value above math.MaxUint64, got nil")
		}
	})

	t.Run("in_range_ok", func(t *testing.T) {
		got, err := numericTypeConverterFunc[uint64]("42")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got != uint64(42) {
			t.Fatalf("expected uint64(42), got %#v", got)
		}
	})

	t.Run("negative_errors", func(t *testing.T) {
		if _, err := numericTypeConverterFunc[uint64]("-5"); err == nil {
			t.Fatal("expected error for negative value, got nil")
		}
	})
}

func TestNumericTypeConverterFloat64(t *testing.T) {
	t.Run("decimal_strings_convert_to_nearest_float64", func(t *testing.T) {
		// These values have no exact binary representation, so the conversion
		// has to round them instead of rejecting them as unrepresentable.
		tests := map[string]float64{
			"0.1":    0.1,
			"3.14":   3.14,
			"-2.718": -2.718,
			"100.7":  100.7,
			"1e-3":   0.001,
		}
		for input, want := range tests {
			t.Run(input, func(t *testing.T) {
				got, err := numericTypeConverterFunc[float64](input)
				require.NoError(t, err)
				// A delta of 0 compares exactly, so a result that is one ulp
				// off fails.
				require.InDelta(t, want, got, 0)
			})
		}
	})

	t.Run("rounds_once_to_nearest_float64", func(t *testing.T) {
		// 1+2^-53 is exactly halfway between 1 and the next float64, 1+2^-52.
		// A decimal string that is above that midpoint by less than what a
		// 64-bit mantissa can hold has to round up to 1+2^-52. Rounding it to
		// 64 bits first lands exactly on the midpoint, and rounding that to a
		// float64 gives 1 (ties to even), which is one ulp too low.
		nextAfterOne := math.Nextafter(1, 2)
		tests := map[string]float64{
			// Above the midpoint.
			"1.0000000000000001110223024625156540423632": nextAfterOne,
			// Exactly the midpoint: ties to even, which is 1.
			"1.00000000000000011102230246251565404236316680908203125": 1,
			// Below the midpoint.
			"1.0000000000000001110223024625156540423631": 1,
			// Same values, negated.
			"-1.0000000000000001110223024625156540423632": -nextAfterOne,
			"-1.0000000000000001110223024625156540423631": -1,
		}
		for input, want := range tests {
			t.Run(input, func(t *testing.T) {
				got, err := numericTypeConverterFunc[float64](input)
				require.NoError(t, err)
				require.InDelta(t, want, got, 0)

				// It has to give the same float64 as strconv.ParseFloat.
				parsed, err := strconv.ParseFloat(input, 64)
				require.NoError(t, err)
				require.InDelta(t, parsed, got, 0)
			})
		}
	})

	t.Run("max_float64_string_converts", func(t *testing.T) {
		got, err := numericTypeConverterFunc[float64]("1.7976931348623157e308")
		require.NoError(t, err)
		require.InDelta(t, math.MaxFloat64, got, 0)
	})

	t.Run("smallest_float64_string_converts", func(t *testing.T) {
		got, err := numericTypeConverterFunc[float64]("4.9e-324")
		require.NoError(t, err)
		require.InDelta(t, math.SmallestNonzeroFloat64, got, 0)
	})

	t.Run("invalid_strings_error", func(t *testing.T) {
		// Only decimal strings are accepted: not NaN, not hexadecimal floats
		// and not digit separators, which strconv.ParseFloat alone would take.
		for _, input := range []string{"", "abc", "NaN", "nan", "0x1p-2", "1_000", "1.2.3", " 1.5"} {
			_, err := numericTypeConverterFunc[float64](input)
			require.Error(t, err, input)
		}
	})

	t.Run("overflow_errors", func(t *testing.T) {
		for _, input := range []string{"1.79769313486231570814527423731704356798070e+309", "-1.79769313486231570814527423731704356798070e+309"} {
			_, err := numericTypeConverterFunc[float64](input)
			require.Error(t, err)
		}
	})

	t.Run("underflow_to_zero_errors", func(t *testing.T) {
		_, err := numericTypeConverterFunc[float64]("1e-400")
		require.Error(t, err)
	})
}

func TestNumericTypeConverterRejectsNonFinite(t *testing.T) {
	t.Run("rejects_infinity_string_values", func(t *testing.T) {
		for _, input := range []string{"-Inf", "+Inf", "Inf", "-inf"} {
			t.Run(input, func(t *testing.T) {
				_, err := numericTypeConverterFunc[float64](input)
				require.Error(t, err)
			})
		}
	})

	t.Run("rejects_nan_string_value", func(t *testing.T) {
		_, err := numericTypeConverterFunc[float64]("NaN")
		require.Error(t, err)
	})

	t.Run("accepts_finite_string_value", func(t *testing.T) {
		v, err := numericTypeConverterFunc[float64]("2.5")
		require.NoError(t, err)
		require.InDelta(t, 2.5, v.(float64), 1e-9)
	})

	t.Run("accepts_finite_float64_value", func(t *testing.T) {
		v, err := numericTypeConverterFunc[float64](float64(2.5))
		require.NoError(t, err)
		require.InDelta(t, 2.5, v.(float64), 1e-9)
		require.False(t, math.IsInf(v.(float64), 0))
	})
}
