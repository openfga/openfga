package types

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

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
