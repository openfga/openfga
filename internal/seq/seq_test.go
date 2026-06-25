package seq

import (
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
)

// collect drains an iter.Seq into a slice.
func collect[T any](s iter.Seq[T]) []T {
	var out []T
	for v := range s {
		out = append(out, v)
	}
	return out
}

func TestSequence(t *testing.T) {
	t.Run("yields_in_order", func(t *testing.T) {
		got := collect(Sequence(1, 2, 3))
		require.Equal(t, []int{1, 2, 3}, got)
	})

	t.Run("empty", func(t *testing.T) {
		got := collect(Sequence[int]())
		require.Empty(t, got)
	})

	t.Run("stops_early_when_consumer_breaks", func(t *testing.T) {
		var seen []int
		for v := range Sequence(1, 2, 3, 4) {
			seen = append(seen, v)
			if v == 2 {
				break
			}
		}
		require.Equal(t, []int{1, 2}, seen)
	})
}

func TestChannel(t *testing.T) {
	t.Run("yields_values_from_channel", func(t *testing.T) {
		ch := make(chan string, 3)
		ch <- "a"
		ch <- "b"
		ch <- "c"
		close(ch)

		got := collect(Channel(ch))
		require.Equal(t, []string{"a", "b", "c"}, got)
	})

	t.Run("stops_early_when_consumer_breaks", func(t *testing.T) {
		ch := make(chan int, 5)
		for i := 0; i < 5; i++ {
			ch <- i
		}
		close(ch)

		var seen []int
		for v := range Channel(ch) {
			seen = append(seen, v)
			if v == 1 {
				break
			}
		}
		require.Equal(t, []int{0, 1}, seen)
	})

	t.Run("empty_closed_channel", func(t *testing.T) {
		ch := make(chan int)
		close(ch)
		require.Empty(t, collect(Channel(ch)))
	})
}

func TestFlatten(t *testing.T) {
	t.Run("merges_in_order", func(t *testing.T) {
		got := collect(Flatten(Sequence(1, 2), Sequence(3), Sequence(4, 5)))
		require.Equal(t, []int{1, 2, 3, 4, 5}, got)
	})

	t.Run("no_sequences", func(t *testing.T) {
		require.Empty(t, collect(Flatten[int]()))
	})

	t.Run("stops_early_across_boundaries", func(t *testing.T) {
		var seen []int
		for v := range Flatten(Sequence(1, 2), Sequence(3, 4)) {
			seen = append(seen, v)
			if v == 3 {
				break
			}
		}
		require.Equal(t, []int{1, 2, 3}, seen)
	})
}

func TestTransform(t *testing.T) {
	t.Run("maps_values", func(t *testing.T) {
		got := collect(Transform(Sequence(1, 2, 3), func(i int) int { return i * 10 }))
		require.Equal(t, []int{10, 20, 30}, got)
	})

	t.Run("changes_type", func(t *testing.T) {
		got := collect(Transform(Sequence(1, 2), func(i int) string {
			if i == 1 {
				return "one"
			}
			return "two"
		}))
		require.Equal(t, []string{"one", "two"}, got)
	})

	t.Run("stops_early", func(t *testing.T) {
		var seen []int
		for v := range Transform(Sequence(1, 2, 3), func(i int) int { return i }) {
			seen = append(seen, v)
			if v == 1 {
				break
			}
		}
		require.Equal(t, []int{1}, seen)
	})
}

func TestFilter(t *testing.T) {
	t.Run("keeps_matching", func(t *testing.T) {
		got := collect(Filter(Sequence(1, 2, 3, 4, 5), func(i int) bool { return i%2 == 0 }))
		require.Equal(t, []int{2, 4}, got)
	})

	t.Run("predicate_never_true", func(t *testing.T) {
		require.Empty(t, collect(Filter(Sequence(1, 3, 5), func(i int) bool { return i%2 == 0 })))
	})

	t.Run("stops_early_on_matching_value", func(t *testing.T) {
		var seen []int
		for v := range Filter(Sequence(1, 2, 3, 4), func(i int) bool { return i%2 == 0 }) {
			seen = append(seen, v)
			break
		}
		require.Equal(t, []int{2}, seen)
	})
}

func TestSeqReader_Read(t *testing.T) {
	t.Run("fills_buffer_fully", func(t *testing.T) {
		r := NewSeqReader(Sequence(1, 2, 3, 4, 5))
		buf := make([]int, 3)
		n := r.Read(buf)
		require.Equal(t, 3, n)
		require.Equal(t, []int{1, 2, 3}, buf)
	})

	t.Run("multiple_reads_exhaust_sequence", func(t *testing.T) {
		r := NewSeqReader(Sequence(1, 2, 3, 4, 5))
		buf := make([]int, 2)

		require.Equal(t, 2, r.Read(buf))
		require.Equal(t, []int{1, 2}, buf)

		require.Equal(t, 2, r.Read(buf))
		require.Equal(t, []int{3, 4}, buf)

		// Third read only has one element left.
		n := r.Read(buf)
		require.Equal(t, 1, n)
		require.Equal(t, 5, buf[0])

		// Sequence is now complete; subsequent reads return 0.
		require.Equal(t, 0, r.Read(buf))
	})

	t.Run("partial_fill_when_fewer_items_than_buffer", func(t *testing.T) {
		r := NewSeqReader(Sequence(1, 2))
		buf := make([]int, 10)
		n := r.Read(buf)
		require.Equal(t, 2, n)
		require.Equal(t, []int{1, 2}, buf[:n])
	})

	t.Run("empty_sequence_returns_zero", func(t *testing.T) {
		r := NewSeqReader(Sequence[int]())
		buf := make([]int, 4)
		require.Equal(t, 0, r.Read(buf))
	})

	t.Run("zero_length_buffer_reads_nothing", func(t *testing.T) {
		r := NewSeqReader(Sequence(1, 2, 3))
		require.Equal(t, 0, r.Read([]int{}))
	})
}

func TestSeqReader_Close(t *testing.T) {
	r := NewSeqReader(Sequence(1, 2, 3))
	buf := make([]int, 1)
	require.Equal(t, 1, r.Read(buf))

	require.NoError(t, r.Close())

	// Closing again is safe (stop is idempotent in iter.Pull semantics).
	require.NoError(t, r.Close())
}
