package utils

// Filter copies values from src to dst that match the
// predicate fn and returns the count of copied values.
// If the length of dst is less than the number of matching
// values from src, iteration will break once dst has been
// filled to its length.
func Filter[T any](dst, src []T, fn func(T) bool) int {
	m := min(len(src), len(dst))
	var i int
	for _, ref := range src {
		if i >= m {
			break
		}
		if fn(ref) {
			dst[i] = ref
			i++
		}
	}
	return i
}
