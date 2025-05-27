package utils

// Filter functions similar to other language list filter functions.
// It accepts a generic with underlying type slice and a func to apply to each element of that slice,
// returning a filtered slice containing only the elements for which the supplied function returned true.
//
// Example filtering for even numbers:
//
//	Filter([]int{1, 2, 3, 4, 5}, func(n int) bool { return n%2 == 0})
//	returns []int{2, 4}
func Filter[S ~[]E, E any](s S, f func(E) bool) []E {
	var filteredItems []E
	for _, item := range s {
		if f(item) {
			filteredItems = append(filteredItems, item)
		}
	}
	return filteredItems
}
