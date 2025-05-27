package utils

func Filter[S ~[]E, E any](s S, f func(E) bool) []E {
	var filteredItems []E
	for _, item := range s {
		if f(item) {
			filteredItems = append(filteredItems, item)
		}
	}
	return filteredItems
}
