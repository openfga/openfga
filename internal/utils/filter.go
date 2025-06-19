package utils

import "iter"

// Filter only yield items that satisfy predicate and return them via sequence.
func Filter[T any](s []T, predicate func(T) bool) iter.Seq[T] {
	return func(yield func(T) bool) {
		for _, item := range s {
			if predicate(item) {
				if !yield(item) {
					// Stop if yield returns false (no more items)
					return
				}
			}
		}
	}
}
