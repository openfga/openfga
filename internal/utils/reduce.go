package utils

func Reduce[S ~[]E, E any, A any](s S, initializer A, f func(A, E) A) A {
	i := initializer
	for _, item := range s {
		i = f(i, item)
	}
	return i
}
