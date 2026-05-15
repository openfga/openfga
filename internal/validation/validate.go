package validation

// Validator is a predicate that reports whether a value of type T is
// valid. It returns false or a non-nil error to indicate rejection.
type Validator[T any] func(T) (bool, error)

// ValidatorFunc converts fn into a [Validator].
func ValidatorFunc[T any](fn func(T) (bool, error)) Validator[T] {
	return Validator[T](fn)
}

// MakeFallible adapts a boolean predicate into a [Validator] that
// always returns a nil error.
func MakeFallible[T any](fn func(T) bool) Validator[T] {
	return func(t T) (bool, error) {
		return fn(t), nil
	}
}

// CombineValidators returns a [Validator] that runs each of the given
// validators in order and short-circuits on the first rejection or
// error. Nil entries are skipped. If all validators pass (or the list
// is empty), the combined validator returns true, nil.
func CombineValidators[T any](validators ...Validator[T]) Validator[T] {
	return func(value T) (bool, error) {
		for _, v := range validators {
			if v == nil {
				continue
			}

			ok, err := v(value)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
}
