package validation

type Validator[T any] func(T) (bool, error)

func ValidatorFunc[T any](fn func(T) (bool, error)) Validator[T] {
	return Validator[T](fn)
}

func MakeFallible[T any](fn func(T) bool) Validator[T] {
	return func(t T) (bool, error) {
		return fn(t), nil
	}
}

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
