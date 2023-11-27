package errors

import (
	"errors"
	reflectlite "reflect"
)

// With returns an error that represents the top error wrapped on top of the base error.
//
// It should be used to return a rich error (e.g. non-sentinel) with specific details but
// where you want to check for sentinel error equality, which the Go stdlib error wrapping
// directives do not allow because of iterative unwrapping. See union#Is below for more info.
//
// This was inspired by the work of https://github.com/natefinch/wrap but varies slightly.
func With(base, top error) error {
	if base == nil && top == nil {
		return nil
	}
	if top == nil {
		return base
	}
	if base == nil {
		return top
	}
	return union{error: base, top: top}
}

type union struct {
	error
	top error
}

func (u union) Is(target error) bool {
	// Copied from errors.Is, but without iterative unwrapping.
	// If top doesn't match, errors.Is will Unwrap, which does the right thing.
	if target == nil {
		return false
	}

	isComparable := reflectlite.TypeOf(target).Comparable()
	if isComparable && u.top == target {
		return true
	}
	if x, ok := u.top.(interface{ Is(error) bool }); ok && x.Is(target) {
		return true
	}
	return false
}

func (u union) As(target any) bool {
	// copied from errors.As, but without the iterative unwrapping.
	// If top doesn't match, errors.Is will Unwrap, which does the right thing.

	if target == nil {
		panic("errors: target cannot be nil")
	}
	val := reflectlite.ValueOf(target)
	typ := val.Type()
	if typ.Kind() != reflectlite.Ptr || val.IsNil() {
		panic("errors: target must be a non-nil pointer")
	}
	targetType := typ.Elem()
	if targetType.Kind() != reflectlite.Interface && !targetType.Implements(errorType) {
		panic("errors: *target must be interface or implement error")
	}
	if reflectlite.TypeOf(u.top).AssignableTo(targetType) {
		val.Elem().Set(reflectlite.ValueOf(u.top))
		return true
	}
	if x, ok := u.top.(interface{ As(any) bool }); ok && x.As(target) {
		return true
	}
	return false
}

var errorType = reflectlite.TypeOf((*error)(nil)).Elem()

func (u union) Unwrap() error {
	if err := errors.Unwrap(u.top); err != nil {
		return union{error: u.error, top: err}
	}
	// otherwise we ran out of errors on top to unwrap, so return the underlying error.
	return u.error
}
