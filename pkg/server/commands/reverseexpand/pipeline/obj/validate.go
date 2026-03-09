package obj

import (
	"context"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/checkutil"
	"github.com/openfga/openfga/internal/validation"
	"github.com/openfga/openfga/pkg/typesystem"
)

func MakeFallible[T, U any](fn func(T) U) func(T) (U, error) {
	return func(t T) (U, error) {
		return fn(t), nil
	}
}

func CombineValidators[T any](validators []func(T) (bool, error)) func(T) (bool, error) {
	return func(value T) (bool, error) {
		for _, v := range validators {
			ok, err := v(value)
			if err != nil || !ok {
				return ok, err
			}
		}
		return true, nil
	}
}

func NewValidator(
	ctx context.Context,
	ts *typesystem.TypeSystem,
	obj *structpb.Struct,
) func(*openfgav1.TupleKey) (bool, error) {
	return CombineValidators(
		[]func(*openfgav1.TupleKey) (bool, error){
			checkutil.BuildTupleKeyConditionFilter(
				ctx,
				obj,
				ts,
			),
			MakeFallible(
				validation.FilterInvalidTuples(
					ts,
				),
			),
		},
	)
}
