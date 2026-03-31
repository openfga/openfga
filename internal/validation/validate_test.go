package validation_test

import (
	"errors"
	"testing"

	"github.com/openfga/openfga/internal/validation"
)

func TestMakeFallible(t *testing.T) {
	double := func(n int) bool { return (n * 2) == 4 }
	fn := validation.MakeFallible(double)

	got, err := fn(3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != false {
		t.Fatal("got true, want false")
	}
}

func TestCombineValidatorsAllPass(t *testing.T) {
	positive := func(n int) (bool, error) { return n > 0, nil }
	even := func(n int) (bool, error) { return n%2 == 0, nil }

	combined := validation.CombineValidators(positive, even)

	ok, err := combined(4)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true for value 4")
	}
}

func TestCombineValidatorsShortCircuitsOnFalse(t *testing.T) {
	called := false
	first := func(int) (bool, error) { return false, nil }
	second := func(int) (bool, error) {
		called = true
		return true, nil
	}

	combined := validation.CombineValidators(first, second)

	ok, err := combined(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false when first validator rejects")
	}
	if called {
		t.Fatal("second validator should not have been called")
	}
}

func TestCombineValidatorsShortCircuitsOnError(t *testing.T) {
	sentinel := errors.New("bad input")
	called := false
	first := func(int) (bool, error) { return false, sentinel }
	second := func(int) (bool, error) {
		called = true
		return true, nil
	}

	combined := validation.CombineValidators(first, second)

	ok, err := combined(1)
	if !errors.Is(err, sentinel) {
		t.Fatalf("got error %v, want %v", err, sentinel)
	}
	if ok {
		t.Fatal("expected false when first validator errors")
	}
	if called {
		t.Fatal("second validator should not have been called")
	}
}

func TestCombineValidatorsEmpty(t *testing.T) {
	combined := validation.CombineValidators[int](nil)

	ok, err := combined(42)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected true with no validators")
	}
}

func TestCombineValidatorsSecondRejects(t *testing.T) {
	pass := func(int) (bool, error) { return true, nil }
	reject := func(int) (bool, error) { return false, nil }

	combined := validation.CombineValidators(pass, reject)

	ok, err := combined(1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false when second validator rejects")
	}
}
