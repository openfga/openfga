package errors

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

var errSentinelError = fmt.Errorf("sentinel error")

type fooError struct {
	message string
}

var _ error = (*fooError)(nil)

func (f *fooError) Error() string {
	return f.message
}

func TestWith(t *testing.T) {
	fooErr := &fooError{message: "foo"}
	require.NotErrorIs(t, fooErr, errSentinelError)
	sentinelFooErr := With(fooErr, errSentinelError)
	require.ErrorIs(t, sentinelFooErr, errSentinelError)

	barErr := fmt.Errorf("bar: %w", errSentinelError)
	require.ErrorIs(t, barErr, errSentinelError)
	sentinelBarErr := With(barErr, errSentinelError)
	require.ErrorIs(t, sentinelBarErr, errSentinelError)
}

func ExampleWith() {
	sentinelError := fmt.Errorf("some concrete error value")

	fooErr := &fooError{message: "foo"}
	if !errors.Is(fooErr, sentinelError) {
		fmt.Println("1")
	}

	sentinelFooErr := With(fooErr, sentinelError)
	if errors.Is(sentinelFooErr, sentinelError) {
		fmt.Println("2")
	}

	// Output: 1
	// 2
}
