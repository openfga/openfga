package stack

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStack(t *testing.T) {
	t.Run("test_push_adds_entry_and_creates_new_stack", func(t *testing.T) {
		firstStack := &Stack[string]{Value: "hello"}
		secondStack := Push(firstStack, "world")

		require.NotEqual(t, firstStack.Value, secondStack.Value)
	})

	t.Run("test_pop_does_not_affect_original", func(t *testing.T) {
		firstStack := &Stack[string]{Value: "hello"}

		val, secondStack := Pop(firstStack)
		require.Equal(t, "hello", val)

		// the second stack should be Nil, since we .popped our only element
		require.Nil(t, secondStack)

		// But the first stack should not have been modified
		require.Equal(t, "hello", firstStack.Value)
	})

	t.Run("test_pop_on_empty_stack", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected test to panic and it did not")
			}
		}()

		firstStack := &Stack[string]{Value: "hello"}
		_, secondStack := Pop(firstStack)

		require.Nil(t, secondStack)
		Pop(secondStack) // this line should cause a panic
	})
}
