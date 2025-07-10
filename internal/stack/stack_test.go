package stack

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStack(t *testing.T) {
	t.Run("test_push_adds_entry_and_creates_new_stack", func(t *testing.T) {
		firstStack := Push(nil, "hello")
		secondStack := Push(firstStack, "world")
		require.NotEqual(t, Peek(firstStack), Peek(secondStack))
	})

	t.Run("test_pop_does_not_affect_original", func(t *testing.T) {
		firstStack := Push(nil, "hello")
		val, secondStack := Pop(firstStack)
		require.Equal(t, "hello", val)

		// the second stack should be Nil, since we .popped our only element
		require.Nil(t, secondStack)

		// But the first stack should not have been modified
		require.Equal(t, "hello", Peek(firstStack))
	})

	t.Run("test_push_then_pop", func(t *testing.T) {
		firstStack := Push(nil, "hello")
		secondStack := Push(firstStack, "world")

		// This should now have the same value as the first one
		_, thirdStack := Pop(secondStack)
		require.Equal(t, Peek(firstStack), Peek(thirdStack))

		// This Pop removes the last value from this stack
		_, fourth := Pop(thirdStack)
		require.Nil(t, fourth)

		// But does not affect the other stack with the same value
		require.NotNil(t, firstStack)
	})

	t.Run("test_pop_on_empty_stack", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected test to panic and it did not")
			}
		}()

		firstStack := Push(nil, "hello")
		_, secondStack := Pop(firstStack)

		require.Nil(t, secondStack)
		Pop(secondStack) // this line should cause a panic
	})
}
