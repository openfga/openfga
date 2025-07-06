package utils

// Some docstring explaining how each of these methods actually returns a new stack
// for thread safety
type LinkedListStack[T any] struct {
	value T
	next  *LinkedListStack[T]
}

func (stack *LinkedListStack[T]) Push(value T) *LinkedListStack[T] {
	newStack := LinkedListStack[T]{value: value, next: nil}
	newStack.next = stack
	return &newStack
}

func (stack *LinkedListStack[T]) Pop() (T, *LinkedListStack[T]) {
	return stack.value, stack.next
}

func (stack *LinkedListStack[T]) Peek() T {
	return stack.value
}
