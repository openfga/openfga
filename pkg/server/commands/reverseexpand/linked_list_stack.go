package reverseexpand

// linkedListStack is a stack implementation on top of a linked list, specific to reverse_expand.
// Each
type linkedListStack[T any] struct {
	value T
	next  *linkedListStack[T]
}

func newLinkedListStack[T any](val T) *linkedListStack[T] {
	return &linkedListStack[T]{value: val}
}

func (stack *linkedListStack[T]) push(value T) *linkedListStack[T] {
	newStack := linkedListStack[T]{value: value, next: nil}
	newStack.next = stack
	return &newStack
}

func (stack *linkedListStack[T]) pop() (T, *linkedListStack[T]) {
	return stack.value, stack.next
}

func (stack *linkedListStack[T]) peek() T {
	return stack.value
}
