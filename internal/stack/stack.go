package stack

// Stack is an implementation of a stack based on a linked list.
//
// *Important*: Each push() or pop() operation creates and returns a pointer to a new stack entirely to
// ensure thread safety.
type Stack[T any] struct {
	Value T
	next  *Stack[T]
}

func Push[T any](stack *Stack[T], value T) *Stack[T] {
	return &Stack[T]{Value: value, next: stack}
}

func Pop[T any](stack *Stack[T]) (T, *Stack[T]) {
	return stack.Value, stack.next
}
