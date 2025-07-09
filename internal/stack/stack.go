package stack

// Stack is an implementation of a stack based on a linked list.
//
// *Important*: Each push() or pop() operation creates and returns a pointer to a new stack entirely to
// ensure thread safety.
type Stack[T any] struct {
	value T
	next  *Stack[T]
}

func New[T any](value T) *Stack[T] {
	return &Stack[T]{value: value}
}

func Push[T any](stack *Stack[T], value T) *Stack[T] {
	return &Stack[T]{value: value, next: stack}
}

func Pop[T any](stack *Stack[T]) (T, *Stack[T]) {
	return stack.value, stack.next
}

func Peek[T any](stack *Stack[T]) T {
	return stack.value
}
