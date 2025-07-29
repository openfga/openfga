package stack

import "fmt"

// Stack is an implementation of a stack based on a linked list.
//
// *Important*: Each push() or pop() operation creates and returns a pointer to a new stack entirely to
// ensure thread safety.
type node[T any] struct {
	value T
	next  *node[T]
}

type Stack[T any] *node[T]

func Push[T any](stack Stack[T], value T) Stack[T] {
	return Stack[T](&node[T]{value: value, next: (*node[T])(stack)})
}

func Pop[T any](stack Stack[T]) (T, Stack[T]) {
	return stack.value, Stack[T](stack.next)
}

func Peek[T any](stack Stack[T]) T {
	return stack.value
}

func Len[T any](stack Stack[T]) int {
	var ctr int
	s := stack
	for s != nil {
		ctr++
		s = s.next
	}
	return ctr
}

func Print[T any](stack Stack[T]) string {
	stackStr := ""
	var val T
	for stack != nil {
		val, stack = Pop(stack)
		stackStr += fmt.Sprintf("%v", val)
	}
	return stackStr
}
