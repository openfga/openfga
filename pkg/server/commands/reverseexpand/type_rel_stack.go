package reverseexpand

// typeRelStack is a stack implementation on top of a linked list, specific to reverse_expand.
//
// *Important*: Each push() or pop() operation creates and returns a pointer to a new stack entirely to
// ensure thread safety, since ReverseExpand kicks off many routines all relying on their own stacks.
type typeRelStack struct {
	value typeRelEntry
	next  *typeRelStack
}

func newTypeRelStack(val typeRelEntry) *typeRelStack {
	return &typeRelStack{value: val}
}

// push does not modify the stack in place. Instead, it creates a new stack assigning it the passed value and pointing
// at the previously-existing stack as .next.
func (stack *typeRelStack) push(value typeRelEntry) *typeRelStack {
	return &typeRelStack{value: value, next: stack}
}

// pop does not modify the stack in place. Instead, it returns the current value of the stack and a pointer to the
// next stack starting at the current stack.next.
func (stack *typeRelStack) pop() (typeRelEntry, *typeRelStack) {
	return stack.value, stack.next
}

func (stack *typeRelStack) peek() typeRelEntry {
	return stack.value
}
