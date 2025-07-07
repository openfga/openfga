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

func (stack *typeRelStack) push(value typeRelEntry) *typeRelStack {
	newStack := newTypeRelStack(value)
	newStack.next = stack
	return newStack
}

func (stack *typeRelStack) pop() (typeRelEntry, *typeRelStack) {
	return stack.value, stack.next
}

func (stack *typeRelStack) peek() typeRelEntry {
	return stack.value
}
