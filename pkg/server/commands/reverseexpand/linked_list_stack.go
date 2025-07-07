package reverseexpand

// linkedListStack is a stack implementation on top of a linked list, specific to reverse_expand.
// Each push() or pop() operation creates and returns a pointer to a new stack entirely to
// ensure thread safety, since ReverseExpand kicks off many routines all relying on their own stacks.
type linkedListStack struct {
	value typeRelEntry
	next  *linkedListStack
}

func newLinkedListStack(val typeRelEntry) *linkedListStack {
	return &linkedListStack{value: val}
}

func (stack *linkedListStack) push(value typeRelEntry) *linkedListStack {
	newStack := linkedListStack{value: value, next: nil}
	newStack.next = stack
	return &newStack
}

func (stack *linkedListStack) pop() (typeRelEntry, *linkedListStack) {
	return stack.value, stack.next
}

func (stack *linkedListStack) peek() typeRelEntry {
	return stack.value
}
