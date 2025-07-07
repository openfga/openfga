package reverseexpand

// typeRelStack is a stack implementation on top of a linked list, specific to reverse_expand.
//
// *Important*: Each push() or pop() operation creates and returns a pointer to a new stack entirely to
// ensure thread safety, since ReverseExpand kicks off many routines all relying on their own stacks.
type typeRelStack struct {
	value typeRelEntry
	next  *typeRelStack
}

func push(stack *typeRelStack, value typeRelEntry) *typeRelStack {
	return &typeRelStack{value: value, next: stack}
}

func pop(stack *typeRelStack) (typeRelEntry, *typeRelStack) {
	return stack.value, stack.next
}
