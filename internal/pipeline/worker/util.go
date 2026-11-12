package worker

import (
	"context"
)

// EdgeLabels returns the source and destination labels for edge,
// or ("nil", "nil") if edge is nil.
func EdgeLabels(edge *Edge) (string, string) {
	src := "nil"
	dst := "nil"
	if edge != nil {
		src = edge.GetTo().GetUniqueLabel()
		dst = edge.GetFrom().GetUniqueLabel()
	}
	return src, dst
}

// DrainSender consumes and discards all remaining messages from sender,
// invoking Done on each to release pooled resources.
func DrainSender(ctx context.Context, sender Sender) {
	for {
		msg, ok := sender.Recv(ctx)
		if !ok {
			break
		}
		msg.Done()
	}
}

// IsCyclical reports whether edge represents a cyclical relationship
// in the authorization model graph.
func IsCyclical(edge *Edge) bool {
	if edge == nil {
		return false
	}
	return len(edge.GetRecursiveRelation()) > 0 || edge.IsPartOfTupleCycle()
}
