package check

import (
	"time"

	"github.com/openfga/language/pkg/go/graph"
)

type Response struct {
	Allowed bool
}

func (r *Response) GetAllowed() bool {
	if r == nil {
		return false
	}
	return r.Allowed
}

type ResponseMsg struct {
	Res   *Response
	Edges []*graph.WeightedAuthorizationModelEdge
	Err   error
}

type ResponseCacheEntry struct {
	LastModified time.Time
	Res          *Response
}

func (e *ResponseCacheEntry) CacheEntityType() string {
	return "check_response"
}
