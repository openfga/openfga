package check

import (
	"time"

	"github.com/openfga/openfga/pkg/storage/cache/keys"
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
	ID  keys.Key
	Res *Response
	Err error
}

type ResponseCacheEntry struct {
	LastModified time.Time
	Res          *Response
}

func (e *ResponseCacheEntry) CacheEntityType() string {
	return "check_response"
}
