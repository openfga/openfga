package check

import (
	"time"
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
	ID  string
	Res *Response
	Err error
}

type ResponseCacheEntry struct {
	LastModified time.Time
	Res          *Response
}
