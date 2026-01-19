package check

import (
	"time"
)

type Response struct {
	Allowed    bool
	Resolution *ResolutionTree
}

func (r *Response) GetAllowed() bool {
	return r.Allowed
}

func (r *Response) GetResolution() *ResolutionTree {
	if r == nil {
		return nil
	}
	return r.Resolution
}

func (r *Response) GetResolutionNode() *ResolutionNode {
	if r == nil || r.Resolution == nil {
		return nil
	}
	return r.Resolution.Tree
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
