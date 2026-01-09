package iterator

import "github.com/openfga/openfga/pkg/storage"

type Msg struct {
	Iter storage.Iterator[string] // NOTE: This could be made completely generic if needed
	Err  error
}

type ValueMsg[T any] struct {
	Value T
	Err   error
}
