package storage

import "github.com/emirpasic/gods/trees/redblacktree"

// SortedSet stores a set (no duplicates allowed) of string IDs in memory
// in a way that also provides fast sorted access.
type SortedSet interface {
	Size() int
	Min() string
	Max() string
	Add(key string)
	Exists(key string) bool
	Values() []string
}

type RedBlackTreeSet struct {
	inner *redblacktree.Tree
}

var _ SortedSet = (*RedBlackTreeSet)(nil)

func NewSortedSet() *RedBlackTreeSet {
	c := &RedBlackTreeSet{
		inner: redblacktree.NewWithStringComparator(),
	}
	return c
}

func (r *RedBlackTreeSet) Min() string {
	return r.inner.Left().Value.(string)
}

func (r *RedBlackTreeSet) Max() string {
	return r.inner.Right().Value.(string)
}

func (r *RedBlackTreeSet) Add(key string) {
	r.inner.Put(key, nil)
}

func (r *RedBlackTreeSet) Exists(key string) bool {
	_, ok := r.inner.Get(key)
	return ok
}

func (r *RedBlackTreeSet) Size() int {
	return r.inner.Size()
}

func (r *RedBlackTreeSet) Values() []string {
	values := make([]string, 0, r.inner.Size())
	for _, v := range r.inner.Keys() {
		values = append(values, v.(string))
	}
	return values
}
