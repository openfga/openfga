package storage

//go:generate mockgen -source tuple_mappers.go -destination ../../internal/mocks/mock_tuple_mappers.go -package mocks

import (
	"context"
	"fmt"
	"sync"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

type TupleMapperFunc func(t *openfgav1.Tuple) string

func UserMapper() TupleMapperFunc {
	return func(t *openfgav1.Tuple) string {
		return t.GetKey().GetUser()
	}
}

func ObjectMapper() TupleMapperFunc {
	return func(t *openfgav1.Tuple) string {
		return t.GetKey().GetObject()
	}
}

type TupleMapperKind int64

const (
	// UsersetKind is a mapper that returns the userset ID from the tuple's user field.
	UsersetKind TupleMapperKind = iota
	// TTUKind is a mapper that returns the user field of the tuple.
	TTUKind
	// ObjectIDKind is mapper that returns the object field of the tuple.
	ObjectIDKind
)

// TupleMapper is an iterator that, on calls to Next and Head, returns a mapping of the tuple.
type TupleMapper interface {
	Iterator[string]
}

// IObjectMapper is an iterator that, on calls to Next and Head, returns an object (type + id).
type IObjectMapper interface {
	TupleMapper
}

var _ IObjectMapper = (*StaticObjectMapper)(nil)

type StaticObjectMapper struct {
	items []string // GUARDED_BY(mu)
	mu    *sync.Mutex
}

// NewStaticObjectMapper is an iterator over objects (type + id).
func NewStaticObjectMapper(items []string) *StaticObjectMapper {
	return &StaticObjectMapper{items: items, mu: &sync.Mutex{}}
}

func (s *StaticObjectMapper) Next(ctx context.Context) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		return "", ErrIteratorDone
	}

	next, rest := s.items[0], s.items[1:]
	s.items = rest

	return next, nil
}

func (s *StaticObjectMapper) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = nil
}

func (s *StaticObjectMapper) Head(ctx context.Context) (string, error) {
	if ctx.Err() != nil {
		return "", ctx.Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		return "", ErrIteratorDone
	}

	return s.items[0], nil
}

type UsersetMapper struct {
	iter TupleKeyIterator
	once *sync.Once
}

var _ TupleMapper = (*UsersetMapper)(nil)

func (n UsersetMapper) Next(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Next(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n UsersetMapper) Stop() {
	n.once.Do(func() {
		n.iter.Stop()
	})
}

func (n UsersetMapper) Head(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Head(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n UsersetMapper) doMap(t *openfgav1.TupleKey) (string, error) {
	usersetName, relation := tuple.SplitObjectRelation(t.GetUser())
	if relation == "" {
		// This should never happen because ReadUsersetTuples only returns usersets as users.
		return "", fmt.Errorf("unexpected userset %s with no relation", t.GetUser())
	}
	return usersetName, nil
}

type TTUMapper struct {
	iter TupleKeyIterator
	once *sync.Once
}

var _ TupleMapper = (*TTUMapper)(nil)

func (n TTUMapper) Next(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Next(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n TTUMapper) Stop() {
	n.once.Do(func() {
		n.iter.Stop()
	})
}

func (n TTUMapper) Head(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Head(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n TTUMapper) doMap(t *openfgav1.TupleKey) (string, error) {
	return t.GetUser(), nil
}

type ObjectIDMapper struct {
	iter TupleKeyIterator
	once *sync.Once
}

var _ TupleMapper = (*ObjectIDMapper)(nil)
var _ IObjectMapper = (*ObjectIDMapper)(nil)

func NewObjectMapper(iter TupleKeyIterator) *ObjectIDMapper {
	return &ObjectIDMapper{iter: iter, once: &sync.Once{}}
}

func (n ObjectIDMapper) Next(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Next(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n ObjectIDMapper) Stop() {
	n.once.Do(func() {
		n.iter.Stop()
	})
}

func (n ObjectIDMapper) Head(ctx context.Context) (string, error) {
	tupleRes, err := n.iter.Head(ctx)
	if err != nil {
		return "", err
	}
	return n.doMap(tupleRes)
}

func (n ObjectIDMapper) doMap(t *openfgav1.TupleKey) (string, error) {
	return t.GetObject(), nil
}

func WrapIterator(kind TupleMapperKind, iter TupleKeyIterator) TupleMapper {
	switch kind {
	case UsersetKind:
		return &UsersetMapper{iter: iter, once: &sync.Once{}}
	case TTUKind:
		return &TTUMapper{iter: iter, once: &sync.Once{}}
	case ObjectIDKind:
		return NewObjectMapper(iter)
	}
	return nil
}
