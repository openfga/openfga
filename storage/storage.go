//go:generate mockgen -source storage.go -destination ./mocks/mock_storage.go -package mocks OpenFGADatastore
package storage

import (
	"context"
	"time"

	"github.com/go-errors/errors"
	"github.com/openfga/openfga/pkg/tuple"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const DefaultPageSize = 50

var IteratorDone = errors.New("iterator done")

type PaginationOptions struct {
	PageSize int
	From     string
}

func NewPaginationOptions(ps int32, contToken string) PaginationOptions {
	pageSize := DefaultPageSize
	if ps != 0 {
		pageSize = int(ps)
	}

	return PaginationOptions{
		PageSize: pageSize,
		From:     contToken,
	}
}

type Iterator[T any] interface {
	Next() (T, error)
	Stop()
}

// ObjectIterator is an iterator for Objects (type + id). It is closed by explicitly calling Stop() or by calling Next() until it
// returns an IteratorDone error.
type ObjectIterator = Iterator[*openfgapb.Object]

// TupleIterator is an iterator for Tuples. It is closed by explicitly calling Stop() or by calling Next() until it
// returns an IteratorDone error.
type TupleIterator = Iterator[*openfgapb.Tuple]

// TupleKeyIterator is an iterator for TupleKeys. It is closed by explicitly calling Stop() or by calling Next() until it
// returns an IteratorDone error.
type TupleKeyIterator = Iterator[*openfgapb.TupleKey]

// UniqueObjectIterator takes two ObjectIterators and yields an ObjectIterator that returns only distinct objects with the
// duplicates removed.
func UniqueObjectIterator(iter1, iter2 ObjectIterator) (ObjectIterator, error) {
	objectMap := map[string]*openfgapb.Object{}

	for {
		val, err := iter1.Next()
		if err != nil {
			if errors.Is(err, IteratorDone) {
				break
			}
			return nil, err
		}

		objectMap[tuple.ObjectKey(val)] = val
	}

	for {
		val, err := iter2.Next()
		if err != nil {
			if errors.Is(err, IteratorDone) {
				break
			}
			return nil, err
		}

		objectMap[tuple.ObjectKey(val)] = val
	}

	objects := make([]*openfgapb.Object, 0, len(objectMap))
	for _, obj := range objectMap {
		objects = append(objects, obj)
	}

	return NewStaticObjectIterator(objects), nil
}

type combinedIterator[T any] struct {
	iter1, iter2 Iterator[T]
}

func (c *combinedIterator[T]) Next() (T, error) {
	val, err := c.iter1.Next()
	if err != nil {
		if !errors.Is(err, IteratorDone) {
			return val, err
		}
	} else {
		return val, nil
	}

	val, err = c.iter2.Next()
	if err != nil {
		if !errors.Is(err, IteratorDone) {
			return val, err
		}
	}

	return val, err
}

func (c *combinedIterator[T]) Stop() {
	c.iter1.Stop()
	c.iter2.Stop()
}

// NewCombinedIterator takes two generic iterators of a given type T and combines them into a single iterator that yields
// all of the values from both iterators. If the two iterators yield the same value then duplicates will be returned.
func NewCombinedIterator[T any](iter1, iter2 Iterator[T]) Iterator[T] {
	return &combinedIterator[T]{iter1, iter2}
}

// NewStaticTupleKeyIterator returns a TupleKeyIterator that iterates over the provided slice.
func NewStaticTupleKeyIterator(tupleKeys []*openfgapb.TupleKey) TupleKeyIterator {
	iter := &staticIterator[*openfgapb.TupleKey]{
		items: tupleKeys,
	}

	return iter
}

type tupleKeyIterator struct {
	iter TupleIterator
}

var _ TupleKeyIterator = (*tupleKeyIterator)(nil)

func (t *tupleKeyIterator) Next() (*openfgapb.TupleKey, error) {
	tuple, err := t.iter.Next()
	return tuple.GetKey(), err
}

func (t *tupleKeyIterator) Stop() {
	t.iter.Stop()
}

// NewTupleKeyIteratorFromTupleIterator takes a TupleIterator and yields all of the TupleKeys from it as a TupleKeyIterator.
func NewTupleKeyIteratorFromTupleIterator(iter TupleIterator) TupleKeyIterator {
	return &tupleKeyIterator{iter}
}

// NewTupleKeyObjectIterator returns an ObjectIterator that iterates over the objects
// contained in the provided list of TupleKeys.
func NewTupleKeyObjectIterator(tupleKeys []*openfgapb.TupleKey) ObjectIterator {

	objects := make([]*openfgapb.Object, 0, len(tupleKeys))
	for _, tk := range tupleKeys {
		objectType, objectID := tuple.SplitObject(tk.GetObject())
		objects = append(objects, &openfgapb.Object{Type: objectType, Id: objectID})
	}

	return NewStaticObjectIterator(objects)
}

type staticIterator[T any] struct {
	items []T
}

func (s *staticIterator[T]) Next() (T, error) {
	var val T
	if len(s.items) == 0 {
		return val, IteratorDone
	}

	next, rest := s.items[0], s.items[1:]
	s.items = rest

	return next, nil
}

func (s *staticIterator[T]) Stop() {}

type staticObjectIterator struct {
	objects []*openfgapb.Object
}

var _ ObjectIterator = (*staticObjectIterator)(nil)

// NewStaticObjectIterator returns an ObjectIterator that iterates over the provided slice of objects.
func NewStaticObjectIterator(objects []*openfgapb.Object) ObjectIterator {
	return &staticObjectIterator{objects}
}

func (s *staticObjectIterator) Next() (*openfgapb.Object, error) {

	if len(s.objects) == 0 {
		return nil, IteratorDone
	}

	next, rest := s.objects[0], s.objects[1:]
	s.objects = rest

	return next, nil
}

func (s *staticObjectIterator) Stop() {}

// Typesafe aliases for Write arguments.

type Writes = []*openfgapb.TupleKey
type Deletes = []*openfgapb.TupleKey

// A TupleBackend provides an R/W interface for managing tuples.
type TupleBackend interface {
	// Read the set of tuples associated with `store` and `key`, which may be partially filled. A key must specify at
	// least one of `Object` or `User` (or both), and may also optionally constrain by relation. The caller must be
	// careful to close the TupleIterator, either by consuming the entire iterator or by closing it.
	Read(context.Context, string, *openfgapb.TupleKey) (TupleIterator, error)

	// ListObjectsByType returns all the objects of a specific type.
	// You can assume that the type has already been validated.
	// The result can't have duplicate elements.
	ListObjectsByType(ctx context.Context, store string, objectType string) (ObjectIterator, error)

	// ReadPage is similar to Read, but with PaginationOptions. Instead of returning a TupleIterator, ReadPage
	// returns a page of tuples and a possibly non-empty continuation token.
	ReadPage(context.Context, string, *openfgapb.TupleKey, PaginationOptions) ([]*openfgapb.Tuple, []byte, error)

	// Write updates data in the tuple backend, performing all delete operations in
	// `deletes` before adding new values in `writes`, returning the time of the transaction, or an error.
	// It is expected that
	// - there is at most 10 deletes/writes
	// - no duplicate item in delete/write list
	Write(context.Context, string, Deletes, Writes) error

	ReadUserTuple(context.Context, string, *openfgapb.TupleKey) (*openfgapb.Tuple, error)

	ReadUsersetTuples(context.Context, string, *openfgapb.TupleKey) (TupleIterator, error)

	// ReadByStore reads the tuples associated with `store`.
	ReadByStore(context.Context, string, PaginationOptions) ([]*openfgapb.Tuple, []byte, error)

	// MaxTuplesInWriteOperation returns the maximum number of items allowed in a single write transaction
	MaxTuplesInWriteOperation() int
}

// AuthorizationModelReadBackend Provides a Read interface for managing type definitions.
type AuthorizationModelReadBackend interface {
	// ReadAuthorizationModel Read the store type definition corresponding to `id`.
	ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgapb.AuthorizationModel, error)

	// ReadAuthorizationModels Read all type definitions ids for the supplied store.
	ReadAuthorizationModels(ctx context.Context, store string, options PaginationOptions) ([]*openfgapb.AuthorizationModel, []byte, error)

	FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error)
}

// TypeDefinitionReadBackend Provides a Read interface for managing type definitions.
type TypeDefinitionReadBackend interface {
	// ReadTypeDefinition Read the store authorization model corresponding to `id` + `objectType`.
	ReadTypeDefinition(ctx context.Context, store, id string, objectType string) (*openfgapb.TypeDefinition, error)
}

// TypeDefinitionWriteBackend Provides a write interface for managing typed definition.
type TypeDefinitionWriteBackend interface {
	// MaxTypesInTypeDefinition returns the maximum number of items allowed for type definitions
	MaxTypesInTypeDefinition() int

	// WriteAuthorizationModel writes an authorization model for the given store.
	// It is expected that the number of type definitions is less than or equal to 24
	WriteAuthorizationModel(ctx context.Context, store, id string, tds *openfgapb.TypeDefinitions) error
}

// AuthorizationModelBackend provides an R/W interface for managing type definition.
type AuthorizationModelBackend interface {
	AuthorizationModelReadBackend
	TypeDefinitionReadBackend
	TypeDefinitionWriteBackend
}

type StoresBackend interface {
	CreateStore(ctx context.Context, store *openfgapb.Store) (*openfgapb.Store, error)

	DeleteStore(ctx context.Context, id string) error

	GetStore(ctx context.Context, id string) (*openfgapb.Store, error)

	ListStores(ctx context.Context, paginationOptions PaginationOptions) ([]*openfgapb.Store, []byte, error)
}

type AssertionsBackend interface {
	WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgapb.Assertion) error
	ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgapb.Assertion, error)
}

type ChangelogBackend interface {

	// ReadChanges returns the writes and deletes that have occurred for tuples of a given object type within a store.
	// The horizonOffset should be specified using a unit no more granular than a millisecond and should be interpreted
	// as a millisecond duration.
	ReadChanges(ctx context.Context, store, objectType string, paginationOptions PaginationOptions, horizonOffset time.Duration) ([]*openfgapb.TupleChange, []byte, error)
}

type OpenFGADatastore interface {
	TupleBackend
	AuthorizationModelBackend
	StoresBackend
	AssertionsBackend
	ChangelogBackend

	// IsReady reports whether the datastore is ready to accept traffic.
	IsReady(ctx context.Context) (bool, error)

	// Close closes the datastore and cleans up any residual resources.
	Close(ctx context.Context) error
}
