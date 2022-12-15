//go:generate mockgen -source storage.go -destination ./mocks/mock_storage.go -package mocks OpenFGADatastore
package storage

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/openfga/openfga/pkg/tuple"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const DefaultPageSize = 50

var ErrIteratorDone = errors.New("iterator done")

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
	// Next will return the next available item. If the context is cancelled or times out, it should return ErrIteratorDone
	Next(ctx context.Context) (T, error)
	// Stop terminates iteration over the underlying iterator.
	Stop()
}

// ObjectIterator is an iterator for Objects (type + id). It is closed by explicitly calling Stop() or by calling Next() until it
// returns an ErrIteratorDone error.
type ObjectIterator = Iterator[*openfgapb.Object]

// TupleIterator is an iterator for Tuples. It is closed by explicitly calling Stop() or by calling Next() until it
// returns an ErrIteratorDone error.
type TupleIterator = Iterator[*openfgapb.Tuple]

// TupleKeyIterator is an iterator for TupleKeys. It is closed by explicitly calling Stop() or by calling Next() until it
// returns an ErrIteratorDone error.
type TupleKeyIterator = Iterator[*openfgapb.TupleKey]

type uniqueObjectIterator struct {
	iter1, iter2 ObjectIterator
	objects      sync.Map
}

// NewUniqueObjectIterator returns an ObjectIterator that iterates over two ObjectIterators and yields only distinct
// objects with the duplicates removed.
//
// iter1 should generally be provided by a constrained iterator (e.g. contextual tuples) and iter2 should be provided
// by a storage iterator that already guarantees uniqueness.
func NewUniqueObjectIterator(iter1, iter2 ObjectIterator) ObjectIterator {
	return &uniqueObjectIterator{
		iter1: iter1,
		iter2: iter2,
	}
}

var _ ObjectIterator = (*uniqueObjectIterator)(nil)

// Next returns the next most unique object from the two underlying iterators.
// If the context is cancelled or times out, it should return ErrIteratorDone
func (u *uniqueObjectIterator) Next(ctx context.Context) (*openfgapb.Object, error) {
	for {
		obj, err := u.iter1.Next(ctx)
		if err != nil {
			if err == ErrIteratorDone {
				break
			}

			return nil, err
		}

		// if the object has not already been seen, then store it and return it
		_, ok := u.objects.Load(tuple.ObjectKey(obj))
		if !ok {
			u.objects.Store(tuple.ObjectKey(obj), struct{}{})
			return obj, nil
		}

	}

	// assumption is that iter2 yields unique values to begin with
	for {
		obj, err := u.iter2.Next(ctx)
		if err != nil {
			if err == ErrIteratorDone {
				return nil, ErrIteratorDone
			}

			return nil, err
		}

		_, ok := u.objects.Load(tuple.ObjectKey(obj))
		if !ok {
			return obj, nil
		}
	}
}

func (u *uniqueObjectIterator) Stop() {
	u.iter1.Stop()
	u.iter2.Stop()
}

type combinedIterator[T any] struct {
	iter1, iter2 Iterator[T]
}

func (c *combinedIterator[T]) Next(ctx context.Context) (T, error) {
	val, err := c.iter1.Next(ctx)
	if err != nil {
		if !errors.Is(err, ErrIteratorDone) {
			return val, err
		}
	} else {
		return val, nil
	}

	val, err = c.iter2.Next(ctx)
	if err != nil {
		if !errors.Is(err, ErrIteratorDone) {
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

// NewStaticTupleIterator returns a TupleIterator that iterates over the provided slice.
func NewStaticTupleIterator(tuples []*openfgapb.Tuple) TupleIterator {
	iter := &staticIterator[*openfgapb.Tuple]{
		items: tuples,
	}

	return iter
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

func (t *tupleKeyIterator) Next(ctx context.Context) (*openfgapb.TupleKey, error) {
	tuple, err := t.iter.Next(ctx)
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

type tupleKeyObjectIterator struct {
	iter TupleKeyIterator
}

var _ ObjectIterator = (*tupleKeyObjectIterator)(nil)

func (t *tupleKeyObjectIterator) Next(ctx context.Context) (*openfgapb.Object, error) {
	tk, err := t.iter.Next(ctx)
	if err != nil {
		return nil, err
	}
	objectType, objectID := tuple.SplitObject(tk.GetObject())
	return &openfgapb.Object{Type: objectType, Id: objectID}, nil
}

func (t *tupleKeyObjectIterator) Stop() {
	t.iter.Stop()
}

// NewObjectIteratorFromTupleKeyIterator takes a TupleKeyIterator and yields all the objects from it as a ObjectIterator.
func NewObjectIteratorFromTupleKeyIterator(iter TupleKeyIterator) ObjectIterator {
	return &tupleKeyObjectIterator{iter}
}

type staticIterator[T any] struct {
	items []T
}

func (s *staticIterator[T]) Next(ctx context.Context) (T, error) {
	var val T
	select {
	case <-ctx.Done():
		return val, ErrIteratorDone
	default:
		if len(s.items) == 0 {
			return val, ErrIteratorDone
		}

		next, rest := s.items[0], s.items[1:]
		s.items = rest

		return next, nil
	}
}

func (s *staticIterator[T]) Stop() {}

// NewStaticObjectIterator returns an ObjectIterator that iterates over the provided slice of objects.
func NewStaticObjectIterator(objects []*openfgapb.Object) ObjectIterator {

	return &staticIterator[*openfgapb.Object]{
		items: objects,
	}
}

// TupleKeyFilterFunc is a filter function that is used to filter out tuples from a TupleKey iterator
// that don't meet some criteria. Implementations should return true if the tuple should be returned
// and false if it should be filtered out.
type TupleKeyFilterFunc func(tupleKey *openfgapb.TupleKey) bool

type filteredTupleKeyIterator struct {
	iter   TupleKeyIterator
	filter TupleKeyFilterFunc
}

var _ TupleKeyIterator = &filteredTupleKeyIterator{}

// Next returns the next most tuple in the underlying iterator that meets
// the filter function this iterator was constructed with.
func (f *filteredTupleKeyIterator) Next(ctx context.Context) (*openfgapb.TupleKey, error) {

	for {
		tuple, err := f.iter.Next(ctx)
		if err != nil {
			return nil, err
		}

		if f.filter(tuple) {
			return tuple, nil
		}
	}
}

func (f *filteredTupleKeyIterator) Stop() {
	f.iter.Stop()
}

// NewFilteredTupleKeyIterator returns an iterator that filters out all tuples that don't
// meet the conditions of the provided TupleFilterFunc.
func NewFilteredTupleKeyIterator(iter TupleKeyIterator, filter TupleKeyFilterFunc) TupleKeyIterator {

	return &filteredTupleKeyIterator{
		iter,
		filter,
	}
}

// Typesafe aliases for Write arguments.

type Writes = []*openfgapb.TupleKey
type Deletes = []*openfgapb.TupleKey

// A TupleBackend provides an R/W interface for managing tuples.
type TupleBackend interface {
	// Read the set of tuples associated with `store` and `TupleKey`, which may be nil or partially filled. If nil,
	// Read will return an iterator over all the `Tuple`s in the given store. If the `TupleKey` is partially filled,
	// it will return an iterator over those `Tuple`s which match the `TupleKey`. Note that at least one of `Object`
	// or `User` (or both), must be specified in this case.
	//
	// The caller must be careful to close the TupleIterator, either by consuming the entire iterator or by closing it.
	Read(context.Context, string, *openfgapb.TupleKey) (TupleIterator, error)

	// ListObjectsByType returns all the objects of a specific type.
	// You can assume that the type has already been validated.
	// The result can't have duplicate elements.
	ListObjectsByType(
		ctx context.Context,
		store string,
		objectType string,
	) (ObjectIterator, error)

	// ReadPage is similar to Read, but with PaginationOptions. Instead of returning a TupleIterator, ReadPage
	// returns a page of tuples and a possibly non-empty continuation token.
	ReadPage(
		ctx context.Context,
		store string,
		tk *openfgapb.TupleKey,
		opts PaginationOptions,
	) ([]*openfgapb.Tuple, []byte, error)

	// Write updates data in the tuple backend, performing all delete operations in
	// `deletes` before adding new values in `writes`, returning the time of the transaction, or an error.
	// It is expected that
	// - there is at most 10 deletes/writes
	// - no duplicate item in delete/write list
	Write(ctx context.Context, store string, d Deletes, w Writes) error

	ReadUserTuple(
		ctx context.Context,
		store string,
		tk *openfgapb.TupleKey,
	) (*openfgapb.Tuple, error)

	ReadUsersetTuples(
		ctx context.Context,
		store string,
		tk *openfgapb.TupleKey,
	) (TupleIterator, error)

	// ReadStartingWithUser performs a reverse read of relationship tuples starting at one or
	// more user(s) or userset(s) and filtered by object type and relation.
	//
	// For example, given the following relationship tuples:
	//   document:doc1, viewer, user:jon
	//   document:doc2, viewer, group:eng#member
	//   document:doc3, editor, user:jon
	//
	// ReverseReadTuples for ['user:jon', 'group:eng#member'] filtered by 'document#viewer' would
	// return ['document:doc1#viewer@user:jon', 'document:doc2#viewer@group:eng#member'].
	ReadStartingWithUser(
		ctx context.Context,
		store string,
		filter ReadStartingWithUserFilter,
	) (TupleIterator, error)

	// MaxTuplesPerWrite returns the maximum number of items allowed in a single write transaction
	MaxTuplesPerWrite() int
}

// ReadStartingWithUserFilter specifies the filter options that will be used to constrain the ReadStartingWithUser
// query.
type ReadStartingWithUserFilter struct {
	ObjectType string
	Relation   string
	UserFilter []*openfgapb.ObjectRelation
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
	// MaxTypesPerAuthorizationModel returns the maximum number of items allowed for type definitions
	MaxTypesPerAuthorizationModel() int

	// WriteAuthorizationModel writes an authorization model for the given store.
	// It is expected that the number of type definitions is less than or equal to 24
	WriteAuthorizationModel(ctx context.Context, store string, model *openfgapb.AuthorizationModel) error
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
	Close()
}
