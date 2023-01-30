//go:generate mockgen -source storage.go -destination ./mocks/mock_storage.go -package mocks OpenFGADatastore
package storage

import (
	"context"
	"time"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const (
	DefaultMaxTuplesPerWrite             = 100
	DefaultMaxTypesPerAuthorizationModel = 100
	DefaultPageSize                      = 50
)

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

// Writes and Deletes are typesafe aliases for Write arguments.
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
