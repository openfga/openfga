package storage

import (
	"context"
	"time"

	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

const DefaultPageSize = 50

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

// TupleIterator is an iterator for Tuples. It is closed by explicitly calling Stop() or by calling Next() until it
// returns an iterator.Done error.
type TupleIterator interface {
	Next() (*openfga.Tuple, error)
	// Stop will release any resources held by the iterator. It must be safe to be called multiple times.
	Stop()
}

// Typesafe aliases for Write arguments.

type Writes = []*openfga.TupleKey
type Deletes = []*openfga.TupleKey

// A TupleBackend provides an R/W interface for managing tuples.
type TupleBackend interface {
	// Read the set of tuples associated with `store` and `key`, which may be partially filled. A key must specify at
	// least one of `Object` or `User` (or both), and may also optionally constrain by relation. The caller must be
	// careful to close the TupleIterator, either by consuming the entire iterator or by closing it.
	Read(context.Context, string, *openfga.TupleKey) (TupleIterator, error)

	// ReadPage is similar to Read, but with PaginationOptions. Instead of returning a TupleIterator, ReadPage
	// returns a page of tuples and a possibly non-empty continuation token.
	ReadPage(context.Context, string, *openfga.TupleKey, PaginationOptions) ([]*openfga.Tuple, []byte, error)

	// Write updates data in the tuple backend, performing all delete operations in
	// `deletes` before adding new values in `writes`, returning the time of the transaction, or an error.
	// It is expected that
	// - there is at most 10 deletes/writes
	// - no duplicate item in delete/write list
	Write(context.Context, string, Deletes, Writes) error

	ReadUserTuple(context.Context, string, *openfga.TupleKey) (*openfga.Tuple, error)

	ReadUsersetTuples(context.Context, string, *openfga.TupleKey) (TupleIterator, error)

	// ReadByStore reads the tuples associated with `store`.
	ReadByStore(context.Context, string, PaginationOptions) ([]*openfga.Tuple, []byte, error)

	// MaxTuplesInWriteOperation returns the maximum number of items allowed in a single write transaction
	MaxTuplesInWriteOperation() int
}

// AuthorizationModelReadBackend Provides a Read interface for managing type definitions.
type AuthorizationModelReadBackend interface {
	// ReadAuthorizationModel Read the store type definition corresponding to `id`.
	ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgav1pb.AuthorizationModel, error)

	// ReadAuthorizationModels Read all type definitions ids for the supplied store.
	ReadAuthorizationModels(ctx context.Context, store string, options PaginationOptions) ([]string, []byte, error)

	FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error)
}

// TypeDefinitionReadBackend Provides a Read interface for managing type definitions.
type TypeDefinitionReadBackend interface {
	// ReadTypeDefinition Read the store authorization model corresponding to `id` + `objectType`.
	ReadTypeDefinition(ctx context.Context, store, id string, objectType string) (*openfgav1pb.TypeDefinition, error)
}

// TypeDefinitionWriteBackend Provides a write interface for managing typed definition.
type TypeDefinitionWriteBackend interface {
	// MaxTypesInTypeDefinition returns the maximum number of items allowed for type definitions
	MaxTypesInTypeDefinition() int

	// WriteAuthorizationModel writes an authorization model for the given store.
	// It is expected that the number of type definitions is less than or equal to 24
	WriteAuthorizationModel(ctx context.Context, store, id string, tds *openfgav1pb.TypeDefinitions) error
}

// AuthorizationModelBackend provides an R/W interface for managing type definition.
type AuthorizationModelBackend interface {
	AuthorizationModelReadBackend
	TypeDefinitionReadBackend
	TypeDefinitionWriteBackend
}

type StoresBackend interface {
	CreateStore(ctx context.Context, store *openfga.Store) (*openfga.Store, error)

	DeleteStore(ctx context.Context, id string) error

	GetStore(ctx context.Context, id string) (*openfga.Store, error)

	ListStores(ctx context.Context, paginationOptions PaginationOptions) ([]*openfga.Store, []byte, error)
}

type AssertionsBackend interface {
	WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfga.Assertion) error
	ReadAssertions(ctx context.Context, store, authzModelId string) ([]*openfga.Assertion, error)
}

type ChangelogBackend interface {

	// ReadChanges returns the writes and deletes that have occurred for tuples of a given object type within a store.
	// The horizonOffset should be specified using a unit no more granular than a millisecond and should be interpreted
	// as a millisecond duration.
	ReadChanges(ctx context.Context, storeId, objectType string, paginationOptions PaginationOptions, horizonOffset time.Duration) ([]*openfga.TupleChange, []byte, error)
}

type AllBackends struct {
	TupleBackend              TupleBackend
	AuthorizationModelBackend AuthorizationModelBackend
	StoresBackend             StoresBackend
	AssertionsBackend         AssertionsBackend
	ChangelogBackend          ChangelogBackend
}
