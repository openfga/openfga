// Package storage contains storage interfaces and implementations
//
//go:generate mockgen -source storage.go -destination ../../internal/mocks/mock_storage.go -package mocks OpenFGADatastore
package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/pkg/errors"

	"github.com/openfga/openfga/pkg/typesystem"
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
type Writes = []*openfgav1.TupleKey
type Deletes = []*openfgav1.TupleKeyWithoutCondition

// A TupleBackend provides an R/W interface for managing tuples.
type TupleBackend interface {
	RelationshipTupleReader
	RelationshipTupleWriter
}

type RelationshipTupleReader interface {
	// Read the set of tuples associated with `store` and `TupleKey`, which may be nil or partially filled. If nil,
	// Read will return an iterator over all the `Tuple`s in the given store. If the `TupleKey` is partially filled,
	// it will return an iterator over those `Tuple`s which match the `TupleKey`. Note that at least one of `Object`
	// or `User` (or both), must be specified in this case.
	//
	// The caller must be careful to close the TupleIterator, either by consuming the entire iterator or by closing it.
	// There is NO guarantee on the order returned on the iterator.
	Read(context.Context, string, *openfgav1.TupleKey) (TupleIterator, error)

	// ReadPage is similar to Read, but with PaginationOptions. Instead of returning a TupleIterator, ReadPage
	// returns a page of tuples and a possibly non-empty continuation token.
	// The tuples returned are ordered by ULID.
	ReadPage(
		ctx context.Context,
		store string,
		tk *openfgav1.TupleKey,
		opts PaginationOptions,
	) ([]*openfgav1.Tuple, []byte, error)

	// ReadUserTuple tries to return one tuple that matches the provided key exactly.
	// If none is found, it must return ErrNotFound.
	ReadUserTuple(
		ctx context.Context,
		store string,
		tk *openfgav1.TupleKey,
	) (*openfgav1.Tuple, error)

	// ReadUsersetTuples returns all userset tuples for a specified object and relation.
	// For example, given the following relationship tuples:
	//	document:doc1, viewer, user:*
	//	document:doc1, viewer, group:eng#member
	// and the filter
	//	object=document:1, relation=viewer, allowedTypesForUser=[group#member]
	// this method would return the tuple (document:doc1, viewer, group:eng#member)
	// If allowedTypesForUser is empty, both tuples would be returned.
	// There is NO guarantee on the order returned on the iterator.
	ReadUsersetTuples(
		ctx context.Context,
		store string,
		filter ReadUsersetTuplesFilter,
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
	// There is NO guarantee on the order returned on the iterator.
	ReadStartingWithUser(
		ctx context.Context,
		store string,
		filter ReadStartingWithUserFilter,
	) (TupleIterator, error)
}

type RelationshipTupleWriter interface {

	// Write updates data in the tuple backend, performing all delete operations in
	// `deletes` before adding new values in `writes`, returning the time of the transaction, or an error.
	// If there are more than MaxTuplesPerWrite, it must return ErrExceededWriteBatchLimit.
	// If two requests attempt to write the same tuple at the same time, it must return ErrTransactionalWriteFailed.
	// If the tuple to be written already existed or the tuple to be deleted didn't exist, it must return ErrInvalidWriteInput.
	Write(ctx context.Context, store string, d Deletes, w Writes) error

	// MaxTuplesPerWrite returns the maximum number of items (writes and deletes combined) allowed in a single write transaction
	MaxTuplesPerWrite() int
}

// ReadStartingWithUserFilter specifies the filter options that will be used to constrain the ReadStartingWithUser
// query.
type ReadStartingWithUserFilter struct {
	ObjectType string
	Relation   string
	UserFilter []*openfgav1.ObjectRelation
}

type ReadUsersetTuplesFilter struct {
	Object                      string                         // required
	Relation                    string                         // required
	AllowedUserTypeRestrictions []*openfgav1.RelationReference // optional
}

// ResolveAuthorizationModel is a helper function that validates a model ID, and tries to
// fetch a specific store + model ID. If model ID is empty, it tries to fetch the latest model from that store.
// TODO this could be a reusable command with unit tests that can return server errors directly
var ResolveAuthorizationModel = func(ctx context.Context, ds AuthorizationModelReadBackend, storeID, modelID string) (*typesystem.TypeSystem, error) {
	if modelID != "" {
		if _, err := ulid.Parse(modelID); err != nil {
			return nil, ErrNotFound
		}
	}
	latestModelID := modelID
	var err error
	if modelID == "" {
		latestModelID, err = ds.FindLatestAuthorizationModelID(ctx, storeID)
		if err != nil {
			if errors.Is(err, ErrNotFound) {
				return nil, ErrLatestAuthorizationModelNotFound
			}
			return nil, fmt.Errorf("failed to FindLatestAuthorizationModelID: %w", err)
		}
	}

	return ds.ReadAuthorizationModel(ctx, storeID, latestModelID)
}

// AuthorizationModelReadBackend Provides a Read interface for managing type definitions.
type AuthorizationModelReadBackend interface {
	// ReadAuthorizationModel Read the model corresponding to store and model id
	// If it's not found, it must return ErrNotFound
	ReadAuthorizationModel(ctx context.Context, store string, id string) (*typesystem.TypeSystem, error)

	// ReadAuthorizationModels Read all type definitions ids for the supplied store.
	ReadAuthorizationModels(ctx context.Context, store string, options PaginationOptions) ([]*openfgav1.AuthorizationModel, []byte, error)

	// FindLatestAuthorizationModelID Returns the last model `id` written for a store.
	// If none were ever written, it must return ErrNotFound
	FindLatestAuthorizationModelID(ctx context.Context, store string) (string, error)
}

// TypeDefinitionWriteBackend Provides a write interface for managing typed definition.
type TypeDefinitionWriteBackend interface {
	// MaxTypesPerAuthorizationModel returns the maximum number of type definition rows/items per model.
	MaxTypesPerAuthorizationModel() int

	// WriteAuthorizationModel writes an authorization model for the given store.
	WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error
}

// AuthorizationModelBackend provides an R/W interface for managing models and their type definitions
type AuthorizationModelBackend interface {
	AuthorizationModelReadBackend
	TypeDefinitionWriteBackend
}

type StoresBackend interface {
	CreateStore(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error)
	DeleteStore(ctx context.Context, id string) error
	GetStore(ctx context.Context, id string) (*openfgav1.Store, error)
	ListStores(ctx context.Context, paginationOptions PaginationOptions) ([]*openfgav1.Store, []byte, error)
}

type AssertionsBackend interface {
	// WriteAssertions overwrites the assertions for a store and modelID.
	WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error

	// ReadAssertions returns the assertions for a store and modelId.
	// If no assertions were ever written, it must return an empty list.
	ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error)
}

type ChangelogBackend interface {

	// ReadChanges returns the writes and deletes that have occurred for tuples of a given object type within a store.
	// The horizonOffset should be specified using a unit no more granular than a millisecond and should be interpreted
	// as a millisecond duration.
	ReadChanges(ctx context.Context, store, objectType string, paginationOptions PaginationOptions, horizonOffset time.Duration) ([]*openfgav1.TupleChange, []byte, error)
}

type Reader interface {
	RelationshipTupleReader
	AuthorizationModelReadBackend
}

type OpenFGADatastore interface {
	TupleBackend
	AuthorizationModelBackend
	StoresBackend
	AssertionsBackend
	ChangelogBackend

	// IsReady reports whether the datastore is ready to accept traffic.
	IsReady(ctx context.Context) (ReadinessStatus, error)

	// Close closes the datastore and cleans up any residual resources.
	Close()
}

// ReadinessStatus represents the readiness status of the datastore.
type ReadinessStatus struct {
	// Message is a human-friendly status message for the current datastore status.
	Message string

	IsReady bool
}
