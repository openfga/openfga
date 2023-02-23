//go:generate mockgen -source storage.go -destination ./mocks/mock_storage.go -package mocks OpenFGADatastore
package storage

import (
	"context"
	"time"

	"github.com/openfga/openfga/pkg/tuple"
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
	RelationshipTupleReader
	RelationshipTupleWriter

	// ListObjectsByType returns all the objects of a specific type.
	// You can assume that the type has already been validated.
	// The result can't have duplicate elements.
	ListObjectsByType(
		ctx context.Context,
		store string,
		objectType string,
	) (ObjectIterator, error)
}

type RelationshipTupleReader interface {
	// Read the set of tuples associated with `store` and `TupleKey`, which may be nil or partially filled. If nil,
	// Read will return an iterator over all the `Tuple`s in the given store. If the `TupleKey` is partially filled,
	// it will return an iterator over those `Tuple`s which match the `TupleKey`. Note that at least one of `Object`
	// or `User` (or both), must be specified in this case.
	//
	// The caller must be careful to close the TupleIterator, either by consuming the entire iterator or by closing it.
	Read(context.Context, string, *openfgapb.TupleKey) (TupleIterator, error)

	// ReadPage is similar to Read, but with PaginationOptions. Instead of returning a TupleIterator, ReadPage
	// returns a page of tuples and a possibly non-empty continuation token.
	ReadPage(
		ctx context.Context,
		store string,
		tk *openfgapb.TupleKey,
		opts PaginationOptions,
	) ([]*openfgapb.Tuple, []byte, error)

	ReadUserTuple(
		ctx context.Context,
		store string,
		tk *openfgapb.TupleKey,
	) (*openfgapb.Tuple, error)

	// ReadUsersetTuples returns all userset tuples for a specified object and relation.
	// For example, given the following relationship tuples:
	//	document:doc1, viewer, user:*
	//	document:doc1, viewer, group:eng#member
	// and the filter
	//	object=document:1, relation=viewer, allowedTypesForUser=[group#member]
	// this method would return the tuple (document:doc1, viewer, group:eng#member)
	// If allowedTypesForUser is empty, both tuples would be returned.
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
	ReadStartingWithUser(
		ctx context.Context,
		store string,
		filter ReadStartingWithUserFilter,
	) (TupleIterator, error)
}

type RelationshipTupleWriter interface {

	// Write updates data in the tuple backend, performing all delete operations in
	// `deletes` before adding new values in `writes`, returning the time of the transaction, or an error.
	// It is expected that
	// - there is at most 10 deletes/writes
	// - no duplicate item in delete/write list
	Write(ctx context.Context, store string, d Deletes, w Writes) error

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

type ReadUsersetTuplesFilter struct {
	Object                      string                         // required
	Relation                    string                         // required
	AllowedUserTypeRestrictions []*openfgapb.RelationReference // optional
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

type ctxKey string

const (
	contextualTuplesCtxKey ctxKey = "contextual-tuples"
)

func ContextualTuplesFromContext(ctx context.Context) ([]*openfgapb.TupleKey, bool) {
	ctxTuples, ok := ctx.Value(contextualTuplesCtxKey).([]*openfgapb.TupleKey)
	return ctxTuples, ok
}

func ContextWithContextualTuples(parent context.Context, ctxTuples []*openfgapb.TupleKey) context.Context {
	return context.WithValue(parent, contextualTuplesCtxKey, ctxTuples)
}

func NewContextualTupleDatastore(ds RelationshipTupleReader) RelationshipTupleReader {
	return &ctxRelationshipTupleReader{wrapped: ds}
}

type ctxRelationshipTupleReader struct {
	wrapped RelationshipTupleReader
}

var _ RelationshipTupleReader = (*ctxRelationshipTupleReader)(nil)

// filterTuples filters out the tuples in the provided slice by removing any tuples in the slice
// that don't match the object and relation provided in the filterKey.
func filterTuples(tuples []*openfgapb.TupleKey, targetObjectID, targetRelation string) []*openfgapb.Tuple {
	var filtered []*openfgapb.Tuple
	for _, tk := range tuples {
		if tk.GetObject() == targetObjectID && tk.GetRelation() == targetRelation {
			filtered = append(filtered, &openfgapb.Tuple{
				Key: tk,
			})
		}
	}

	return filtered
}

func (b *ctxRelationshipTupleReader) Read(
	ctx context.Context,
	storeID string,
	tk *openfgapb.TupleKey,
) (TupleIterator, error) {

	var iter1 TupleIterator = &emptyTupleIterator{}

	tuples, ok := ContextualTuplesFromContext(ctx)
	if ok {
		iter1 = NewStaticTupleIterator(filterTuples(tuples, tk.Object, tk.Relation))
	}

	iter2, err := b.wrapped.Read(ctx, storeID, tk)
	if err != nil {
		return nil, err
	}

	return NewCombinedIterator(iter1, iter2), nil
}

func (b *ctxRelationshipTupleReader) ReadPage(
	ctx context.Context,
	store string,
	tk *openfgapb.TupleKey,
	opts PaginationOptions,
) ([]*openfgapb.Tuple, []byte, error) {
	return b.wrapped.ReadPage(ctx, store, tk, opts)
}

func (b *ctxRelationshipTupleReader) ReadUserTuple(
	ctx context.Context,
	store string,
	tk *openfgapb.TupleKey,
) (*openfgapb.Tuple, error) {

	var filteredTuples []*openfgapb.Tuple

	tuples, ok := ContextualTuplesFromContext(ctx)
	if ok {
		filteredTuples = filterTuples(tuples, tk.Object, tk.Relation)
	}

	for _, tuple := range filteredTuples {
		if tuple.GetKey().GetUser() == tk.GetUser() {
			return tuple, nil
		}
	}

	return b.wrapped.ReadUserTuple(ctx, store, tk)
}

func (b *ctxRelationshipTupleReader) ReadUsersetTuples(
	ctx context.Context,
	store string,
	filter ReadUsersetTuplesFilter,
) (TupleIterator, error) {

	var iter1 TupleIterator = &emptyTupleIterator{}

	var filteredTuples []*openfgapb.Tuple

	tuples, ok := ContextualTuplesFromContext(ctx)
	if ok {
		for _, t := range filterTuples(tuples, filter.Object, filter.Relation) {
			if tuple.GetUserTypeFromUser(t.GetKey().GetUser()) == tuple.UserSet {
				filteredTuples = append(filteredTuples, t)
			}
		}

		iter1 = NewStaticTupleIterator(filteredTuples)
	}

	iter2, err := b.wrapped.ReadUsersetTuples(ctx, store, filter)
	if err != nil {
		return nil, err
	}

	return NewCombinedIterator(iter1, iter2), nil
}

func (b *ctxRelationshipTupleReader) ReadStartingWithUser(
	ctx context.Context,
	store string,
	filter ReadStartingWithUserFilter,
) (TupleIterator, error) {

	var iter1 TupleIterator = &emptyTupleIterator{}

	tuples, ok := ContextualTuplesFromContext(ctx)
	if ok {

		var filteredTuples []*openfgapb.Tuple
		for _, t := range tuples {
			if tuple.GetType(t.GetObject()) != filter.ObjectType {
				continue
			}

			if t.GetRelation() != filter.Relation {
				continue
			}

			for _, u := range filter.UserFilter {
				targetUser := u.GetObject()
				if u.GetRelation() != "" {
					targetUser = tuple.ToObjectRelationString(targetUser, u.GetRelation())
				}

				if t.GetUser() == targetUser {
					filteredTuples = append(filteredTuples, &openfgapb.Tuple{
						Key: t,
					})
				}
			}
		}

		iter1 = NewStaticTupleIterator(filteredTuples)
	}

	iter2, err := b.wrapped.ReadStartingWithUser(ctx, store, filter)
	if err != nil {
		return nil, err
	}

	return NewCombinedIterator(iter1, iter2), nil
}
