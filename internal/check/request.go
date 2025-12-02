package check

import (
	"errors"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

var ErrMissingStoreID = errors.New("missing store_id")
var ErrMissingAuthZModelID = errors.New("missing authorization_model_id")
var ErrInvalidUser = errors.New("the 'user' field is malformed")

type Request struct {
	StoreID              string
	AuthorizationModelID string
	TupleKey             *openfgav1.TupleKey
	ContextualTuples     []*openfgav1.TupleKey
	Context              *structpb.Struct
	Consistency          openfgav1.ConsistencyPreference

	cacheKey string
	// Invariant parts of a check request are those that don't change in sub-problems
	// AuthorizationModelID, StoreID, Context, and ContextualTuples.
	// the invariantCacheKey is computed once per request, and passed to sub-problems via copy in .clone()
	invariantCacheKey string

	objectType          string
	userType            string
	ctxTuplesByUserID   map[string][]*openfgav1.TupleKey
	ctxTuplesByObjectID map[string][]*openfgav1.TupleKey
}

type RequestParams = struct {
	StoreID          string
	Model            *modelgraph.AuthorizationModelGraph
	TupleKey         *openfgav1.TupleKey
	ContextualTuples []*openfgav1.TupleKey
	Context          *structpb.Struct
	Consistency      openfgav1.ConsistencyPreference
}

func validateRequestTupleInModel(m *modelgraph.AuthorizationModelGraph, t *openfgav1.TupleKey) error {
	objectType := tuple.GetType(t.GetObject())
	_, ok := m.GetNodeByID(tuple.ToObjectRelationString(objectType, t.GetRelation()))
	if !ok {
		return ErrValidation
	}
	userType := tuple.GetType(t.GetUser())
	if tuple.IsObjectRelation(t.GetUser()) {
		objectRelation := tuple.GetRelation(t.GetUser())
		userType = tuple.ToObjectRelationString(userType, objectRelation)
	}
	if _, ok := m.GetNodeByID(userType); !ok {
		return ErrInvalidUser
	}

	return nil
}

func validateCtxTupleInModel(m *modelgraph.AuthorizationModelGraph, t *openfgav1.TupleKey) error {
	objectType := tuple.GetType(t.GetObject())
	node, ok := m.GetNodeByID(tuple.ToObjectRelationString(objectType, t.GetRelation()))
	// if the object#relation node does not exist the relation is not defined in the model
	if !ok {
		return &tuple.InvalidTupleError{Cause: ErrValidation, TupleKey: t}
	}

	userType := tuple.GetType(t.GetUser())
	if tuple.IsObjectRelation(t.GetUser()) {
		objectRelation := tuple.GetRelation(t.GetUser())
		userType = tuple.ToObjectRelationString(userType, objectRelation)
	} else if tuple.IsTypedWildcard(t.GetUser()) {
		userType = t.GetUser()
	}

	directEdge, ok := m.GetDirectEdgeForUserType(node, userType)
	// if there is not directEdge that belongs to the relation node that connects to the userType then the tuple is invalid,
	// usertype could be the object, it could be the wildcard definition of the object or it could be the userset (object#relation)
	if !ok {
		return &tuple.InvalidTupleError{Cause: ErrValidation, TupleKey: t}
	}

	conditionName := ""
	if t.Condition != nil {
		conditionName = t.Condition.Name
	}
	if !slices.Contains(directEdge.GetConditions(), conditionName) {
		return &tuple.InvalidTupleError{Cause: ErrValidation, TupleKey: t}
	}

	return nil
}

func NewRequest(p RequestParams) (*Request, error) {
	if p.StoreID == "" {
		return nil, ErrMissingStoreID
	}

	if p.Model == nil {
		return nil, ErrMissingAuthZModelID
	}

	modelID := p.Model.GetModelID()
	if modelID == "" {
		return nil, ErrMissingAuthZModelID
	}

	if !tuple.IsValidUser(p.TupleKey.GetUser()) {
		return nil, ErrInvalidUser
	}

	if err := validateRequestTupleInModel(p.Model, p.TupleKey); err != nil {
		return nil, err
	}

	for _, t := range p.ContextualTuples {
		if err := validateCtxTupleInModel(p.Model, t); err != nil {
			return nil, err
		}
	}

	userType := tuple.GetType(p.TupleKey.GetUser())
	if tuple.IsObjectRelation(p.TupleKey.GetUser()) {
		objectRelation := tuple.GetRelation(p.TupleKey.GetUser())
		userType = tuple.ToObjectRelationString(userType, objectRelation)
	}

	r := &Request{
		StoreID:              p.StoreID,
		AuthorizationModelID: modelID,
		TupleKey:             p.TupleKey,
		ContextualTuples:     p.ContextualTuples,
		Context:              p.Context,
		Consistency:          p.Consistency,

		objectType: tuple.GetType(p.TupleKey.GetObject()),
		userType:   userType,
	}

	keyBuilder := &strings.Builder{}
	// TODO: we really should refactor this method to not depend on storage package and improve the implementation overall
	err := storage.WriteInvariantCheckCacheKey(keyBuilder, &storage.CheckCacheKeyParams{
		StoreID:              p.StoreID,
		AuthorizationModelID: modelID,
		ContextualTuples:     p.ContextualTuples,
		Context:              p.Context,
	})
	if err != nil {
		return nil, err
	}

	hasher := xxhash.New()
	_, _ = hasher.WriteString(keyBuilder.String())
	r.invariantCacheKey = strconv.FormatUint(hasher.Sum64(), 10)

	tup := tuple.From(r.GetTupleKey())
	cacheKeyString := tup.String() + r.GetInvariantCacheKey()

	hasher = xxhash.New()
	_, _ = hasher.WriteString(cacheKeyString)

	cacheKey := CacheKeyPrefix + strconv.FormatUint(hasher.Sum64(), 10)
	r.cacheKey = cacheKey

	r.buildContextualTupleMaps()

	return r, nil
}

func (r *Request) GetCacheKey() string {
	return r.cacheKey
}

func (r *Request) buildContextualTupleMaps() {
	ctxTuples := r.GetContextualTuples()
	if len(ctxTuples) == 0 {
		return
	}

	// Pre-allocate maps with estimated capacity
	r.ctxTuplesByUserID = make(map[string][]*openfgav1.TupleKey, len(ctxTuples))
	r.ctxTuplesByObjectID = make(map[string][]*openfgav1.TupleKey, len(ctxTuples))

	// Use string builder to avoid allocations
	var keyBuilder strings.Builder

	for _, t := range ctxTuples {
		user := t.GetUser()
		relation := t.GetRelation()
		object := t.GetObject()

		objectType, _ := tuple.SplitObject(object)

		var userType string

		// Handle two different cases for user
		if tuple.IsObjectRelation(user) {
			// User is an object relation (e.g., "group:1#member")
			// Extract the base object without the relation
			baseObject, objectRelation := tuple.SplitObjectRelation(user)

			userType, _ = tuple.SplitObject(baseObject)
			userType = tuple.ToObjectRelationString(userType, objectRelation)
		} else {
			// User is a simple object or user (e.g., "user:alice" or "group:1")
			userType, _ = tuple.SplitObject(user)
		}

		// Build key for ctxTuplesByUserID: userId+relation+objectType
		keyBuilder.Reset()
		keyBuilder.WriteString(user)
		keyBuilder.WriteString(relation)
		keyBuilder.WriteString(objectType)
		userKey := keyBuilder.String()

		r.ctxTuplesByUserID[userKey] = insertSortedTuple(r.ctxTuplesByUserID[userKey], t, "object")

		// Build key for ctxTuplesByObjectID: objectId+relation+userType
		keyBuilder.Reset()
		keyBuilder.WriteString(object)
		keyBuilder.WriteString(relation)
		keyBuilder.WriteString(userType)
		objectKey := keyBuilder.String()

		r.ctxTuplesByObjectID[objectKey] = insertSortedTuple(r.ctxTuplesByObjectID[objectKey], t, "user")
	}
}

// insertSortedTuple inserts a tuple into a sorted slice maintaining sort order based on sortKey.
func insertSortedTuple(slice []*openfgav1.TupleKey, t *openfgav1.TupleKey, sortKey string) []*openfgav1.TupleKey {
	var newKey string
	if sortKey == "object" {
		newKey = t.GetObject()
	} else {
		newKey = t.GetUser()
	}

	// Binary search to find insertion point
	i := sort.Search(len(slice), func(j int) bool {
		var existingKey string
		if sortKey == "object" {
			existingKey = slice[j].GetObject()
		} else {
			existingKey = slice[j].GetUser()
		}
		return existingKey >= newKey
	})

	// Check if duplicate exists at insertion point
	if i < len(slice) {
		var existingKey string
		if sortKey == "object" {
			existingKey = slice[i].GetObject()
		} else {
			existingKey = slice[i].GetUser()
		}

		// Skip if duplicate found
		if existingKey == newKey {
			return slice
		}
	}

	// Grow slice and insert at position
	slice = append(slice, nil)
	copy(slice[i+1:], slice[i:])
	slice[i] = t

	return slice
}

// GetContextualTuplesByUserID returns the map of contextual tuples indexed by userId+relation+objectType.
func (r *Request) GetContextualTuplesByUserID(userID, relation, objectType string) ([]*openfgav1.TupleKey, bool) {
	var keyBuilder strings.Builder
	keyBuilder.WriteString(userID)
	keyBuilder.WriteString(relation)
	keyBuilder.WriteString(objectType)

	entry, ok := r.ctxTuplesByUserID[keyBuilder.String()]
	return entry, ok
}

// GetContextualTuplesByObjectID returns the map of contextual tuples indexed by objectId+relation+userType.
func (r *Request) GetContextualTuplesByObjectID(objectID, relation, userType string) ([]*openfgav1.TupleKey, bool) {
	var keyBuilder strings.Builder
	keyBuilder.WriteString(objectID)
	keyBuilder.WriteString(relation)
	keyBuilder.WriteString(userType)

	entry, ok := r.ctxTuplesByObjectID[keyBuilder.String()]
	return entry, ok
}

func (r *Request) GetStoreID() string {
	if r == nil {
		return ""
	}
	return r.StoreID
}

func (r *Request) GetAuthorizationModelID() string {
	if r == nil {
		return ""
	}
	return r.AuthorizationModelID
}

func (r *Request) GetTupleKey() *openfgav1.TupleKey {
	if r == nil {
		return nil
	}
	return r.TupleKey
}

func (r *Request) GetContextualTuples() []*openfgav1.TupleKey {
	if r == nil {
		return nil
	}
	return r.ContextualTuples
}

func (r *Request) GetContext() *structpb.Struct {
	if r == nil {
		return nil
	}
	return r.Context
}

func (r *Request) GetConsistency() openfgav1.ConsistencyPreference {
	if r == nil {
		return openfgav1.ConsistencyPreference_UNSPECIFIED
	}
	return r.Consistency
}

func (r *Request) GetInvariantCacheKey() string {
	if r == nil {
		return ""
	}
	return r.invariantCacheKey
}

func (r *Request) GetObjectType() string {
	if r == nil {
		return ""
	}
	return r.objectType
}

func (r *Request) GetUserType() string {
	if r == nil {
		return ""
	}
	return r.userType
}

func (r *Request) cloneWithTupleKey(tk *openfgav1.TupleKey) *Request {
	req := &Request{
		StoreID:              r.GetStoreID(),
		AuthorizationModelID: r.GetAuthorizationModelID(),
		TupleKey:             tk,
		ContextualTuples:     r.GetContextualTuples(),
		Context:              r.GetContext(),
		Consistency:          r.GetConsistency(),
	}

	req.invariantCacheKey = r.GetInvariantCacheKey()
	req.ctxTuplesByObjectID = r.ctxTuplesByObjectID
	req.ctxTuplesByUserID = r.ctxTuplesByUserID
	req.objectType = tuple.GetType(tk.GetObject())

	userType := tuple.GetType(tk.GetUser())
	if tuple.IsObjectRelation(tk.GetUser()) {
		objectRelation := tuple.GetRelation(tk.GetUser())
		userType = tuple.ToObjectRelationString(userType, objectRelation)
	}
	req.userType = userType

	tup := tuple.From(tk)
	cacheKeyString := tup.String() + req.GetInvariantCacheKey()

	hasher := xxhash.New()
	_, _ = hasher.WriteString(cacheKeyString)

	cacheKey := CacheKeyPrefix + strconv.FormatUint(hasher.Sum64(), 10)
	req.cacheKey = cacheKey

	return req
}
