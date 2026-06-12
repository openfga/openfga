package check

import (
	"errors"
	"slices"
	"sort"

	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/modelgraph"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache/keys"
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

	cacheKey keys.Key

	// invariantCacheKey encodes the request parts that are constant across
	// sub-problems (storeID, modelID, context, contextual tuples). Computed
	// once and propagated to clones.
	invariantCacheKey uint64

	objectType          string
	userType            string
	userWildcard        bool
	tupleString         string
	ctxTuplesByUserID   map[keys.Key][]*openfgav1.TupleKey
	ctxTuplesByObjectID map[keys.Key][]*openfgav1.TupleKey
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
	if t.GetCondition() != nil {
		conditionName = t.GetCondition().GetName()
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

		tupleString:  tuple.TupleKeyWithConditionToString(p.TupleKey),
		objectType:   tuple.GetType(p.TupleKey.GetObject()),
		userType:     userType,
		userWildcard: tuple.IsTypedWildcard(p.TupleKey.GetUser()),
	}

	r.invariantCacheKey = storage.InvariantCacheKey(p.StoreID, modelID, p.Context, p.ContextualTuples...)

	tupleKey := r.GetTupleKey()
	r.cacheKey = storage.CheckCacheKey(
		r.GetStoreID(),
		tupleKey.GetObject(),
		tupleKey.GetRelation(),
		tupleKey.GetUser(),
		r.GetInvariantCacheKey(),
	)
	r.buildContextualTupleMaps()
	return r, nil
}

func (r *Request) GetCacheKey() keys.Key {
	return r.cacheKey
}

func (r *Request) IsTypedWildcard() bool {
	return r.userWildcard
}

func (r *Request) buildContextualTupleMaps() {
	ctxTuples := r.GetContextualTuples()
	if len(ctxTuples) == 0 {
		return
	}

	// Pre-allocate maps with estimated capacity
	r.ctxTuplesByUserID = make(map[keys.Key][]*openfgav1.TupleKey, len(ctxTuples))
	r.ctxTuplesByObjectID = make(map[keys.Key][]*openfgav1.TupleKey, len(ctxTuples))

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

		userKey := ctxTuplesByUserKey(user, relation, objectType)

		r.ctxTuplesByUserID[userKey] = insertSortedTuple(r.ctxTuplesByUserID[userKey], t, "object")

		objectKey := ctxTuplesByObjectKey(object, relation, userType)

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

	slice = slices.Insert(slice, i, t)

	return slice
}

func ctxTuplesByUserKey(userID, relation, objectType string) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(userID)
	builder.EncodeString(relation)
	builder.EncodeString(objectType)
	return builder.Key()
}

// GetContextualTuplesByUserID returns the map of contextual tuples indexed by userId+relation+objectType.
func (r *Request) GetContextualTuplesByUserID(userID, relation, objectType string) ([]*openfgav1.TupleKey, bool) {
	key := ctxTuplesByUserKey(userID, relation, objectType)
	entry, ok := r.ctxTuplesByUserID[key]
	return entry, ok
}

func ctxTuplesByObjectKey(objectID, relation, userType string) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	builder.EncodeString(objectID)
	builder.EncodeString(relation)
	builder.EncodeString(userType)
	return builder.Key()
}

// GetContextualTuplesByObjectID returns the map of contextual tuples indexed by objectId+relation+userType.
func (r *Request) GetContextualTuplesByObjectID(objectID, relation, userType string) ([]*openfgav1.TupleKey, bool) {
	key := ctxTuplesByObjectKey(objectID, relation, userType)
	entry, ok := r.ctxTuplesByObjectID[key]
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

func (r *Request) GetInvariantCacheKey() uint64 {
	if r == nil {
		return 0
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

func (r *Request) GetTupleString() string {
	if r == nil {
		return ""
	}
	return r.tupleString
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
	req.tupleString = tuple.TupleKeyWithConditionToString(tk)

	userType := tuple.GetType(tk.GetUser())
	if tuple.IsObjectRelation(tk.GetUser()) {
		objectRelation := tuple.GetRelation(tk.GetUser())
		userType = tuple.ToObjectRelationString(userType, objectRelation)
	}
	req.userType = userType
	req.cacheKey = storage.CheckCacheKey(
		req.GetStoreID(),
		tk.GetObject(),
		tk.GetRelation(),
		tk.GetUser(),
		req.GetInvariantCacheKey(),
	)
	req.userWildcard = tuple.IsWildcard(tk.GetUser())
	return req
}
