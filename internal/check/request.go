package check

import (
	"sort"
	"strconv"
	"strings"

	"github.com/cespare/xxhash/v2"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/graph"
	"github.com/openfga/openfga/pkg/tuple"
)

type Request struct {
	*graph.ResolveCheckRequest // TODO: Once we finish migrating, move into this package and drop useless fields (VisitedPaths, RequestMetadata)
	cacheKey                   string
	ctxTuplesByUserID          map[string][]*openfgav1.TupleKey
	ctxTuplesByObjectID        map[string][]*openfgav1.TupleKey
}

type RequestParams = graph.ResolveCheckRequestParams

func NewRequest(p RequestParams) (*Request, error) {
	req, err := graph.NewResolveCheckRequest(p)
	if err != nil {
		return nil, err
	}

	tup := tuple.From(req.GetTupleKey())
	cacheKeyString := tup.String() + req.GetInvariantCacheKey()
	hasher := xxhash.New()
	_, _ = hasher.WriteString(cacheKeyString)
	cacheKey := CacheKeyPrefix + strconv.FormatUint(hasher.Sum64(), 10)

	r := &Request{
		ResolveCheckRequest: req,
		cacheKey:            cacheKey,
	}
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
