package check

import (
	"fmt"
	"sync"

	dispatchv1 "github.com/openfga/openfga/internal/proto/dispatch/v1alpha1"
)

// ResolveCheckSet represents the set of individual dispatched Check
// results per objectID. The keys are the objectIDs and the values
// are the Check result for that objectID.
type ResolvedCheckSet struct {
	resolvedCheckSetMap       map[string]*dispatchv1.DispatchCheckResult
	hasAnyPermittableDecision bool
	mu                        sync.RWMutex
}

// AsMap returns this ResolvedCheckSet as a map.
func (r *ResolvedCheckSet) AsMap() map[string]*dispatchv1.DispatchCheckResult {
	return r.resolvedCheckSetMap
}

func NewResolvedCheckSet() *ResolvedCheckSet {
	return &ResolvedCheckSet{
		resolvedCheckSetMap:       map[string]*dispatchv1.DispatchCheckResult{},
		hasAnyPermittableDecision: false,
	}
}

// ResolveCheckSetFromSlice constructs a ResolvedCheckSet from the list
// of objectIDs provided with initial results defaulted to empty values.
func ResolvedCheckSetFromSlice(objectIDs ...string) *ResolvedCheckSet {
	m := make(map[string]*dispatchv1.DispatchCheckResult, len(objectIDs))

	for _, objectID := range objectIDs {
		m[objectID] = &dispatchv1.DispatchCheckResult{}
	}

	return &ResolvedCheckSet{
		resolvedCheckSetMap:       m,
		hasAnyPermittableDecision: false,
	}
}

// Set writes or overwrites the DispatchCheckResult value stored for the provided objectID.
func (r *ResolvedCheckSet) Set(objectID string, result *dispatchv1.DispatchCheckResult) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.resolvedCheckSetMap[objectID] = result
}

// Get returns the DispatchCheckResult associated with the provided objectID. If the objectID
// is not contained in the set then an empty result is returned.
func (r *ResolvedCheckSet) Get(objectID string) *dispatchv1.DispatchCheckResult {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result, ok := r.resolvedCheckSetMap[objectID]
	if !ok {
		return &dispatchv1.DispatchCheckResult{}
	}

	return result
}

// Equal performs a deep equality comparison between ResolvedCheckSet 'r' and 'rhs'
// and reports if they are equal.
func (r *ResolvedCheckSet) Equal(rhs *ResolvedCheckSet) bool {
	if r == rhs {
		return true
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.resolvedCheckSetMap) != len(rhs.resolvedCheckSetMap) {
		return false
	}

	for key, lhsVal := range r.resolvedCheckSetMap {
		rhsVal, ok := rhs.resolvedCheckSetMap[key]
		if !ok {
			return false
		}

		if rhsVal.GetAllowed() != lhsVal.GetAllowed() {
			return false
		}
	}

	return true
}

// Union merges all of the key/value pairs provided in 'l' with this ResolvedCheckSet 'r'.
func (r *ResolvedCheckSet) Union(l map[string]*dispatchv1.DispatchCheckResult) *ResolvedCheckSet {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, value := range l {
		r.resolvedCheckSetMap[key] = value

		if value.GetAllowed() {
			r.hasAnyPermittableDecision = true
		}
	}

	return r
}

// HasAnyPermittedDecision returns true if any of the results for any of the keys in this
// ResolvedCheckSet have an outcome of {Allowed: true}.
func (r *ResolvedCheckSet) HasAnyPermittedDecision() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// if any previous result adding to this ResovledCheckSet was a permittable decision, then
	// we can return quicker, otherwise we should evaluate the results in the current set.
	if r.hasAnyPermittableDecision {
		return true
	}

	for _, value := range r.resolvedCheckSetMap {
		if value.GetAllowed() {
			return true
		}
	}

	return false
}

// ObjectIDsSet represents a set of objectIDs and is provided as a utility/helper to
// simplify the composition of adding and removing objectIDs to keep track of which
// objectIDs need further evaluation.
type ObjectIDsSet map[string]struct{}

// ObjectIDsSetFromSlice constructs a set of the slice of objectIDs provided with any duplicates
// removed. The underlying representation of the returned ObjectIDsSet is a map.
func ObjectIDsSetFromSlice(objectIDs ...string) ObjectIDsSet {
	m := make(ObjectIDsSet, len(objectIDs))

	for _, objectID := range objectIDs {
		m[objectID] = struct{}{}
	}

	return m
}

// Add adds the provided key to this set.
func (m ObjectIDsSet) Add(key string) {
	m[key] = struct{}{}
}

// Remove removes the provided key from this set.
func (m ObjectIDsSet) Remove(key string) {
	delete(m, key)
}

// Union merges all of the key/value pairs in the set 'm' and set 'other'.
func (m ObjectIDsSet) Union(other ObjectIDsSet) ObjectIDsSet {
	union := ObjectIDsSet{}

	for objectID := range m {
		union.Add(objectID)
	}

	for objectID := range other {
		union.Add(objectID)
	}

	return union
}

// AsSlice returns this ObjectIDsSet as a slice of values.
//
// There shouldn't be duplicates in the returned slice because the underlying representation of
// the ObjectIDsSet is a map, which doesn't allow for duplicate keys.
func (m ObjectIDsSet) AsSlice() []string {
	s := make([]string, 0, len(m))

	for key := range m {
		s = append(s, key)
	}

	return s
}

// DispatchSetGroup represents the set of dispatches that are grouped by (type, relation).
//
// The keys of a DispatchSetGroup are 'type#relation' while the values are the IndividualDispatch
// (e.g. the dispatch query) that will dispatched and evaluated.
type DispatchSetGroup map[string]IndividualDispatch

// Add adds the DispatchItem to the DispatchSetGroup by appending the ObjectID of the DispatchItem to the
// list of ObjectIDs for the group of the same (type, relation) pair.
func (r DispatchSetGroup) Add(item DispatchItem) {
	key := fmt.Sprintf("%s#%s", item.Type, item.Relation)
	val := r[key]

	r[key] = IndividualDispatch{
		Type:      item.Type,
		Relation:  item.Relation,
		ObjectIDs: append(val.ObjectIDs, item.ObjectID),
	}
}

// AsSlice returns the DispatchSetGroup as a slice.
//
// There shouldn't be duplicates in the returned slice because the underlying representation of
// the DispatchSetGroup is a map, which doesn't allow for duplicate keys.
func (r DispatchSetGroup) AsSlice() []IndividualDispatch {
	s := make([]IndividualDispatch, 0, len(r))

	for _, val := range r {
		s = append(s, val)
	}

	return s
}

// DispatchItem represents a single item to be included in a DispatchSetGroup.
//
// Two different DispatchItem of the same type and relation are grouped together and
// the object ids are unioned together an included in an IndividualDispatch.
type DispatchItem struct {
	Type     string
	Relation string
	ObjectID string
}

// IndividualDispatch is an internal representation for a single dispatch grouped by
// type and relation for one or more objectIDs.
type IndividualDispatch struct {
	Type      string
	Relation  string
	ObjectIDs []string
}
