package storage

import (
	"slices"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage/cache/keys"
	"github.com/openfga/openfga/pkg/tuple"
)

// Operation names used for metrics and tracing attribution.
const (
	OperationRead                 = "Read"
	OperationReadStartingWithUser = "ReadStartingWithUser"
	OperationReadUsersetTuples    = "ReadUsersetTuples"
	OperationReadUserTuple        = "ReadUserTuple"
)

// Subfield prefixes used within hashed suffixes of iterator cache keys
// to distinguish different filter components.
const (
	PrefixUserFilter           = "UF"
	PrefixObjectIDs            = "OID"
	PrefixConditions           = "COND"
	PrefixUserTypeRestrictions = "UTR"
)

func copyObjectRelations(a keys.Array, rels []*openfgav1.ObjectRelation) int {
	if len(rels) == 0 || len(a) == 0 {
		return 0
	}

	values := make([]string, len(rels))

	for i, rel := range rels {
		subject := rel.GetObject()
		if relation := rel.GetRelation(); relation != "" {
			subject = tuple.ToObjectRelationString(subject, relation)
		}
		values[i] = subject
	}
	slices.Sort(values)

	var w int

	for i, e := range values {
		if i >= len(a) {
			break
		}
		a[i] = keys.String(e)
		w++
	}
	return w
}

func copyConditions(a keys.Array, conditions []string) int {
	if len(conditions) == 0 || len(a) == 0 {
		return 0
	}

	sorted := make([]string, len(conditions))
	copy(sorted, conditions)
	slices.Sort(sorted)

	var w int

	for _, c := range sorted {
		if w >= len(a) {
			break
		}

		// The empty string is the "unconditioned" sentinel (language NoCond)
		// and is a meaningful filter value: the datastore treats it as "match
		// only rows with no condition". It must be encoded, not dropped, so
		// that Conditions=[""] does not collide with Conditions=nil.
		a[w] = keys.String(c)
		w++
	}
	return w
}

// ReadStartingWithUserKey builds the iterator cache key for ReadStartingWithUser queries.
func ReadStartingWithUserKey(store string, filter ReadStartingWithUserFilter) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	a := make([]keys.Serializable, len(filter.UserFilter))
	n := copyObjectRelations(a, filter.UserFilter)
	builder.EncodeArray(a[:n])

	a = nil
	if filter.ObjectIDs != nil {
		values := filter.ObjectIDs.Values()
		a = make([]keys.Serializable, len(values))
		if len(values) > 0 {
			for i, oid := range values {
				a[i] = keys.String(oid)
			}
		}
	}
	builder.EncodeArray(a)

	a = make([]keys.Serializable, len(filter.Conditions))
	n = copyConditions(a, filter.Conditions)
	builder.EncodeArray(a[:n])

	digest := keys.GetDigest()
	digest.Write(builder.Bytes())
	suffix := digest.Sum64()
	digest.Close()

	builder.Reset()

	builder.EncodeString(PrefixIteratorCache)
	builder.EncodeString("RSWU")
	builder.EncodeString(store)
	builder.EncodeString(filter.ObjectType)
	builder.EncodeString(filter.Relation)
	builder.EncodeUint64(suffix)

	return builder.Key()
}

func copyRelationReferences(a keys.Array, refs []*openfgav1.RelationReference) int {
	if len(refs) == 0 || len(a) == 0 {
		return 0
	}

	parts := make([]string, 0, len(refs))
	for _, ref := range refs {
		var part string
		switch r := ref.GetRelationOrWildcard().(type) {
		case *openfgav1.RelationReference_Relation:
			part = ref.GetType() + "#" + r.Relation
		case *openfgav1.RelationReference_Wildcard:
			part = ref.GetType() + ":*"
		default:
			part = ref.GetType()
		}
		parts = append(parts, part)
	}

	slices.Sort(parts)

	var w int

	for i, part := range parts {
		if i >= len(a) {
			break
		}
		a[i] = keys.String(part)
		w++
	}
	return w
}

// ReadUsersetTuplesKey builds the iterator cache key for ReadUsersetTuples queries.
func ReadUsersetTuplesKey(store string, filter ReadUsersetTuplesFilter) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	a := make(keys.Array, len(filter.AllowedUserTypeRestrictions))
	n := copyRelationReferences(a, filter.AllowedUserTypeRestrictions)
	builder.EncodeArray(a[:n])

	a = make(keys.Array, len(filter.Conditions))
	n = copyConditions(a, filter.Conditions)
	builder.EncodeArray(a[:n])

	digest := keys.GetDigest()
	digest.Write(builder.Bytes())
	suffix := digest.Sum64()
	digest.Close()

	builder.Reset()

	builder.EncodeString(PrefixIteratorCache)
	builder.EncodeString("RUT")
	builder.EncodeString(store)
	builder.EncodeString(filter.Object)
	builder.EncodeString(filter.Relation)
	builder.EncodeUint64(suffix)

	return builder.Key()
}

// ReadKey builds the iterator cache key for Read queries.
func ReadKey(store string, filter ReadFilter) keys.Key {
	builder := keys.GetBuilder()
	defer builder.Close()

	a := make(keys.Array, len(filter.Conditions))
	n := copyConditions(a, filter.Conditions)
	builder.EncodeArray(a[:n])

	digest := keys.GetDigest()
	digest.Write(builder.Bytes())
	suffix := digest.Sum64()
	digest.Close()

	builder.Reset()

	builder.EncodeString(PrefixIteratorCache)
	builder.EncodeString("READ")
	builder.EncodeString(store)
	builder.EncodeString(filter.Object)
	builder.EncodeString(filter.Relation)
	builder.EncodeString(filter.User)
	builder.EncodeUint64(suffix)

	return builder.Key()
}
