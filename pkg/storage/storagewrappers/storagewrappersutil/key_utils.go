package storagewrappersutil

import (
	"github.com/cespare/xxhash/v2"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

const (
	OperationRead                 = "Read"
	OperationReadStartingWithUser = "ReadStartingWithUser"
	OperationReadUsersetTuples    = "ReadUsersetTuples"
	OperationReadUserTuple        = "ReadUserTuple"
)

func ReadStartingWithUserKey(
	store string,
	filter storage.ReadStartingWithUserFilter,
) (string, error) {
	var builder storage.CacheKeyBuilder
	storage.GetReadStartingWithUserCacheKeyPrefix(&builder, store, filter.ObjectType, filter.Relation)

	size := len(filter.UserFilter)
	if filter.ObjectIDs != nil {
		size++
	}

	builder.Grow(size)

	for _, objectRel := range filter.UserFilter {
		subject := objectRel.GetObject()
		if objectRel.GetRelation() != "" {
			subject = tuple.ToObjectRelationString(objectRel.GetObject(), objectRel.GetRelation())
		}
		builder.WriteString(subject)
	}

	if filter.ObjectIDs != nil {
		hasher := xxhash.New()
		for _, oid := range filter.ObjectIDs.Values() {
			hasher.WriteString(oid)
			hasher.Write([]byte{0})
		}
		builder.Write(hasher.Sum([]byte{}))
	}

	return builder.String(), nil
}

func ReadUsersetTuplesKey(store string, filter storage.ReadUsersetTuplesFilter) string {
	var builder storage.CacheKeyBuilder
	storage.GetReadUsersetTuplesCacheKeyPrefix(&builder, store, filter.Object, filter.Relation)

	filterKey := storage.BuildUserTypeRestrictionsHash(filter.AllowedUserTypeRestrictions)

	builder.Write(filterKey)

	return builder.String()
}

func ReadKey(store string, tupleKey *openfgav1.TupleKey) string {
	var builder storage.CacheKeyBuilder
	storage.GetReadCacheKey(&builder, store, tuple.TupleKeyToString(tupleKey))
	return builder.String()
}
