package storagewrappersutil

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/cache"
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
) string {
	var builder cache.KeyBuilder
	storage.ApplyReadStartingWithUserCacheKeyPrefix(&builder, store, filter.ObjectType, filter.Relation)

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
		var hasher cache.KeyBuilder

		for _, oid := range filter.ObjectIDs.Values() {
			hasher.WriteString(oid)
		}
		builder.WriteUint64(hasher.Sum64())
	}

	return builder.String()
}

func ReadUsersetTuplesKey(store string, filter storage.ReadUsersetTuplesFilter) string {
	var builder cache.KeyBuilder
	storage.ApplyReadUsersetTuplesCacheKeyPrefix(&builder, store, filter.Object, filter.Relation)

	filterKey := storage.BuildUserTypeRestrictionsHash(filter.AllowedUserTypeRestrictions)

	builder.WriteUint64(filterKey)

	return builder.String()
}

func ReadKey(store string, tupleKey *openfgav1.TupleKey) string {
	var builder cache.KeyBuilder
	storage.ApplyReadCacheKey(&builder, store, tuple.TupleKeyToString(tupleKey))
	return builder.String()
}
