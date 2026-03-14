package storagewrappersutil

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/tuple"
)

func BenchmarkReadKey(b *testing.B) {
	tk := tuple.NewTupleKey("document:budget-123", "viewer", "user:jon")

	for n := 0; n < b.N; n++ {
		_ = ReadKey("01HXYZ", tk)
	}
}

func BenchmarkReadUsersetTuplesKey(b *testing.B) {
	filter := storage.ReadUsersetTuplesFilter{
		Object:   "document:budget-123",
		Relation: "viewer",
		AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
			{
				Type:               "group",
				RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"},
			},
			{
				Type:               "user",
				RelationOrWildcard: &openfgav1.RelationReference_Wildcard{Wildcard: &openfgav1.Wildcard{}},
			},
		},
	}

	for n := 0; n < b.N; n++ {
		_ = ReadUsersetTuplesKey("01HXYZ", filter)
	}
}

func BenchmarkReadStartingWithUserKey(b *testing.B) {
	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{
			{Object: "user:jon"},
			{Object: "user:*"},
		},
	}

	for n := 0; n < b.N; n++ {
		_, _ = ReadStartingWithUserKey("01HXYZ", filter)
	}
}
