package storage

import (
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	tupleutils "github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/types"
)

// TupleRecord represents a record structure used
// to store information about a specific tuple.
type TupleRecord struct {
	Store string

	ObjectType string
	ObjectID   string
	Relation   string

	// Deprecated: this field will be going away in favor of the discrete user/subject components
	// below (SubjectType, SubjectID, SubjectRelation)
	User            string
	SubjectType     string
	SubjectID       string
	SubjectRelation string

	ConditionName    string
	ConditionContext *structpb.Struct
	Ulid             string
	InsertedAt       time.Time
}

func (t *TupleRecord) AsRelationshipTuple() *types.RelationshipTuple {
	return &types.RelationshipTuple{
		Object: types.Object{
			Type: t.ObjectType,
			ID:   t.ObjectID,
		},
		Relation: t.Relation,
		Subject:  types.SubjectFromParts(t.SubjectType, t.SubjectID, t.SubjectRelation),
	}
}

// AsTuple converts a [TupleRecord] into a [*openfgav1.Tuple].
func (t *TupleRecord) AsTuple() *openfgav1.Tuple {
	return &openfgav1.Tuple{
		Key: tupleutils.NewTupleKeyWithCondition(
			tupleutils.BuildObject(t.ObjectType, t.ObjectID),
			t.Relation,
			t.User,
			t.ConditionName,
			t.ConditionContext,
		),
		Timestamp: timestamppb.New(t.InsertedAt),
	}
}

// MapTupleRecordToRelationshipTupleMapper maps the provided TupleRecord to the internally well-known
// RelationshipTuple type.
func MapTupleRecordToRelationshipTupleMapper(r *TupleRecord) *types.RelationshipTuple {
	return &types.RelationshipTuple{
		Object: types.Object{
			Type: r.ObjectType,
			ID:   r.ObjectID,
		},
		Relation: r.Relation,
		Subject:  types.SubjectFromParts(r.SubjectType, r.SubjectID, r.SubjectRelation),
	}
}

// MapTupleRecordToTupleProtobuf maps the provided TupleRecord to the OpenFGA protobuf representing
// a Tuple (Timestamp, TupleKey).
//
// Deprecated: Internal consumers should lean toward using 'types.RelationshipTuple' for most cases
// since the attached Timestamp is unused in the majority of scenarios.
func MapTupleRecordToTupleProtobuf(r *TupleRecord) *openfgav1.Tuple {
	return r.AsTuple()
}
