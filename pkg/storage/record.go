package storage

import (
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TupleRecord struct {
	Store      string
	ObjectType string
	ObjectID   string
	Relation   string
	User       string
	Ulid       string
	InsertedAt time.Time
}

func (t *TupleRecord) AsTuple() *openfgav1.Tuple {
	tk := &openfgav1.TupleKey{
		Object:   tupleUtils.BuildObject(t.ObjectType, t.ObjectID),
		Relation: t.Relation,
		User:     t.User,
	}

	return &openfgav1.Tuple{
		Key:       tk,
		Timestamp: timestamppb.New(t.InsertedAt),
	}
}

// Match returns true if all the fields in *TupleRecord are equal to the same field in the target *TupleKey.
// If the input Object doesn't specify an ID, only the Object Types are compared.
// If a field in the input parameter is empty, it is ignored in the comparison.
func (t *TupleRecord) Match(target *openfgav1.TupleKey) bool {
	if target.Object != "" {
		td, objectid := tupleUtils.SplitObject(target.Object)
		if objectid == "" {
			if td != t.ObjectType {
				return false
			}
		} else {
			if td != t.ObjectType || objectid != t.ObjectID {
				return false
			}
		}
	}
	if target.Relation != "" && t.Relation != target.Relation {
		return false
	}
	if target.User != "" && t.User != target.User {
		return false
	}
	return true
}

// Find returns true if there is any *TupleRecord for which storage.Match returns true
func Find(records []*TupleRecord, tupleKey *openfgav1.TupleKey) bool {
	for _, tr := range records {
		if tr.Match(tupleKey) {
			return true
		}
	}
	return false
}
