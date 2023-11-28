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
