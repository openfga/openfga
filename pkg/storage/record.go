package storage

import (
	"time"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	tupleutils "github.com/openfga/openfga/pkg/tuple"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type TupleRecord struct {
	Store            string
	ObjectType       string
	ObjectID         string
	Relation         string
	User             string
	ConditionName    string
	ConditionContext *structpb.Struct
	Ulid             string
	InsertedAt       time.Time
}

func (t *TupleRecord) AsTuple() *openfgav1.Tuple {
	tk := &openfgav1.TupleKey{
		Object:   tupleutils.BuildObject(t.ObjectType, t.ObjectID),
		Relation: t.Relation,
		User:     t.User,
	}

	if t.ConditionName != "" {
		tk.Condition = &openfgav1.RelationshipCondition{
			Name:    t.ConditionName,
			Context: t.ConditionContext,
		}

		// normalize nil context to empty context
		if t.ConditionContext == nil {
			tk.Condition.Context = &structpb.Struct{}
		}
	}

	return &openfgav1.Tuple{
		Key:       tk,
		Timestamp: timestamppb.New(t.InsertedAt),
	}
}
