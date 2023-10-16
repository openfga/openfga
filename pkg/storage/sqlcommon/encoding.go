package sqlcommon

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/proto"
)

func marshalRelationshipCondition(
	rel *openfgav1.RelationshipCondition,
) (string, *[]byte, error) {
	if rel != nil {
		context, err := proto.Marshal(rel.Context)
		if err != nil {
			return "", nil, err
		}

		return rel.ConditionName, &context, err
	}

	return "", nil, nil
}
