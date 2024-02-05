package sqlcommon

import (
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"google.golang.org/protobuf/proto"
)

func marshalRelationshipCondition(
	rel *openfgav1.RelationshipCondition,
) (name string, context []byte, err error) {
	if rel != nil {
		// Normalize empty context to nil.
		if rel.Context != nil && len(rel.Context.GetFields()) > 0 {
			context, err = proto.Marshal(rel.Context)
			if err != nil {
				return name, context, err
			}
		}

		return rel.Name, context, err
	}

	return name, context, err
}
