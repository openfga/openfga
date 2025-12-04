package grpc

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
)

func TestUsersetConversion(t *testing.T) {
	tests := []struct {
		name     string
		input    *openfgav1.Userset
		expected *storagev1.Userset
	}{
		{
			name:  "nil",
			input: nil,
		},
		{
			name: "this",
			input: &openfgav1.Userset{
				Userset: &openfgav1.Userset_This{},
			},
			expected: &storagev1.Userset{
				Userset: &storagev1.Userset_This{
					This: &storagev1.DirectUserset{},
				},
			},
		},
		{
			name: "computed_userset",
			input: &openfgav1.Userset{
				Userset: &openfgav1.Userset_ComputedUserset{
					ComputedUserset: &openfgav1.ObjectRelation{
						Object:   "doc:1",
						Relation: "viewer",
					},
				},
			},
			expected: &storagev1.Userset{
				Userset: &storagev1.Userset_ComputedUserset{
					ComputedUserset: &storagev1.ObjectRelation{
						Object:   "doc:1",
						Relation: "viewer",
					},
				},
			},
		},
		{
			name: "tuple_to_userset",
			input: &openfgav1.Userset{
				Userset: &openfgav1.Userset_TupleToUserset{
					TupleToUserset: &openfgav1.TupleToUserset{
						Tupleset: &openfgav1.ObjectRelation{
							Object:   "doc:1",
							Relation: "viewer",
						},
						ComputedUserset: &openfgav1.ObjectRelation{
							Object:   "doc:1",
							Relation: "editor",
						},
					},
				},
			},
			expected: &storagev1.Userset{
				Userset: &storagev1.Userset_TupleToUserset{
					TupleToUserset: &storagev1.TupleToUserset{
						Tupleset: &storagev1.ObjectRelation{
							Object:   "doc:1",
							Relation: "viewer",
						},
						ComputedUserset: &storagev1.ObjectRelation{
							Object:   "doc:1",
							Relation: "editor",
						},
					},
				},
			},
		},
		{
			name: "union",
			input: &openfgav1.Userset{
				Userset: &openfgav1.Userset_Union{
					Union: &openfgav1.Usersets{
						Child: []*openfgav1.Userset{
							{Userset: &openfgav1.Userset_This{}},
						},
					},
				},
			},
			expected: &storagev1.Userset{
				Userset: &storagev1.Userset_Union{
					Union: &storagev1.Usersets{
						Child: []*storagev1.Userset{
							{Userset: &storagev1.Userset_This{This: &storagev1.DirectUserset{}}},
						},
					},
				},
			},
		},
		{
			name: "intersection",
			input: &openfgav1.Userset{
				Userset: &openfgav1.Userset_Intersection{
					Intersection: &openfgav1.Usersets{
						Child: []*openfgav1.Userset{
							{Userset: &openfgav1.Userset_This{}},
						},
					},
				},
			},
			expected: &storagev1.Userset{
				Userset: &storagev1.Userset_Intersection{
					Intersection: &storagev1.Usersets{
						Child: []*storagev1.Userset{
							{Userset: &storagev1.Userset_This{This: &storagev1.DirectUserset{}}},
						},
					},
				},
			},
		},
		{
			name: "difference",
			input: &openfgav1.Userset{
				Userset: &openfgav1.Userset_Difference{
					Difference: &openfgav1.Difference{
						Base:     &openfgav1.Userset{Userset: &openfgav1.Userset_This{}},
						Subtract: &openfgav1.Userset{Userset: &openfgav1.Userset_This{}},
					},
				},
			},
			expected: &storagev1.Userset{
				Userset: &storagev1.Userset_Difference{
					Difference: &storagev1.Difference{
						Base:     &storagev1.Userset{Userset: &storagev1.Userset_This{This: &storagev1.DirectUserset{}}},
						Subtract: &storagev1.Userset{Userset: &storagev1.Userset_This{This: &storagev1.DirectUserset{}}},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			converted := toStorageUserset(test.input)
			if test.expected == nil {
				require.Nil(t, converted)
			} else {
				require.True(t, proto.Equal(test.expected, converted))
			}

			if test.input != nil {
				reverted := fromStorageUserset(converted)
				require.True(t, proto.Equal(test.input, reverted))
			} else {
				require.Nil(t, fromStorageUserset(nil))
			}
		})
	}
}

func TestUsersetsConversion(t *testing.T) {
	input := &openfgav1.Usersets{
		Child: []*openfgav1.Userset{
			{Userset: &openfgav1.Userset_This{}},
		},
	}
	expected := &storagev1.Usersets{
		Child: []*storagev1.Userset{
			{Userset: &storagev1.Userset_This{This: &storagev1.DirectUserset{}}},
		},
	}

	converted := toStorageUsersets(input)
	require.True(t, proto.Equal(expected, converted))

	reverted := fromStorageUsersets(converted)
	require.True(t, proto.Equal(input, reverted))

	require.Nil(t, toStorageUsersets(nil))
	require.Nil(t, fromStorageUsersets(nil))
}

func TestDifferenceConversion(t *testing.T) {
	input := &openfgav1.Difference{
		Base:     &openfgav1.Userset{Userset: &openfgav1.Userset_This{}},
		Subtract: &openfgav1.Userset{Userset: &openfgav1.Userset_This{}},
	}
	expected := &storagev1.Difference{
		Base:     &storagev1.Userset{Userset: &storagev1.Userset_This{This: &storagev1.DirectUserset{}}},
		Subtract: &storagev1.Userset{Userset: &storagev1.Userset_This{This: &storagev1.DirectUserset{}}},
	}

	converted := toStorageDifference(input)
	require.True(t, proto.Equal(expected, converted))

	reverted := fromStorageDifference(converted)
	require.True(t, proto.Equal(input, reverted))

	require.Nil(t, toStorageDifference(nil))
	require.Nil(t, fromStorageDifference(nil))
}

func TestTupleToUsersetConversion(t *testing.T) {
	input := &openfgav1.TupleToUserset{
		Tupleset: &openfgav1.ObjectRelation{
			Object:   "doc:1",
			Relation: "viewer",
		},
		ComputedUserset: &openfgav1.ObjectRelation{
			Object:   "doc:1",
			Relation: "editor",
		},
	}
	expected := &storagev1.TupleToUserset{
		Tupleset: &storagev1.ObjectRelation{
			Object:   "doc:1",
			Relation: "viewer",
		},
		ComputedUserset: &storagev1.ObjectRelation{
			Object:   "doc:1",
			Relation: "editor",
		},
	}

	converted := toStorageTupleToUserset(input)
	require.True(t, proto.Equal(expected, converted))

	reverted := fromStorageTupleToUserset(converted)
	require.True(t, proto.Equal(input, reverted))

	require.Nil(t, toStorageTupleToUserset(nil))
	require.Nil(t, fromStorageTupleToUserset(nil))
}

func TestTupleConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageTuple(nil))
		require.Nil(t, fromStorageTuple(nil))
	})

	t.Run("with_values", func(t *testing.T) {
		timestamp := timestamppb.Now()
		input := &openfgav1.Tuple{
			Key: &openfgav1.TupleKey{
				Object:   "doc:1",
				Relation: "viewer",
				User:     "user:anne",
			},
			Timestamp: timestamp,
		}

		converted := toStorageTuple(input)
		require.NotNil(t, converted)
		require.Equal(t, "doc:1", converted.GetKey().GetObject())
		require.Equal(t, "viewer", converted.GetKey().GetRelation())
		require.Equal(t, "user:anne", converted.GetKey().GetUser())
		require.True(t, proto.Equal(timestamp, converted.GetTimestamp()))

		reverted := fromStorageTuple(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("with_condition", func(t *testing.T) {
		input := &openfgav1.Tuple{
			Key: &openfgav1.TupleKey{
				Object:   "doc:1",
				Relation: "viewer",
				User:     "user:anne",
				Condition: &openfgav1.RelationshipCondition{
					Name: "is_valid",
					Context: &structpb.Struct{
						Fields: map[string]*structpb.Value{
							"x": structpb.NewNumberValue(10),
						},
					},
				},
			},
			Timestamp: timestamppb.Now(),
		}

		converted := toStorageTuple(input)
		require.NotNil(t, converted.GetKey().GetCondition())
		require.Equal(t, "is_valid", converted.GetKey().GetCondition().GetName())

		reverted := fromStorageTuple(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestTuplesConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, fromStorageTuples(nil))
	})

	t.Run("empty_slice", func(t *testing.T) {
		result := fromStorageTuples([]*storagev1.Tuple{})
		require.NotNil(t, result)
		require.Empty(t, result)
	})

	t.Run("with_values", func(t *testing.T) {
		input := []*storagev1.Tuple{
			{
				Key: &storagev1.TupleKey{
					Object:   "doc:1",
					Relation: "viewer",
					User:     "user:anne",
				},
			},
			{
				Key: &storagev1.TupleKey{
					Object:   "doc:2",
					Relation: "editor",
					User:     "user:bob",
				},
			},
		}

		result := fromStorageTuples(input)
		require.Len(t, result, 2)
		require.Equal(t, "doc:1", result[0].GetKey().GetObject())
		require.Equal(t, "doc:2", result[1].GetKey().GetObject())
	})
}

func TestTupleKeyConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageTupleKey(nil))
		require.Nil(t, fromStorageTupleKey(nil))
	})

	t.Run("basic_tuple_key", func(t *testing.T) {
		input := &openfgav1.TupleKey{
			Object:   "doc:1",
			Relation: "viewer",
			User:     "user:anne",
		}

		converted := toStorageTupleKey(input)
		require.NotNil(t, converted)
		require.Equal(t, "doc:1", converted.GetObject())
		require.Equal(t, "viewer", converted.GetRelation())
		require.Equal(t, "user:anne", converted.GetUser())
		require.Nil(t, converted.GetCondition())

		reverted := fromStorageTupleKey(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("with_condition", func(t *testing.T) {
		input := &openfgav1.TupleKey{
			Object:   "doc:1",
			Relation: "viewer",
			User:     "user:anne",
			Condition: &openfgav1.RelationshipCondition{
				Name: "is_valid",
			},
		}

		converted := toStorageTupleKey(input)
		require.NotNil(t, converted.GetCondition())
		require.Equal(t, "is_valid", converted.GetCondition().GetName())

		reverted := fromStorageTupleKey(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestTupleKeysConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageTupleKeys(nil))
	})

	t.Run("empty_slice", func(t *testing.T) {
		result := toStorageTupleKeys([]*openfgav1.TupleKey{})
		require.NotNil(t, result)
		require.Empty(t, result)
	})

	t.Run("with_values", func(t *testing.T) {
		input := []*openfgav1.TupleKey{
			{Object: "doc:1", Relation: "viewer", User: "user:anne"},
			{Object: "doc:2", Relation: "editor", User: "user:bob"},
		}

		result := toStorageTupleKeys(input)
		require.Len(t, result, 2)
		require.Equal(t, "doc:1", result[0].GetObject())
		require.Equal(t, "doc:2", result[1].GetObject())
	})
}

func TestTupleKeysFromDeletesConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageTupleKeysFromDeletes(nil))
	})

	t.Run("empty_slice", func(t *testing.T) {
		result := toStorageTupleKeysFromDeletes([]*openfgav1.TupleKeyWithoutCondition{})
		require.NotNil(t, result)
		require.Empty(t, result)
	})

	t.Run("with_values", func(t *testing.T) {
		input := []*openfgav1.TupleKeyWithoutCondition{
			{Object: "doc:1", Relation: "viewer", User: "user:anne"},
			{Object: "doc:2", Relation: "editor", User: "user:bob"},
		}

		result := toStorageTupleKeysFromDeletes(input)
		require.Len(t, result, 2)
		require.Equal(t, "doc:1", result[0].GetObject())
		require.Equal(t, "doc:2", result[1].GetObject())
		// Verify condition is nil for deletes
		require.Nil(t, result[0].GetCondition())
		require.Nil(t, result[1].GetCondition())
	})
}

func TestRelationshipConditionConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageRelationshipCondition(nil))
		require.Nil(t, fromStorageRelationshipCondition(nil))
	})

	t.Run("basic_condition", func(t *testing.T) {
		input := &openfgav1.RelationshipCondition{
			Name: "is_valid",
		}

		converted := toStorageRelationshipCondition(input)
		require.NotNil(t, converted)
		require.Equal(t, "is_valid", converted.GetName())
		require.Nil(t, converted.GetContext())

		reverted := fromStorageRelationshipCondition(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("with_context", func(t *testing.T) {
		input := &openfgav1.RelationshipCondition{
			Name: "is_valid",
			Context: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"x": structpb.NewNumberValue(10),
				},
			},
		}

		converted := toStorageRelationshipCondition(input)
		require.NotNil(t, converted.GetContext())
		require.NotNil(t, converted.GetContext().GetFields()["x"])

		reverted := fromStorageRelationshipCondition(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestObjectRelationConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageObjectRelation(nil))
		require.Nil(t, fromStorageObjectRelation(nil))
	})

	t.Run("with_values", func(t *testing.T) {
		input := &openfgav1.ObjectRelation{
			Object:   "doc:1",
			Relation: "viewer",
		}

		converted := toStorageObjectRelation(input)
		require.NotNil(t, converted)
		require.Equal(t, "doc:1", converted.GetObject())
		require.Equal(t, "viewer", converted.GetRelation())

		reverted := fromStorageObjectRelation(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("empty_strings", func(t *testing.T) {
		input := &openfgav1.ObjectRelation{
			Object:   "",
			Relation: "",
		}

		converted := toStorageObjectRelation(input)
		require.NotNil(t, converted)
		require.Empty(t, converted.GetObject())
		require.Empty(t, converted.GetRelation())

		reverted := fromStorageObjectRelation(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestObjectRelationsConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageObjectRelations(nil))
	})

	t.Run("empty_slice", func(t *testing.T) {
		result := toStorageObjectRelations([]*openfgav1.ObjectRelation{})
		require.NotNil(t, result)
		require.Empty(t, result)
	})

	t.Run("with_values", func(t *testing.T) {
		input := []*openfgav1.ObjectRelation{
			{Object: "doc:1", Relation: "viewer"},
			{Object: "doc:2", Relation: "editor"},
		}

		result := toStorageObjectRelations(input)
		require.Len(t, result, 2)
		require.Equal(t, "doc:1", result[0].GetObject())
		require.Equal(t, "doc:2", result[1].GetObject())
	})
}

func TestRelationReferenceConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageRelationReference(nil))
		require.Nil(t, fromStorageRelationReference(nil))
	})

	t.Run("basic_type", func(t *testing.T) {
		input := &openfgav1.RelationReference{
			Type: "user",
		}

		converted := toStorageRelationReference(input)
		require.NotNil(t, converted)
		require.Equal(t, "user", converted.GetType())

		reverted := fromStorageRelationReference(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("with_relation", func(t *testing.T) {
		input := &openfgav1.RelationReference{
			Type: "group",
			RelationOrWildcard: &openfgav1.RelationReference_Relation{
				Relation: "member",
			},
		}

		converted := toStorageRelationReference(input)
		require.NotNil(t, converted)
		require.Equal(t, "group", converted.GetType())
		require.Equal(t, "member", converted.GetRelation())

		reverted := fromStorageRelationReference(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("with_wildcard", func(t *testing.T) {
		input := &openfgav1.RelationReference{
			Type: "user",
			RelationOrWildcard: &openfgav1.RelationReference_Wildcard{
				Wildcard: &openfgav1.Wildcard{},
			},
		}

		converted := toStorageRelationReference(input)
		require.NotNil(t, converted)
		require.Equal(t, "user", converted.GetType())
		require.NotNil(t, converted.GetWildcard())

		reverted := fromStorageRelationReference(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("with_condition", func(t *testing.T) {
		input := &openfgav1.RelationReference{
			Type:      "user",
			Condition: "is_valid",
		}

		converted := toStorageRelationReference(input)
		require.Equal(t, "is_valid", converted.GetCondition())

		reverted := fromStorageRelationReference(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestRelationReferencesConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageRelationReferences(nil))
		require.Nil(t, fromStorageRelationReferences(nil))
	})

	t.Run("empty_slice", func(t *testing.T) {
		result := toStorageRelationReferences([]*openfgav1.RelationReference{})
		require.NotNil(t, result)
		require.Empty(t, result)

		result2 := fromStorageRelationReferences([]*storagev1.RelationReference{})
		require.NotNil(t, result2)
		require.Empty(t, result2)
	})

	t.Run("with_values", func(t *testing.T) {
		input := []*openfgav1.RelationReference{
			{Type: "user"},
			{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "member"}},
		}

		result := toStorageRelationReferences(input)
		require.Len(t, result, 2)
		require.Equal(t, "user", result[0].GetType())
		require.Equal(t, "group", result[1].GetType())
	})
}

func TestTupleWriteOptionsConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		result := fromStorageTupleWriteOptions(nil)
		require.Nil(t, result)
	})

	t.Run("ignore_options", func(t *testing.T) {
		input := &storagev1.TupleWriteOptions{
			OnMissingDelete:   storagev1.OnMissingDelete_ON_MISSING_DELETE_IGNORE,
			OnDuplicateInsert: storagev1.OnDuplicateInsert_ON_DUPLICATE_INSERT_IGNORE,
		}

		result := fromStorageTupleWriteOptions(input)
		require.Len(t, result, 2)

		// Apply options and verify
		opts := storage.NewTupleWriteOptions(result...)
		require.Equal(t, storage.OnMissingDeleteIgnore, opts.OnMissingDelete)
		require.Equal(t, storage.OnDuplicateInsertIgnore, opts.OnDuplicateInsert)
	})

	t.Run("error_options", func(t *testing.T) {
		input := &storagev1.TupleWriteOptions{
			OnMissingDelete:   storagev1.OnMissingDelete_ON_MISSING_DELETE_ERROR,
			OnDuplicateInsert: storagev1.OnDuplicateInsert_ON_DUPLICATE_INSERT_ERROR,
		}

		result := fromStorageTupleWriteOptions(input)
		require.Len(t, result, 2)

		opts := storage.NewTupleWriteOptions(result...)
		require.Equal(t, storage.OnMissingDeleteError, opts.OnMissingDelete)
		require.Equal(t, storage.OnDuplicateInsertError, opts.OnDuplicateInsert)
	})

	t.Run("empty_options_defaults_to_error", func(t *testing.T) {
		// Test with empty/default values (0) which should default to ERROR
		input := &storagev1.TupleWriteOptions{}

		result := fromStorageTupleWriteOptions(input)
		require.Len(t, result, 2)

		opts := storage.NewTupleWriteOptions(result...)
		require.Equal(t, storage.OnMissingDeleteError, opts.OnMissingDelete)
		require.Equal(t, storage.OnDuplicateInsertError, opts.OnDuplicateInsert)
	})
}

func TestAuthorizationModelConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageAuthorizationModel(nil))
		require.Nil(t, fromStorageAuthorizationModel(nil))
	})

	t.Run("basic_model", func(t *testing.T) {
		input := &openfgav1.AuthorizationModel{
			Id:            "01HXQZ9F8G7YRTXMN50BQP6XQZ",
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{Type: "user"},
			},
		}

		converted := toStorageAuthorizationModel(input)
		require.NotNil(t, converted)
		require.Equal(t, "01HXQZ9F8G7YRTXMN50BQP6XQZ", converted.GetId())
		require.Equal(t, "1.1", converted.GetSchemaVersion())
		require.Len(t, converted.GetTypeDefinitions(), 1)

		reverted := fromStorageAuthorizationModel(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("with_conditions", func(t *testing.T) {
		input := &openfgav1.AuthorizationModel{
			Id:            "01HXQZ9F8G7YRTXMN50BQP6XQZ",
			SchemaVersion: "1.1",
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{Type: "user"},
			},
			Conditions: map[string]*openfgav1.Condition{
				"is_valid": {
					Name:       "is_valid",
					Expression: "x < 100",
				},
			},
		}

		converted := toStorageAuthorizationModel(input)
		require.NotNil(t, converted.GetConditions())
		require.Contains(t, converted.GetConditions(), "is_valid")

		reverted := fromStorageAuthorizationModel(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestAuthorizationModelsConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageAuthorizationModels(nil))
		require.Nil(t, fromStorageAuthorizationModels(nil))
	})

	t.Run("empty_slice", func(t *testing.T) {
		result := toStorageAuthorizationModels([]*openfgav1.AuthorizationModel{})
		require.NotNil(t, result)
		require.Empty(t, result)

		result2 := fromStorageAuthorizationModels([]*storagev1.AuthorizationModel{})
		require.NotNil(t, result2)
		require.Empty(t, result2)
	})

	t.Run("with_values", func(t *testing.T) {
		input := []*openfgav1.AuthorizationModel{
			{Id: "model1", SchemaVersion: "1.1", TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "user"}}},
			{Id: "model2", SchemaVersion: "1.1", TypeDefinitions: []*openfgav1.TypeDefinition{{Type: "document"}}},
		}

		result := toStorageAuthorizationModels(input)
		require.Len(t, result, 2)
		require.Equal(t, "model1", result[0].GetId())
		require.Equal(t, "model2", result[1].GetId())
	})
}

func TestStoreConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageStore(nil))
		require.Nil(t, fromStorageStore(nil))
	})

	t.Run("basic_store", func(t *testing.T) {
		createdAt := timestamppb.Now()
		updatedAt := timestamppb.Now()

		input := &openfgav1.Store{
			Id:        "store-1",
			Name:      "Test Store",
			CreatedAt: createdAt,
			UpdatedAt: updatedAt,
		}

		converted := toStorageStore(input)
		require.NotNil(t, converted)
		require.Equal(t, "store-1", converted.GetId())
		require.Equal(t, "Test Store", converted.GetName())
		require.True(t, proto.Equal(createdAt, converted.GetCreatedAt()))
		require.True(t, proto.Equal(updatedAt, converted.GetUpdatedAt()))

		reverted := fromStorageStore(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("deleted_store", func(t *testing.T) {
		deletedAt := timestamppb.Now()

		input := &openfgav1.Store{
			Id:        "store-1",
			Name:      "Deleted Store",
			DeletedAt: deletedAt,
		}

		converted := toStorageStore(input)
		require.NotNil(t, converted.GetDeletedAt())

		reverted := fromStorageStore(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestStoresConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, fromStorageStores(nil))
	})

	t.Run("empty_slice", func(t *testing.T) {
		result := fromStorageStores([]*storagev1.Store{})
		require.NotNil(t, result)
		require.Empty(t, result)
	})

	t.Run("with_values", func(t *testing.T) {
		input := []*storagev1.Store{
			{Id: "store-1", Name: "Store One"},
			{Id: "store-2", Name: "Store Two"},
		}

		result := fromStorageStores(input)
		require.Len(t, result, 2)
		require.Equal(t, "store-1", result[0].GetId())
		require.Equal(t, "store-2", result[1].GetId())
	})
}

func TestTupleChangeConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageTupleChange(nil))
		require.Nil(t, fromStorageTupleChange(nil))
	})

	t.Run("write_operation", func(t *testing.T) {
		timestamp := timestamppb.Now()
		input := &openfgav1.TupleChange{
			TupleKey: &openfgav1.TupleKey{
				Object:   "doc:1",
				Relation: "viewer",
				User:     "user:anne",
			},
			Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			Timestamp: timestamp,
		}

		converted := toStorageTupleChange(input)
		require.NotNil(t, converted)
		require.Equal(t, storagev1.TupleOperation_TUPLE_OPERATION_WRITE, converted.GetOperation())
		require.True(t, proto.Equal(timestamp, converted.GetTimestamp()))

		reverted := fromStorageTupleChange(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("delete_operation", func(t *testing.T) {
		input := &openfgav1.TupleChange{
			TupleKey: &openfgav1.TupleKey{
				Object:   "doc:1",
				Relation: "viewer",
				User:     "user:anne",
			},
			Operation: openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
			Timestamp: timestamppb.Now(),
		}

		converted := toStorageTupleChange(input)
		require.Equal(t, storagev1.TupleOperation_TUPLE_OPERATION_DELETE, converted.GetOperation())

		reverted := fromStorageTupleChange(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestTupleChangesConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageTupleChanges(nil))
		require.Nil(t, fromStorageTupleChanges(nil))
	})

	t.Run("empty_slice", func(t *testing.T) {
		result := toStorageTupleChanges([]*openfgav1.TupleChange{})
		require.NotNil(t, result)
		require.Empty(t, result)

		result2 := fromStorageTupleChanges([]*storagev1.TupleChange{})
		require.NotNil(t, result2)
		require.Empty(t, result2)
	})

	t.Run("with_values", func(t *testing.T) {
		input := []*openfgav1.TupleChange{
			{
				TupleKey:  &openfgav1.TupleKey{Object: "doc:1", Relation: "viewer", User: "user:anne"},
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_WRITE,
			},
			{
				TupleKey:  &openfgav1.TupleKey{Object: "doc:2", Relation: "editor", User: "user:bob"},
				Operation: openfgav1.TupleOperation_TUPLE_OPERATION_DELETE,
			},
		}

		result := toStorageTupleChanges(input)
		require.Len(t, result, 2)
		require.Equal(t, storagev1.TupleOperation_TUPLE_OPERATION_WRITE, result[0].GetOperation())
		require.Equal(t, storagev1.TupleOperation_TUPLE_OPERATION_DELETE, result[1].GetOperation())
	})
}

func TestAssertionConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageAssertion(nil))
		require.Nil(t, fromStorageAssertion(nil))
	})

	t.Run("with_expectation_true", func(t *testing.T) {
		input := &openfgav1.Assertion{
			TupleKey: &openfgav1.AssertionTupleKey{
				Object:   "doc:1",
				Relation: "viewer",
				User:     "user:anne",
			},
			Expectation: true,
		}

		converted := toStorageAssertion(input)
		require.NotNil(t, converted)
		require.True(t, converted.GetExpectation())

		reverted := fromStorageAssertion(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("with_expectation_false", func(t *testing.T) {
		input := &openfgav1.Assertion{
			TupleKey: &openfgav1.AssertionTupleKey{
				Object:   "doc:1",
				Relation: "viewer",
				User:     "user:anne",
			},
			Expectation: false,
		}

		converted := toStorageAssertion(input)
		require.False(t, converted.GetExpectation())

		reverted := fromStorageAssertion(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestAssertionsConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageAssertions(nil))
		require.Nil(t, fromStorageAssertions(nil))
	})

	t.Run("empty_slice", func(t *testing.T) {
		result := toStorageAssertions([]*openfgav1.Assertion{})
		require.NotNil(t, result)
		require.Empty(t, result)

		result2 := fromStorageAssertions([]*storagev1.Assertion{})
		require.NotNil(t, result2)
		require.Empty(t, result2)
	})

	t.Run("with_values", func(t *testing.T) {
		input := []*openfgav1.Assertion{
			{
				TupleKey:    &openfgav1.AssertionTupleKey{Object: "doc:1", Relation: "viewer", User: "user:anne"},
				Expectation: true,
			},
			{
				TupleKey:    &openfgav1.AssertionTupleKey{Object: "doc:2", Relation: "editor", User: "user:bob"},
				Expectation: false,
			},
		}

		result := toStorageAssertions(input)
		require.Len(t, result, 2)
		require.True(t, result[0].GetExpectation())
		require.False(t, result[1].GetExpectation())
	})
}

func TestAssertionTupleKeyConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageAssertionTupleKey(nil))
		require.Nil(t, fromStorageAssertionTupleKey(nil))
	})

	t.Run("with_values", func(t *testing.T) {
		input := &openfgav1.AssertionTupleKey{
			Object:   "doc:1",
			Relation: "viewer",
			User:     "user:anne",
		}

		converted := toStorageAssertionTupleKey(input)
		require.NotNil(t, converted)
		require.Equal(t, "doc:1", converted.GetObject())
		require.Equal(t, "viewer", converted.GetRelation())
		require.Equal(t, "user:anne", converted.GetUser())

		reverted := fromStorageAssertionTupleKey(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("empty_strings", func(t *testing.T) {
		input := &openfgav1.AssertionTupleKey{
			Object:   "",
			Relation: "",
			User:     "",
		}

		converted := toStorageAssertionTupleKey(input)
		require.NotNil(t, converted)
		require.Empty(t, converted.GetObject())
		require.Empty(t, converted.GetRelation())
		require.Empty(t, converted.GetUser())

		reverted := fromStorageAssertionTupleKey(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestConditionConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageCondition(nil))
		require.Nil(t, fromStorageCondition(nil))
	})

	t.Run("basic_condition", func(t *testing.T) {
		input := &openfgav1.Condition{
			Name:       "is_valid",
			Expression: "x < 100",
			Parameters: map[string]*openfgav1.ConditionParamTypeRef{
				"x": {
					TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
				},
			},
		}

		converted := toStorageCondition(input)
		require.NotNil(t, converted)
		require.Equal(t, "is_valid", converted.GetName())
		require.Equal(t, "x < 100", converted.GetExpression())
		require.Contains(t, converted.GetParameters(), "x")

		reverted := fromStorageCondition(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("with_generic_types", func(t *testing.T) {
		input := &openfgav1.Condition{
			Name:       "is_valid",
			Expression: "x in y",
			Parameters: map[string]*openfgav1.ConditionParamTypeRef{
				"x": {
					TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_LIST,
					GenericTypes: []*openfgav1.ConditionParamTypeRef{
						{TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING},
					},
				},
			},
		}

		converted := toStorageCondition(input)
		require.NotNil(t, converted.GetParameters()["x"].GetGenericTypes())
		require.Len(t, converted.GetParameters()["x"].GetGenericTypes(), 1)

		reverted := fromStorageCondition(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestConditionsConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageConditions(nil))
		require.Nil(t, fromStorageConditions(nil))
	})

	t.Run("empty_map", func(t *testing.T) {
		result := toStorageConditions(map[string]*openfgav1.Condition{})
		require.NotNil(t, result)
		require.Empty(t, result)

		result2 := fromStorageConditions(map[string]*storagev1.Condition{})
		require.NotNil(t, result2)
		require.Empty(t, result2)
	})

	t.Run("with_values", func(t *testing.T) {
		input := map[string]*openfgav1.Condition{
			"cond1": {Name: "cond1", Expression: "x < 100"},
			"cond2": {Name: "cond2", Expression: "y > 0"},
		}

		result := toStorageConditions(input)
		require.Len(t, result, 2)
		require.Contains(t, result, "cond1")
		require.Contains(t, result, "cond2")
	})
}

func TestMetadataConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageMetadata(nil))
		require.Nil(t, fromStorageMetadata(nil))
	})

	t.Run("with_relations", func(t *testing.T) {
		input := &openfgav1.Metadata{
			Relations: map[string]*openfgav1.RelationMetadata{
				"viewer": {
					DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
						{Type: "user"},
					},
				},
			},
		}

		converted := toStorageMetadata(input)
		require.NotNil(t, converted)
		require.Contains(t, converted.GetRelations(), "viewer")

		reverted := fromStorageMetadata(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("with_module_and_source_info", func(t *testing.T) {
		input := &openfgav1.Metadata{
			Module: "test-module",
			SourceInfo: &openfgav1.SourceInfo{
				File: "test.fga",
			},
		}

		converted := toStorageMetadata(input)
		require.Equal(t, "test-module", converted.GetModule())
		require.Equal(t, "test.fga", converted.GetSourceInfo().GetFile())

		reverted := fromStorageMetadata(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}

func TestSourceInfoConversion(t *testing.T) {
	t.Run("nil", func(t *testing.T) {
		require.Nil(t, toStorageSourceInfo(nil))
		require.Nil(t, fromStorageSourceInfo(nil))
	})

	t.Run("with_file", func(t *testing.T) {
		input := &openfgav1.SourceInfo{
			File: "model.fga",
		}

		converted := toStorageSourceInfo(input)
		require.NotNil(t, converted)
		require.Equal(t, "model.fga", converted.GetFile())

		reverted := fromStorageSourceInfo(converted)
		require.True(t, proto.Equal(input, reverted))
	})

	t.Run("empty_file", func(t *testing.T) {
		input := &openfgav1.SourceInfo{
			File: "",
		}

		converted := toStorageSourceInfo(input)
		require.NotNil(t, converted)
		require.Empty(t, converted.GetFile())

		reverted := fromStorageSourceInfo(converted)
		require.True(t, proto.Equal(input, reverted))
	})
}
