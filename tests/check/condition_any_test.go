package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
)

// conditionAnyModel returns an authorization model with a condition that has
// a parameter of type 'any'. The DSL parser does not support 'any' as a
// standalone condition parameter type, so the model is constructed via proto.
func conditionAnyModel() *openfgav1.WriteAuthorizationModelRequest {
	return &openfgav1.WriteAuthorizationModelRequest{
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": typesystem.This(),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								typesystem.ConditionedRelationReference(
									typesystem.DirectRelationReference("user", ""),
									"any_cond",
								),
							},
						},
					},
				},
			},
		},
		Conditions: map[string]*openfgav1.Condition{
			"any_cond": {
				Name:       "any_cond",
				Expression: "data != null",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"data": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_ANY,
					},
				},
			},
		},
	}
}

func mustNewStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err != nil {
		panic(err)
	}
	return s
}

type conditionAnyTestCase struct {
	name    string
	context *structpb.Struct
	allowed bool
}

func runConditionAnyTests(t *testing.T, client tests.ClientInterface) {
	t.Helper()
	ctx := context.Background()

	storeResp, err := client.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: "condition_any_tests",
	})
	require.NoError(t, err)
	storeID := storeResp.GetId()

	modelReq := conditionAnyModel()
	modelReq.StoreId = storeID

	modelResp, err := client.WriteAuthorizationModel(ctx, modelReq)
	require.NoError(t, err)
	modelID := modelResp.GetAuthorizationModelId()

	_, err = client.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{
					User:     "user:jon",
					Relation: "viewer",
					Object:   "document:1",
					Condition: &openfgav1.RelationshipCondition{
						Name: "any_cond",
					},
				},
			},
		},
	})
	require.NoError(t, err)

	testCases := []conditionAnyTestCase{
		{
			name:    "string_value",
			context: mustNewStruct(map[string]any{"data": "hello"}),
			allowed: true,
		},
		{
			name:    "integer_value",
			context: mustNewStruct(map[string]any{"data": 42}),
			allowed: true,
		},
		{
			name:    "boolean_value",
			context: mustNewStruct(map[string]any{"data": true}),
			allowed: true,
		},
		{
			name:    "simple_list",
			context: mustNewStruct(map[string]any{"data": []any{1, 2, 3}}),
			allowed: true,
		},
		{
			name:    "nested_list",
			context: mustNewStruct(map[string]any{"data": []any{[]any{1, 2}, []any{3, 4}}}),
			allowed: true,
		},
		{
			name:    "simple_map",
			context: mustNewStruct(map[string]any{"data": map[string]any{"key": "value"}}),
			allowed: true,
		},
		{
			name:    "nested_map",
			context: mustNewStruct(map[string]any{"data": map[string]any{"a": map[string]any{"b": "c"}}}),
			allowed: true,
		},
		{
			name:    "null_value",
			context: mustNewStruct(map[string]any{"data": nil}),
			allowed: false,
		},
		{
			name: "complex_nested_structure",
			context: mustNewStruct(map[string]any{
				"data": map[string]any{
					"client1": map[string]any{
						"type": "a",
						"roles": []any{
							map[string]any{"name": "a", "id": 1},
							map[string]any{"name": "b", "id": 2},
						},
					},
					"client2": map[string]any{
						"type": "b",
						"roles": []any{
							map[string]any{"name": "c", "id": 3},
							map[string]any{"name": "b", "id": 2},
						},
					},
				},
			}),
			allowed: true,
		},
	}

	t.Run("Check", func(t *testing.T) {
		t.Run("no_context_returns_error", func(t *testing.T) {
			t.Parallel()
			_, err := client.Check(ctx, &openfgav1.CheckRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:jon",
					Relation: "viewer",
					Object:   "document:1",
				},
			})
			require.Error(t, err)
			e, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, 2000, int(e.Code()))
		})

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				resp, err := client.Check(ctx, &openfgav1.CheckRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					TupleKey: &openfgav1.CheckRequestTupleKey{
						User:     "user:jon",
						Relation: "viewer",
						Object:   "document:1",
					},
					Context: tc.context,
				})
				require.NoError(t, err)
				require.Equal(t, tc.allowed, resp.GetAllowed())
			})
		}
	})

	t.Run("ListObjects", func(t *testing.T) {
		t.Run("no_context_returns_error", func(t *testing.T) {
			t.Parallel()
			_, err := client.ListObjects(ctx, &openfgav1.ListObjectsRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				User:                 "user:jon",
				Relation:             "viewer",
				Type:                 "document",
			})
			require.Error(t, err)
			e, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, 2000, int(e.Code()))
		})

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				resp, err := client.ListObjects(ctx, &openfgav1.ListObjectsRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					User:                 "user:jon",
					Relation:             "viewer",
					Type:                 "document",
					Context:              tc.context,
				})
				require.NoError(t, err)
				if tc.allowed {
					require.Equal(t, []string{"document:1"}, resp.GetObjects())
				} else {
					require.Empty(t, resp.GetObjects())
				}
			})
		}
	})

	t.Run("ListUsers", func(t *testing.T) {
		t.Run("no_context_returns_error", func(t *testing.T) {
			t.Parallel()
			_, err := client.ListUsers(ctx, &openfgav1.ListUsersRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
				Object:               &openfgav1.Object{Type: "document", Id: "1"},
				Relation:             "viewer",
				UserFilters:          []*openfgav1.UserTypeFilter{{Type: "user"}},
			})
			require.Error(t, err)
			e, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, 2000, int(e.Code()))
		})

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				resp, err := client.ListUsers(ctx, &openfgav1.ListUsersRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					Object:               &openfgav1.Object{Type: "document", Id: "1"},
					Relation:             "viewer",
					UserFilters:          []*openfgav1.UserTypeFilter{{Type: "user"}},
					Context:              tc.context,
				})
				require.NoError(t, err)
				if tc.allowed {
					require.Len(t, resp.GetUsers(), 1)
					require.Equal(t, "user:jon", tuple.UserProtoToString(resp.GetUsers()[0]))
				} else {
					require.Empty(t, resp.GetUsers())
				}
			})
		}
	})
}

func TestConditionAnyTypeMemory(t *testing.T) {
	client := tests.BuildClientInterface(t, "memory", nil)
	t.Run("condition_any_type", func(t *testing.T) {
		runConditionAnyTests(t, client)
	})
}
