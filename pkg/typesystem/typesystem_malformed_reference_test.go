package typesystem

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
)

// TestMalformedRelationReference covers the validation-ordering defect where a RelationReference
// with a set relation_or_wildcard oneof that yields neither a wildcard nor a non-empty relation
// previously panicked the server (variant A) or was silently accepted with model-wide performance
// degradation (variant B).
//
// These shapes cannot be expressed in the DSL — parser.MustTransformDSLToProto is unusable here,
// and the protos are hand-constructed.
//
// Test A is also the ordering pin: graph.NewAuthorizationModelGraph's only error path is an
// impossible type-cast failure, so it never *returns* an error — it either succeeds or panics.
// If a future refactor moves graph construction back in front of validation, test A will panic
// again rather than return an error.
func TestMalformedRelationReference(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		name  string
		model *openfgav1.AuthorizationModel
		err   error
	}{
		{
			name: "empty_relation_single_entry_panicked_production",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "user"},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
											RelationOrWildcard: &openfgav1.RelationReference_Relation{
												Relation: "",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrEmptyRelationReference,
		},
		{
			name: "empty_relation_second_entry_was_silently_accepted",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "user"},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
										{
											Type: "user",
											RelationOrWildcard: &openfgav1.RelationReference_Relation{
												Relation: "",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrEmptyRelationReference,
		},
		{
			name: "wildcard_oneof_set_but_nil_wildcard",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "user"},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type:               "user",
											RelationOrWildcard: &openfgav1.RelationReference_Wildcard{Wildcard: nil},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrEmptyRelationReference,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewAndValidate(context.Background(), test.model)
			require.ErrorIs(t, err, test.err)
			require.ErrorContains(t, err, "empty relation reference")
		})
	}
}

// TestMalformedRelationReferenceRegressionGuards ensures valid models still validate after the
// reorder fix, and that the trap (omitting schemaVersion/modelID from newWithoutGraphs) was not
// triggered.
func TestMalformedRelationReferenceRegressionGuards(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		name  string
		model *openfgav1.AuthorizationModel
		err   error // nil for success cases
	}{
		{
			name: "valid_direct_type_reference",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "user"},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},
		{
			name: "valid_wildcard_reference",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "user"},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										WildcardRelationReference("user"),
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},
		{
			name: "valid_userset_reference",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "user"},
					{
						Type: "group",
						Relations: map[string]*openfgav1.Userset{
							"member": This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"viewer": This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},
		{
			name: "valid_ttu_with_tupleset_relation",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "user"},
					{
						Type: "folder",
						Relations: map[string]*openfgav1.Userset{
							"viewer": This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{Type: "folder"},
									},
								},
							},
						},
					},
				},
			},
			err: nil,
		},
		{
			name: "invalid_unsupported_schema_version_still_fails",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: "9.9",
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{Type: "user"},
				},
			},
			err: ErrInvalidSchemaVersion,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewAndValidate(context.Background(), test.model)
			if test.err == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, test.err)
			}
		})
	}
}

// TestNewWithValidModel verifies the refactored New() still works correctly: newWithoutGraphs +
// attachGraphs produces the same result as the original monolithic implementation. This exercises
// the happy path through both helpers, ensuring no regressions from the split.
func TestNewWithValidModel(t *testing.T) {
	t.Parallel()

	model := &openfgav1.AuthorizationModel{
		SchemaVersion: SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": This(),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
								WildcardRelationReference("user"),
							},
						},
					},
				},
			},
			{
				Type: "folder",
				Relations: map[string]*openfgav1.Userset{
					"viewer": This(),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
					},
				},
			},
		},
	}

	typesys, err := New(model)
	require.NoError(t, err)
	require.NotNil(t, typesys)
	require.Equal(t, SchemaVersion1_1, typesys.GetSchemaVersion())
	require.NotNil(t, typesys.GetWeightedGraph(), "weighted graph should be built")

	// Verify the model is queryable
	_, err = typesys.GetRelation("document", "viewer")
	require.NoError(t, err)
}

// TestMalformedRelationReferenceReadPath documents the accepted consequence: a variant-B model
// (empty relation, but not first in the list) that was previously accepted and persisted now
// fails to resolve on cache miss, causing Check/ListObjects/ListUsers to fail with ErrInvalidModel
// rather than silently running with nil weighted graph and degraded performance.
func TestMalformedRelationReferenceReadPath(t *testing.T) {
	t.Parallel()

	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockAuthorizationModelReadBackend(mockController)

	// Variant B: the malformed reference is second, so it was previously accepted at write time
	// and persisted.
	variantBModel := &openfgav1.AuthorizationModel{
		Id:            "01M092CBDNRJ6DPRDN2RX0SNH7",
		SchemaVersion: SchemaVersion1_1,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": This(),
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
								{
									Type:               "user",
									RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: ""},
								},
							},
						},
					},
				},
			},
		},
	}

	mockDatastore.EXPECT().
		ReadAuthorizationModel(gomock.Any(), "store1", "01M092CBDNRJ6DPRDN2RX0SNH7").
		Return(variantBModel, nil).
		Times(1)

	typesystemResolver, resolverStop, err := MemoizedTypesystemResolverFunc(mockDatastore, 10)
	require.NoError(t, err)
	defer resolverStop()

	_, err = typesystemResolver(context.Background(), "store1", "01M092CBDNRJ6DPRDN2RX0SNH7")
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidModel, "resolver must wrap validation errors in ErrInvalidModel")
	require.ErrorIs(t, err, ErrEmptyRelationReference, "the underlying cause must be identifiable")
}
