package typesystem

import (
	"context"
	"fmt"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/testutils"
)

type relationDetails struct {
	hasEntrypoints bool
	hasLoop        bool
}

func TestHasEntrypoints(t *testing.T) {
	tests := map[string]struct {
		model         string
		inputType     string
		inputRelation string
		expectError   string
		expectDetails *relationDetails
	}{
		`undefined_input_type`: {
			model: `
				model
					schema 1.1
				type document
					relations
						define viewer: [folder]`,
			inputType:     "unknown",
			inputRelation: "viewer",
			expectError:   "undefined type definition for 'unknown#viewer'",
		},
		`undefined_input_relation`: {
			model: `
				model
					schema 1.1
				type document
					relations
						define viewer: [folder]`,
			inputType:     "document",
			inputRelation: "unknown",
			expectError:   "undefined type definition for 'document#unknown'",
		},
		`undefined_type_in_assignable_type`: {
			model: `
				model
					schema 1.1
				type document
					relations
						define viewer: [unknown#editor]`,
			inputType:     "document",
			inputRelation: "viewer",
			expectError:   "undefined type definition for 'unknown#editor'",
		},
		`undefined_relation_in_assignable_type`: {
			model: `
				model
					schema 1.1
				type document
					relations
						define viewer: [document#unknown]`,
			inputType:     "document",
			inputRelation: "viewer",
			expectError:   "undefined type definition for 'document#unknown'",
		},
		`undefined_computed_userset`: {
			model: `
				model
					schema 1.1
				type document
					relations
						define viewer: unknown`,
			inputType:     "document",
			inputRelation: "viewer",
			expectError:   "undefined type definition for 'document#unknown'",
		},
		`undefined_tupleset`: {
			model: `
				model
					schema 1.1
				type document
					relations
						define viewer: viewer from unknown`,
			inputType:     "document",
			inputRelation: "viewer",
			expectError:   "undefined type definition for 'document#unknown'",
		},
		`undefined_computed_relation_on_tupleset_target`: {
			model: `
				model
					schema 1.1
				type user
				type folder
					relations
						define owner: [user]
				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			inputType:     "document",
			inputRelation: "viewer",
			expectDetails: &relationDetails{false, false}, // TODO this should be an error
		},
		`this_has_entrypoints_to_same_type`: {
			model: `
				model
					schema 1.1
				type document
					relations
						define viewer: [document]`,
			inputType:     "document",
			inputRelation: "viewer",
			expectDetails: &relationDetails{true, false},
		},
		`this_has_entrypoints_through_user_wildcard`: {
			model: `
				model
					schema 1.1
				type document
					relations
						define viewer: [document:*]`,
			inputType:     "document",
			inputRelation: "viewer",
			expectDetails: &relationDetails{true, false},
		},
		`this_has_entrypoints_through_userset`: {
			model: `
				model
					schema 1.1
				type user
				type org
					relations
						define member: [user]
				type folder
					relations
						define parent: [org#member]`,
			inputType:     "folder",
			inputRelation: "parent",
			expectDetails: &relationDetails{true, false},
		},
		`this_with_two_assignable_types_has_entrypoints_through_first`: {
			model: `
				model
					schema 1.1
				type user
				type folder
					relations
						define parent: [user, folder#parent]`,
			inputType:     "folder",
			inputRelation: "parent",
			expectDetails: &relationDetails{true, false},
		},
		`this_with_two_assignable_types_has_entrypoints_through_second`: {
			model: `
				model
					schema 1.1
				type user
				type folder
					relations
						define editor: [user]
						define parent: [folder#parent, folder#editor]`,
			inputType:     "folder",
			inputRelation: "parent",
			expectDetails: &relationDetails{true, false},
		},
		// TODO fix
		// `this_has_no_entrypoints_because_type_unknown_is_not_defined`: {
		//	model: `
		//	model
		//		schema 1.1
		//	type folder
		//		relations
		//			define parent: [unknown]`,
		//	inputType:     "folder",
		//	inputRelation: "parent",
		//	expectError:   "undefined type 'unknown'",
		// },
		`this_has_no_entrypoints_through_userset`: {
			model: `
				model
					schema 1.1
				type folder
					relations
						define parent: [folder#parent]`,
			inputType:     "folder",
			inputRelation: "parent",
			expectDetails: &relationDetails{false, false},
		},
		`this_has_no_entrypoints_through_recursive_userset`: {
			model: `
				model
					schema 1.1
				type group
					relations
						define member: [group#member]
				
				type folder
					relations
						define parent: [group#member]`,
			inputType:     "folder",
			inputRelation: "parent",
			expectDetails: &relationDetails{false, false},
		},
		`computed_relation_has_entrypoint_through_user`: {
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [user]
						define viewer: editor`,
			inputType:     "document",
			inputRelation: "viewer",
			expectDetails: &relationDetails{true, false},
		},
		`computed_relation_has_no_entrypoint_through_usersets`: {
			model: `
				model
					schema 1.1
				type user
				type document
					relations
						define editor: [document#viewer]
						define viewer: [document#editor]`,
			inputType:     "document",
			inputRelation: "viewer",
			expectDetails: &relationDetails{false, false},
		},
		`computed_relation_has_entrypoint_through_userset`: {
			model: `
				model
					schema 1.1
				type user
				type org
					relations
					define member: [user]
				type folder
					relations
					define a2: [org#member]
					define a1: a2`,
			inputType:     "folder",
			inputRelation: "a1",
			expectDetails: &relationDetails{true, false},
		},
		`computed_relation_has_no_entrypoints_because_no_direct_relationships`: {
			model: `
				model
					schema 1.1
				type folder
					relations
						define a2: a1
						define a1: a2`,
			inputType:     "folder",
			inputRelation: "a1",
			expectDetails: &relationDetails{false, true},
		},
		`computed_relation_has_no_entrypoints_through_ttu`: {
			model: `
				model
					schema 1.1
				type user
				
				type folder
					relations
						define parent: [document]
						define viewer: editor from parent
				
				type document
					relations
						define parent: [folder]
						define editor: viewer
						define viewer: viewer from parent`,
			inputType:     "folder",
			inputRelation: "viewer",
			expectDetails: &relationDetails{false, false}, // TODO it DOES have a cycle
		},
		`union_has_entrypoint_through_user`: {
			model: `
				model
					schema 1.1
				type user
				
				type document
					relations
						define editor: [user]
						define viewer: [document#viewer] or editor`,
			inputType:     "document",
			inputRelation: "viewer",
			expectDetails: &relationDetails{true, false},
		},
		`union_has_no_entrypoint`: {
			model: `
				model
					schema 1.1
				type user
				
				type document
					relations
						define editor: [document#viewer]
						define viewer: [document#viewer] or editor`,
			inputType:     "document",
			inputRelation: "viewer",
			expectDetails: &relationDetails{false, false},
		},
		`ttu_has_entrypoint_through_user`: {
			model: `
				model
					schema 1.1
				type user
				type org
					relations
						define viewer: [user]
				type folder
					relations
						define parent: [org]
						define viewer: viewer from parent`,
			inputType:     "folder",
			inputRelation: "viewer",
			expectDetails: &relationDetails{true, false},
		},
		`ttu_has_entrypoint_through_userset`: {
			model: `
				model
					schema 1.1
				type user
				type org
					relations
						define viewer: [user]
						define member: [user]
				type folder
					relations
						define parent: [org#member]
						define viewer: viewer from parent`,
			inputType:     "folder",
			inputRelation: "viewer",
			expectDetails: &relationDetails{true, false},
		},
		`ttu_has_no_entrypoint`: {
			model: `
				model
					schema 1.1
				type folder
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			inputType:     "folder",
			inputRelation: "viewer",
			expectDetails: &relationDetails{false, false},
		},
		`intersection_has_entrypoint_and_no_cycle`: {
			model: `
				model
					schema 1.1
				type user
				
				type document
					relations
						define action1: admin and editor
						define admin: [user]
						define editor: [user]`,
			inputType:     "document",
			inputRelation: "action1",
			expectDetails: &relationDetails{true, false},
		},
		`intersection_has_no_entrypoint_and_no_cycle`: {
			model: `
				model
					schema 1.1
				type user
				
				type document
					relations
						define action1: [document#action1] and editor
						define editor: [user]`,
			inputType:     "document",
			inputRelation: "action1",
			expectDetails: &relationDetails{false, false},
		},
		`intersection_has_no_entrypoint_and_has_cycle_2`: {
			model: `
				model
					schema 1.1
				type user
				
				type document
					relations
						define admin: [user]
						define action1: admin and action2 and action3
						define action2: admin and action1 and action3
						define action3: admin and action1 and action2`,
			inputType:     "document",
			inputRelation: "action1",
			expectDetails: &relationDetails{false, true},
		},
		`difference_has_entrypoints_and_no_cycle`: {
			model: `
				model
					schema 1.1
				type user
				
				type document
					relations
						define action1: admin but not editor
						define admin: [user]
						define editor: [user]`,
			inputType:     "document",
			inputRelation: "action1",
			expectDetails: &relationDetails{true, false},
		},
		`difference_has_entrypoints_and_no_cycle_2`: {
			model: `
				model
					schema 1.1
				type user
				
				type document
					relations
						define restricted: [user]
						define editor: [user]
						define viewer: [document#viewer] or editor
						define can_view: viewer but not restricted
						define can_view_actual: can_view`,
			inputType:     "document",
			inputRelation: "can_view_actual",
			expectDetails: &relationDetails{true, false},
		},
		`difference_has_no_entrypoint_and_no_cycle`: {
			model: `
				model
					schema 1.1
				type user
				
				type document
					relations
						define action1: [document#action1] but not editor
						define editor: [user]`,
			inputType:     "document",
			inputRelation: "action1",
			expectDetails: &relationDetails{false, false},
		},
		`difference_has_no_entrypoint_and_has_cycle`: {
			model: `
				model
					schema 1.1
				type user
				
				type document
					relations
						define admin: [user]
						define action1: admin but not action2
						define action2: admin but not action3
						define action3: admin but not action1`,
			inputType:     "document",
			inputRelation: "action1",
			expectDetails: &relationDetails{false, true},
		},
		`issue_1385`: {
			model: `
				model
					schema 1.1
			
				type user
			
				type entity
					relations
						define member : [user]
						define contextual_user: [user]
						define contextual_member : member and contextual_user
						define has_logging_product: [entity]
						define block_logging : [user] and contextual_user
						define has_access_to_logging : contextual_member from has_logging_product but not block_logging from has_logging_product
						define can_enable_logging : has_access_to_logging
			`,
			inputType:     "entity",
			inputRelation: "can_enable_logging",
			expectDetails: &relationDetails{true, false},
		},
		`issue_1260_parallel_edges_mean_entrypoints`: {
			model: `
				model
					schema 1.1
			
				type user
			
				type state
					relations
						define can_view: [user]
						define associated_transition: [transition]
						define can_transition_with: can_apply from associated_transition
			
				type transition
					relations
						define start: [state]
						define end: [state]
						define can_apply: [user] and can_view from start and can_view from end
			`,
			inputType:     "state",
			inputRelation: "can_transition_with",
			expectDetails: &relationDetails{true, false},
		},
		`ttu_has_entrypoint_through_second_tupleset`: {
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define viewer: [user]
				type folder
					relations
						define parent: [folder, group]
						define viewer: viewer from parent`,
			inputType:     "folder",
			inputRelation: "viewer",
			expectDetails: &relationDetails{true, false},
		},
		`revisited_direct_has_entrypoints`: {
			model: `
				model
					schema 1.1
			
				type user
			
				type document
					relations
						define a: [user]
						define b: a
						define c: a
						define d: b and c
			`,
			inputType:     "document",
			inputRelation: "d",
			expectDetails: &relationDetails{true, false},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			ts := New(model)
			inputRelation, _ := ts.GetRelation(test.inputType, test.inputRelation)

			rewrite := inputRelation.GetRewrite()
			hasEntrypoints, hasCycle, err := hasEntrypoints(ts.GetAllRelations(), test.inputType, test.inputRelation, rewrite, map[string]map[string]bool{})

			if test.expectError != "" {
				require.ErrorContains(t, err, test.expectError)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.expectDetails.hasEntrypoints, hasEntrypoints, "unexpected value for hasEntrypoints")
				require.Equal(t, test.expectDetails.hasLoop, hasCycle, "unexpected value for hasLoop")
			}
		})
	}
}

func TestHasCycle(t *testing.T) {
	tests := []struct {
		name       string
		model      string
		objectType string
		relation   string
		expected   bool
	}{
		{
			name: "computed_userset_1",
			model: `
				model
					schema 1.1
				type resource
					relations
						define x: y
						define y: x`,
			objectType: "resource",
			relation:   "x",
			expected:   true,
		},
		{
			name: "computed_userset_2",
			model: `
				model
					schema 1.1
				type resource
					relations
						define x: y
						define y: z
						define z: x`,
			objectType: "resource",
			relation:   "y",
			expected:   true,
		},
		{
			name: "union_1",
			model: `
				model
					schema 1.1
				type user

				type resource
					relations
						define x: [user] or y
						define y: [user] or z
						define z: [user] or x`,
			objectType: "resource",
			relation:   "z",
			expected:   true,
		},
		{
			name: "union_2",
			model: `
				model
					schema 1.1
				type user

				type resource
					relations
						define x: [user] or y
						define y: [user] or z
						define z: [user] or x`,
			objectType: "resource",
			relation:   "z",
			expected:   true,
		},
		{
			name: "intersection_and_union",
			model: `
				model
					schema 1.1
				type user
				
				type resource
					relations
						define x: [user] and y
						define y: [user] and z
						define z: [user] or x`,
			objectType: "resource",
			relation:   "x",
			expected:   true,
		},
		{
			name: "exclusion_and_union",
			model: `
				model
					schema 1.1
				type user

				type resource
					relations
						define x: [user] but not y
						define y: [user] but not z
						define z: [user] or x`,
			objectType: "resource",
			relation:   "x",
			expected:   true,
		},
		{
			name: "union_3",
			model: `
				model
					schema 1.1
				type user

				type group
					relations
						define member: [user] or memberA or memberB or memberC
						define memberA: [user] or member or memberB or memberC
						define memberB: [user] or member or memberA or memberC
						define memberC: [user] or member or memberA or memberB`,
			objectType: "group",
			relation:   "member",
			expected:   true,
		},
		{
			name: "union_4",
			model: `
				model
					schema 1.1
				type user

				type account
					relations
						define admin: [user] or member or super_admin or owner
						define member: [user] or owner or admin or super_admin
						define super_admin: [user] or admin or member or owner
						define owner: [user]`,
			objectType: "account",
			relation:   "member",
			expected:   true,
		},
		{
			name: "union_5",
			model: `
				model
					schema 1.1
				type user

				type account
					relations
						define admin: [user] or member or super_admin or owner
						define member: [user] or owner or admin or super_admin
						define super_admin: [user] or admin or member or owner
						define owner: [user]`,
			objectType: "account",
			relation:   "owner",
			expected:   false,
		},
		{
			name: "union_6",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define editor: [user]
						define viewer: [document#viewer] or editor`,
			objectType: "document",
			relation:   "viewer",
			expected:   false,
		},
		{
			name: "many_circular_computed_relations",
			model: `
				model
					schema 1.1
				type user
				
				type canvas
					relations
						define can_edit: editor or owner
						define editor: [user, account#member]
						define owner: [user]
						define viewer: [user, account#member]
				
				type account
					relations
						define admin: [user] or member or super_admin or owner
						define member: [user] or owner or admin or super_admin
						define owner: [user]
						define super_admin: [user] or admin or member`,
			objectType: "account",
			relation:   "admin",
			expected:   true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)

			typesys := New(model)

			hasCycle, err := typesys.HasCycle(test.objectType, test.relation)
			require.Equal(t, test.expected, hasCycle)
			require.NoError(t, err)
		})
	}
}

func TestNewAndValidate(t *testing.T) {
	tests := []struct {
		name          string
		model         string
		expectedError error
	}{
		{
			// TODO remove - same as this_has_entrypoints_through_user
			name: "direct_relationship_with_entrypoint",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user]`,
		},
		{
			// TODO remove - same as computed_relation_has_entrypoint_through_user
			name: "computed_relationship_with_entrypoint",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define editor: [user]
						define viewer: editor`,
		},
		{
			// TODO remove - same as intersection_has_no_entrypoint_and_has_cycle_2
			name: "no_entrypoint_1",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define admin: [user]
						define action1: admin and action2 and action3
						define action2: admin and action1 and action3
						define action3: admin and action1 and action2`,
			expectedError: ErrNoEntryPointsLoop,
		},
		{
			// TODO remove - same as difference_has_no_entrypoint_and_has_cycle
			name: "no_entrypoint_2",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define admin: [user]
						define action1: admin but not action2
						define action2: admin but not action3
						define action3: admin but not action1`,
			expectedError: ErrNoEntryPointsLoop,
		},
		{
			// TODO remove - same as intersection_has_no_entrypoint_and_no_cycle
			name: "no_entrypoint_3a",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [document#viewer] and editor
						define editor: [user]`,
			expectedError: ErrNoEntrypoints,
		},
		{
			// TODO remove - same as difference_has_no_entrypoint_and_no_cycle
			name: "no_entrypoint_3b",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [document#viewer] but not editor
						define editor: [user]`,
			expectedError: ErrNoEntrypoints,
		},
		{
			// TODO this test is invalid - "editor from parent" is invalid - "folder#editor" is not defined
			// Replaced by computed_relation_has_no_entrypoints
			name: "no_entrypoint_4",
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define parent: [document]
						define viewer: editor from parent

				type document
					relations
						define parent: [folder]
						define editor: viewer
						define viewer: editor from parent`,
			expectedError: ErrNoEntrypoints,
		},
		{
			// TODO remove - same as difference_has_entrypoints_and_no_cycle_2
			name: "self_referencing_type_restriction_with_entrypoint_1",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define restricted: [user]
						define editor: [user]
						define viewer: [document#viewer] or editor
						define can_view: viewer but not restricted
						define can_view_actual: can_view`,
		},
		{
			// TODO remove - same as union_has_entrypoint_through_user
			name: "self_referencing_type_restriction_with_entrypoint_2",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define editor: [user]
						define viewer: [document#viewer] or editor`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			_, err := NewAndValidate(context.Background(), model)
			require.ErrorIs(t, err, test.expectedError)
		})
	}
}

func TestSuccessfulRewriteValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgav1.AuthorizationModel
	}{
		{
			name: "empty_relations",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
					},
				},
			},
		},
		{
			name: "zero_length_relations_is_valid",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type:      "repo",
						Relations: map[string]*openfgav1.Userset{},
					},
				},
			},
		},
		{
			name: "self_referencing_type_restriction_with_entrypoint",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define editor: [user]
							define viewer: [document#viewer] or editor`).GetTypeDefinitions(),
				SchemaVersion: SchemaVersion1_1,
			},
		},
		{
			name: "intersection_may_contain_repeated_relations",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type document
						relations
							define editor: [user]
							define viewer: editor and editor`).GetTypeDefinitions(),
				SchemaVersion: SchemaVersion1_1,
			},
		},
		{
			name: "exclusion_may_contain_repeated_relations",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user
					type document
						relations
							define editor: [user]
							define viewer: editor but not editor`).GetTypeDefinitions(),
				SchemaVersion: SchemaVersion1_1,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAndValidate(context.Background(), test.model)
			require.NoError(t, err)
		})
	}
}

func TestInvalidRewriteValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgav1.AuthorizationModel
		err   error
	}{
		{
			name: "empty_rewrites",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {},
						},
					},
				},
			},
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "duplicate_types_is_invalid",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type:      "repo",
						Relations: map[string]*openfgav1.Userset{},
					},
					{
						Type:      "repo",
						Relations: map[string]*openfgav1.Userset{},
					},
				},
			},
			err: ErrDuplicateTypes,
		},
		{
			name: "invalid_relation:_self_reference_in_computedUserset",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_ComputedUserset{
									ComputedUserset: &openfgav1.ObjectRelation{Relation: "reader"},
								},
							},
						},
					},
				},
			},
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_self_reference_in_union",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_Union{
									Union: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_This{},
											},
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{Relation: "reader"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_self_reference_in_intersection",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_Intersection{
									Intersection: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_This{},
											},
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{Relation: "reader"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_self_reference_in_difference_base",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_Difference{
									Difference: &openfgav1.Difference{
										Base: &openfgav1.Userset{
											Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{Relation: "reader"},
											},
										},
										Subtract: &openfgav1.Userset{
											Userset: &openfgav1.Userset_This{},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_self_reference_in_difference_subtract",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_Difference{
									Difference: &openfgav1.Difference{
										Base: &openfgav1.Userset{
											Userset: &openfgav1.Userset_This{},
										},
										Subtract: &openfgav1.Userset{
											Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{Relation: "reader"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrInvalidUsersetRewrite,
		},
		{
			name: "invalid_relation:_computedUserset_to_relation_which_does_not_exist",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_ComputedUserset{
									ComputedUserset: &openfgav1.ObjectRelation{Relation: "writer"},
								},
							},
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_computedUserset_in_a_union",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_Union{
									Union: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_This{},
											},
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{Relation: "writer"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_computedUserset_in_a_intersection",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_Intersection{
									Intersection: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_This{},
											},
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{Relation: "writer"},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_computedUserset_in_a_difference_base",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_Difference{
									Difference: &openfgav1.Difference{
										Base: &openfgav1.Userset{
											Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{Relation: "writer"},
											},
										},
										Subtract: &openfgav1.Userset{
											Userset: &openfgav1.Userset_This{},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_computedUserset_in_a_difference_subtract",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_Difference{
									Difference: &openfgav1.Difference{
										Base: &openfgav1.Userset{
											Userset: &openfgav1.Userset_This{},
										},
										Subtract: &openfgav1.Userset{
											Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{Relation: "writer"},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_tupleToUserset_where_tupleset_is_not_valid",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "group",
						Relations: map[string]*openfgav1.Userset{
							"member": {
								Userset: &openfgav1.Userset_This{},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_Union{
									Union: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_This{},
											},
											{
												Userset: &openfgav1.Userset_TupleToUserset{
													TupleToUserset: &openfgav1.TupleToUserset{
														Tupleset: &openfgav1.ObjectRelation{
															Relation: "notavalidrelation",
														},
														ComputedUserset: &openfgav1.ObjectRelation{
															Relation: "member",
														},
													},
												},
											},
										},
									},
								},
							},
							"writer": {
								Userset: &openfgav1.Userset_This{},
							},
						},
					},
				},
			},
			err: ErrRelationUndefined,
		},
		{
			name: "invalid_relation:_tupleToUserset_where_computed_userset_is_not_valid",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type user

					type document
						relations
							define reader: notavalidrelation from writer
							define writer: [user]`).GetTypeDefinitions(),
			},
			err: ErrRelationUndefined,
		},
		{
			name: "Fails_If_Using_This_As_Relation_Name",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"this": This(),
						},
					},
				},
			},
			err: ErrReservedKeywords,
		},
		{
			name: "Fails_If_Using_Self_As_Relation_Name",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "repo",
						Relations: map[string]*openfgav1.Userset{
							"self": This(),
						},
					},
				},
			},
			err: ErrReservedKeywords,
		},
		{
			name: "Fails_If_Using_This_As_Type_Name",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "this",
						Relations: map[string]*openfgav1.Userset{
							"viewer": This(),
						},
					},
				},
			},
			err: ErrReservedKeywords,
		},
		{
			name: "Fails_If_Using_Self_As_Type_Name",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "self",
						Relations: map[string]*openfgav1.Userset{
							"viewer": This(),
						},
					},
				},
			},
			err: ErrReservedKeywords,
		},
		{
			name: "Fails_If_Auth_Model_1.1_Has_A_Cycle_And_Only_One_Type",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: parser.MustTransformDSLToProto(`
					model
						schema 1.1
					type folder
						relations
							define parent: [folder]
							define viewer: viewer from parent`).GetTypeDefinitions(),
			},
			err: ErrNoEntrypoints,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAndValidate(context.Background(), test.model)
			require.ErrorIs(t, err, test.err)
		})
	}
}
func TestSuccessfulRelationTypeRestrictionsValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgav1.AuthorizationModel
	}{
		{
			name: "succeeds_on_a_valid_typeSystem_with_an_objectType_type",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "succeeds_on_a_valid_typeSystem_with_a_type_and_type#relation_type",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgav1.Userset{
							"admin":  {Userset: &openfgav1.Userset_This{}},
							"member": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"admin": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"member": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
							"writer": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										DirectRelationReference("user", ""),
										DirectRelationReference("group", "member"),
									},
								},
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										DirectRelationReference("user", ""),
										DirectRelationReference("group", "admin"),
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAndValidate(context.Background(), test.model)
			require.NoError(t, err)
		})
	}
}

func TestInvalidRelationTypeRestrictionsValidations(t *testing.T) {
	var tests = []struct {
		name  string
		model *openfgav1.AuthorizationModel
		err   error
	}{
		{
			name: "relational_type_which_does_not_exist",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "group",
										},
									},
								},
							},
						},
					},
				},
			},
			err: InvalidRelationTypeError("document", "reader", "group", ""),
		},
		{
			name: "relation_type_of_form_type#relation_where_relation_doesn't_exist",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "group",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {Userset: &openfgav1.Userset_This{}},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type:               "group",
											RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "admin"},
										},
									},
								},
							},
						},
					},
				},
			},
			err: InvalidRelationTypeError("document", "reader", "group", "admin"),
		},
		{
			name: "assignable_relation_with_no_type:_this",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"reader": {
								Userset: &openfgav1.Userset_This{},
							},
						},
					},
				},
			},
			err: AssignableRelationError("document", "reader"),
		},
		{
			name: "assignable_relation_with_no_type:_union",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"writer": {
								Userset: &openfgav1.Userset_This{},
							},
							"reader": {
								Userset: &openfgav1.Userset_Union{
									Union: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_This{},
											},
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
														Relation: "writer",
													},
												},
											},
										},
									},
								},
							},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			err: AssignableRelationError("document", "reader"),
		},
		{
			name: "assignable_relation_wit_no_type:_intersection",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"writer": {
								Userset: &openfgav1.Userset_This{},
							},
							"reader": {
								Userset: &openfgav1.Userset_Intersection{
									Intersection: &openfgav1.Usersets{
										Child: []*openfgav1.Userset{
											{
												Userset: &openfgav1.Userset_This{},
											},
											{
												Userset: &openfgav1.Userset_ComputedUserset{
													ComputedUserset: &openfgav1.ObjectRelation{
														Relation: "writer",
													},
												},
											},
										},
									},
								},
							},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			err: AssignableRelationError("document", "reader"),
		},
		{
			name: "assignable_relation_with_no_type:_difference base",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"writer": {
								Userset: &openfgav1.Userset_This{},
							},
							"reader": {
								Userset: &openfgav1.Userset_Difference{
									Difference: &openfgav1.Difference{
										Base: &openfgav1.Userset{
											Userset: &openfgav1.Userset_This{},
										},
										Subtract: &openfgav1.Userset{
											Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{
													Relation: "writer",
												},
											},
										},
									},
								},
							},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			err: AssignableRelationError("document", "reader"),
		},
		{
			name: "assignable_relation_with_no_type:_difference_subtract",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"writer": {
								Userset: &openfgav1.Userset_This{},
							},
							"reader": {
								Userset: &openfgav1.Userset_Difference{
									Difference: &openfgav1.Difference{
										Base: &openfgav1.Userset{
											Userset: &openfgav1.Userset_ComputedUserset{
												ComputedUserset: &openfgav1.ObjectRelation{
													Relation: "writer",
												},
											},
										},
										Subtract: &openfgav1.Userset{
											Userset: &openfgav1.Userset_This{},
										},
									},
								},
							},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			err: AssignableRelationError("document", "reader"),
		},
		{
			name: "non-assignable_relation_with_a_type",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"writer": {
								Userset: &openfgav1.Userset_This{},
							},
							"reader": {
								Userset: &openfgav1.Userset_ComputedUserset{
									ComputedUserset: &openfgav1.ObjectRelation{Relation: "writer"},
								},
							},
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"writer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
								"reader": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
				},
			},
			err: NonAssignableRelationError("document", "reader"),
		},
		{
			name: "userset_specified_as_allowed_type_but_the_relation_is_used_in_a_TTU_rewrite",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgav1.Userset{
							"member": This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent":   This(),
							"can_view": TupleToUserset("parent", "member"),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										DirectRelationReference("folder", "member"),
									},
								},
							},
						},
					},
				},
			},
			err: InvalidRelationTypeError("document", "parent", "folder", "member"),
		},
		{
			name: "userset_specified_as_allowed_type_but_the_relation_is_used_in_a_TTU_rewrite_included_in_a_union",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": This(),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "folder",
										},
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										{
											Type: "user",
										},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": Union(TupleToUserset("parent", "viewer"), This()),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										DirectRelationReference("folder", "parent"),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										DirectRelationReference("user", ""),
										DirectRelationReference("folder", "parent"),
									},
								},
							},
						},
					},
				},
			},
			err: InvalidRelationTypeError("document", "parent", "folder", "parent"),
		},
		{
			name: "WildcardNotAllowedInTheTuplesetPartOfTTU",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
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
										DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": Union(This(), TupleToUserset("parent", "viewer")),
						},
						Metadata: &openfgav1.Metadata{
							Relations: map[string]*openfgav1.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										WildcardRelationReference("folder"),
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
										DirectRelationReference("user", ""),
									},
								},
							},
						},
					},
				},
			},
			err: InvalidRelationTypeError("document", "parent", "folder", ""),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAndValidate(context.Background(), test.model)
			require.EqualError(t, err, test.err.Error())
		})
	}
}

func TestRelationInvolvesIntersection(t *testing.T) {
	tests := []struct {
		name        string
		model       string
		rr          *openfgav1.RelationReference
		expected    bool
		expectedErr error
	}{
		{
			name: "indirect_computeduserset_through_ttu_containing_intersection",
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define manage: [user]
						define editor: [user] and manage

				type document
					relations
						define parent: [folder]
						define editor: editor from parent
						define viewer: editor`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "ttu_relations_containing_intersection",
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define editor: [user]
						define viewer: [user] and editor

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "indirect_relations_containing_intersection",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define editor: [user]
						define viewer: [user] and editor`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "undefined_type",
			model: `
				model
					schema 1.1
				type user`,
			rr:          DirectRelationReference("document", "viewer"),
			expected:    false,
			expectedErr: ErrObjectTypeUndefined,
		},
		{
			name: "undefined_relation",
			model: `
				model
					schema 1.1
				type user`,
			rr:          DirectRelationReference("user", "viewer"),
			expected:    false,
			expectedErr: ErrRelationUndefined,
		},
		{
			name: "non-assignable_indirect_type_restriction_involving_intersection",
			model: `
				model
					schema 1.1
				type user

				type org
					relations
						define allowed: [user]
						define dept: [group]
						define dept_member: member from dept
						define dept_allowed_member: dept_member and allowed

				type resource
					relations
						define reader: [user] or writer
						define writer: [org#dept_allowed_member]`,
			rr:       DirectRelationReference("resource", "reader"),
			expected: true,
		},
		{
			name: "indirect_relationship_through_type_restriction",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define allowed: [user]
						define editor: [user] and allowed
						define viewer: [document#editor]`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "github_model",
			model: `
				model
					schema 1.1
				type user

				type organization
					relations
						define member: [user] or owner
						define owner: [user]
						define repo_admin: [user, organization#member]
						define repo_reader: [user, organization#member]
						define repo_writer: [user, organization#member]

				type team
					relations
						define member: [user, team#member]

				type repo
					relations
						define admin: [user, team#member] or repo_admin from owner
						define maintainer: [user, team#member] or admin
						define owner: [organization]
						define reader: [user, team#member] or triager or repo_reader from owner
						define triager: [user, team#member] or writer
						define writer: [user, team#member] or maintainer or repo_writer from owner`,
			rr:       DirectRelationReference("repo", "admin"),
			expected: false,
		},
		{
			name: "github_model",
			model: `
				model
					schema 1.1
				type user

				type organization
					relations
						define member: [user] or owner
						define owner: [user]
						define repo_admin: [user, organization#member]
						define repo_reader: [user, organization#member]
						define repo_writer: [user, organization#member]

				type team
					relations
						define member: [user, team#member]

				type repo
					relations
						define admin: [user, team#member] or repo_admin from owner
						define maintainer: [user, team#member] or admin
						define owner: [organization]
						define reader: [user, team#member] or triager or repo_reader from owner
						define triager: [user, team#member] or writer
						define writer: [user, team#member] or maintainer or repo_writer from owner`,
			rr:       DirectRelationReference("repo", "admin"),
			expected: false,
		},
		{
			name: "direct_relations_related_to_each_other",
			model: `
				model
					schema 1.1
				type user

				type example
					relations
						define editor: [example#viewer]
						define viewer: [example#editor]`,
			rr:       DirectRelationReference("example", "editor"),
			expected: false,
		},
		{
			name: "cyclical_evaluation_of_tupleset",
			model: `
				model
					schema 1.1
				type user

				type node
					relations
						define parent: [node]
						define editor: [user] or editor from parent`,
			rr:       DirectRelationReference("node", "editor"),
			expected: false,
		},
		{
			name: "nested_intersection_1",
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define allowed: [user]
						define viewer: [user] and allowed

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)

			typesys := New(model)

			objectType := test.rr.GetType()
			relationStr := test.rr.GetRelation()

			actual, err := typesys.RelationInvolvesIntersection(objectType, relationStr)
			require.ErrorIs(t, err, test.expectedErr)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestRelationInvolvesExclusion(t *testing.T) {
	tests := []struct {
		name        string
		model       string
		rr          *openfgav1.RelationReference
		expected    bool
		expectedErr error
	}{
		{
			name: "indirect_computed_userset_through_ttu_containing_exclusion",
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define restricted: [user]
						define editor: [user] but not restricted

				type document
					relations
						define parent: [folder]
						define editor: editor from parent
						define viewer: editor`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "ttu_relations_containing_exclusion",
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define restricted: [user]
						define viewer: [user] but not restricted

				type document
					relations
						define parent: [folder]
						define viewer: viewer from parent`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "indirect_relations_containing_exclusion",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define restricted: [user]
						define editor: [user] but not restricted
						define viewer: editor`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "undefined_type",
			model: `
				model
					schema 1.1
				type user`,
			rr:          DirectRelationReference("document", "viewer"),
			expected:    false,
			expectedErr: ErrObjectTypeUndefined,
		},
		{
			name: "undefined_relation",
			model: `
				model
					schema 1.1
				type user`,
			rr:          DirectRelationReference("user", "viewer"),
			expected:    false,
			expectedErr: ErrRelationUndefined,
		},
		{
			name: "non-assignable_indirect_type_restriction_involving_exclusion",
			model: `
				model
					schema 1.1
				type user

				type org
					relations
						define removed: [user]
						define dept: [group]
						define dept_member: member from dept
						define dept_allowed_member: dept_member but not removed

				type resource
					relations
						define reader: [user] or writer
						define writer: [org#dept_allowed_member]`,
			rr:       DirectRelationReference("resource", "reader"),
			expected: true,
		},
		{
			name: "indirect_relationship_through_type_restriction",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define restricted: [user]
						define editor: [user] but not restricted
						define viewer: [document#editor]`,
			rr:       DirectRelationReference("document", "viewer"),
			expected: true,
		},
		{
			name: "direct_relations_related_to_each_other",
			model: `
				model
					schema 1.1
				type user

				type example
					relations
						define editor: [example#viewer]
						define viewer: [example#editor]`,
			rr:       DirectRelationReference("example", "editor"),
			expected: false,
		},
		{
			name: "cyclical_evaluation_of_tupleset",
			model: `
				model
					schema 1.1
				type user

				type node
					relations
						define parent: [node]
						define editor: [user] or editor from parent`,
			rr:       DirectRelationReference("node", "editor"),
			expected: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)

			typesys := New(model)

			objectType := test.rr.GetType()
			relationStr := test.rr.GetRelation()

			actual, err := typesys.RelationInvolvesExclusion(objectType, relationStr)
			require.ErrorIs(t, err, test.expectedErr)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestIsTuplesetRelation(t *testing.T) {
	tests := []struct {
		name          string
		model         *openfgav1.AuthorizationModel
		objectType    string
		relation      string
		expected      bool
		expectedError error
	}{
		{
			name:          "undefined_object_type_returns_error",
			objectType:    "document",
			relation:      "viewer",
			expected:      false,
			expectedError: ErrObjectTypeUndefined,
		},
		{
			name: "undefined_relation_returns_error",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
					},
				},
			},
			objectType:    "document",
			relation:      "viewer",
			expected:      false,
			expectedError: ErrRelationUndefined,
		},
		{
			name: "direct_tupleset_relation",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_union",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": Union(
								This(),
								TupleToUserset("parent", "viewer"),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_intersection",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": Intersection(
								This(),
								TupleToUserset("parent", "viewer"),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_exclusion",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": Difference(
								This(),
								TupleToUserset("parent", "viewer"),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_nested_union",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": Intersection(
								This(),
								Union(TupleToUserset("parent", "viewer")),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_nested_intersection",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": Union(
								This(),
								Intersection(TupleToUserset("parent", "viewer")),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "tupleset_relation_under_nested_exclusion",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": Union(
								This(),
								Difference(This(), TupleToUserset("parent", "viewer")),
							),
						},
					},
				},
			},
			objectType: "document",
			relation:   "parent",
			expected:   true,
		},
		{
			name: "not_a_tupleset_relation",
			model: &openfgav1.AuthorizationModel{
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "document",
						Relations: map[string]*openfgav1.Userset{
							"parent": This(),
							"viewer": TupleToUserset("parent", "viewer"),
						},
					},
				},
			},
			objectType: "document",
			relation:   "viewer",
			expected:   false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			typesys := New(test.model)

			actual, err := typesys.IsTuplesetRelation(test.objectType, test.relation)
			require.ErrorIs(t, err, test.expectedError)
			require.Equal(t, test.expected, actual)
		})
	}
}

func TestIsDirectlyRelated(t *testing.T) {
	tests := []struct {
		name   string
		model  string
		target *openfgav1.RelationReference
		source *openfgav1.RelationReference
		result bool
	}{
		{
			name: "wildcard_and_wildcard",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user:*]`,
			target: DirectRelationReference("document", "viewer"),
			source: WildcardRelationReference("user"),
			result: true,
		},
		{
			name: "wildcard_and_direct",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user:*]`,
			target: DirectRelationReference("document", "viewer"),
			source: DirectRelationReference("user", ""),
			result: false,
		},
		{
			name: "direct_and_wildcard",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user]`,
			target: DirectRelationReference("document", "viewer"),
			source: WildcardRelationReference("user"),
			result: false,
		},
		{
			name: "direct_type",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user]`,
			target: DirectRelationReference("document", "viewer"),
			source: DirectRelationReference("user", ""),
			result: true,
		},
		{
			name: "relation_not_related",
			model: `
				model
					schema 1.1
				type user
					relations
						define manager: [user]

				type document
					relations
						define viewer: [user]`,
			target: DirectRelationReference("document", "viewer"),
			source: DirectRelationReference("user", "manager"),
			result: false,
		},
		{
			name: "direct_and_userset",
			model: `
				model
					schema 1.1
				type group
					relations
						define member: [group#member]

				type document
					relations
						define viewer: [group#member]`,
			target: DirectRelationReference("document", "viewer"),
			source: DirectRelationReference("group", "member"),
			result: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			typesys := New(model)

			ok, err := typesys.IsDirectlyRelated(test.target, test.source)
			require.NoError(t, err)
			require.Equal(t, test.result, ok)
		})
	}
}

func TestIsPubliclyAssignable(t *testing.T) {
	tests := []struct {
		name       string
		model      string
		target     *openfgav1.RelationReference
		objectType string
		result     bool
	}{
		{
			name: "1",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user:*]`,
			target:     DirectRelationReference("document", "viewer"),
			objectType: "user",
			result:     true,
		},
		{
			name: "2",
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user]`,
			target:     DirectRelationReference("document", "viewer"),
			objectType: "user",
			result:     false,
		},
		{
			name: "3",
			model: `
				model
					schema 1.1
				type user
				type employee

				type document
					relations
						define viewer: [employee:*]`,
			target:     DirectRelationReference("document", "viewer"),
			objectType: "user",
			result:     false,
		},
		{
			name: "4",
			model: `
				model
					schema 1.1
				type user

				type group
					relations
						define member: [user:*]

				type document
					relations
						define viewer: [group#member]`,
			target:     DirectRelationReference("document", "viewer"),
			objectType: "user",
			result:     false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			typesys := New(model)

			ok, err := typesys.IsPubliclyAssignable(test.target, test.objectType)
			require.NoError(t, err)
			require.Equal(t, ok, test.result)
		})
	}
}

func TestGetRelationReferenceAsString(t *testing.T) {
	require.Equal(t, "", GetRelationReferenceAsString(nil))
	require.Equal(t, "team#member", GetRelationReferenceAsString(DirectRelationReference("team", "member")))
	require.Equal(t, "team:*", GetRelationReferenceAsString(WildcardRelationReference("team")))
}

func TestDirectlyRelatedUsersets(t *testing.T) {
	tests := []struct {
		name       string
		model      string
		objectType string
		relation   string
		expected   []*openfgav1.RelationReference
	}{
		{
			name: "only_direct_relation",
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define allowed: [user]`,
			objectType: "folder",
			relation:   "allowed",
			expected:   nil,
		},
		{
			name: "with_public_relation",
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define allowed: [user, user:*]`,
			objectType: "folder",
			relation:   "allowed",
			expected: []*openfgav1.RelationReference{
				WildcardRelationReference("user"),
			},
		},
		{
			name: "with_ttu_relation",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]

				type folder
					relations
						define allowed: [group#member]`,
			objectType: "folder",
			relation:   "allowed",
			expected: []*openfgav1.RelationReference{
				DirectRelationReference("group", "member"),
			},
		},
		{
			name: "mix_direct_and_public_relation",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]

				type folder
					relations
						define allowed: [group#member, user]`,
			objectType: "folder",
			relation:   "allowed",
			expected: []*openfgav1.RelationReference{
				DirectRelationReference("group", "member"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)

			typesys := New(model)
			result, err := typesys.DirectlyRelatedUsersets(test.objectType, test.relation)
			require.NoError(t, err)
			require.Equal(t, test.expected, result)
		})
	}
}

func TestResolvesExclusivelyToDirectlyAssignable(t *testing.T) {
	tests := []struct {
		name               string
		model              string
		relationReferences []*openfgav1.RelationReference
		expected           bool
	}{
		{
			name: "simple_userset",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type folder
					relations
						define allowed: [group#member]`,
			relationReferences: []*openfgav1.RelationReference{
				DirectRelationReference("group", "member"),
			},
			expected: true,
		},
		{
			name: "complex_userset_exclusion",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define exclude: [user]
						define member: [user]
						define complexMember: [user] but not exclude
				type folder
					relations
						define allowed: [group#complexMember]`,
			relationReferences: []*openfgav1.RelationReference{
				DirectRelationReference("group", "complexMember"),
			},
			expected: false,
		},
		{
			name: "complex_userset_intersection",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define owner: [user]
						define member: [user]
						define complexMember: [user] or owner
				type folder
					relations
						define allowed: [group#complexMember]`,
			relationReferences: []*openfgav1.RelationReference{
				DirectRelationReference("group", "complexMember"),
			},
			expected: false,
		},
		{
			name: "complex_userset_union",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define allowed: [user]
						define member: [user]
						define complexMember: [user] and allowed
				type folder
					relations
						define allowed: [group#complexMember]`,
			relationReferences: []*openfgav1.RelationReference{
				DirectRelationReference("group", "complexMember"),
			},
			expected: false,
		},
		{
			name: "multiple_assignment_userset",
			model: `
				model
					schema 1.1
				type user1
				type user2
				type group
					relations
						define member: [user1, user2]
				type folder
					relations
						define allowed: [group#member]`,
			relationReferences: []*openfgav1.RelationReference{
				DirectRelationReference("group", "member"),
			},
			expected: true,
		},
		{
			name: "computed_userset",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
						define viewable_member: member
				type folder
					relations
						define allowed: [group#viewable_member]`,
			relationReferences: []*openfgav1.RelationReference{
				DirectRelationReference("group", "viewable_member"),
			},
			// TODO: once we are able to handle the computed userset, we will change this to true.
			expected: false,
		},
		{
			name: "public_assignable",
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]

				type folder
					relations
						define allowed: [user, user:*]`,
			relationReferences: []*openfgav1.RelationReference{
				WildcardRelationReference("user"),
			},
			expected: false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			typeSystem := New(model)
			result, err := typeSystem.ResolvesExclusivelyToDirectlyAssignable(test.relationReferences)
			require.NoError(t, err)
			require.Equal(t, test.expected, result)
		})
	}
}

func TestConditions(t *testing.T) {
	tests := []struct {
		name          string
		model         *openfgav1.AuthorizationModel
		expectedError error
	}{
		{
			name: "condition_fails_undefined",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
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
										ConditionedRelationReference(WildcardRelationReference("user"), "invalid_condition_name"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("condition invalid_condition_name is undefined for relation viewer"),
		},
		{
			name: "condition_fails_key_condition_name_mismatch",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
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
										ConditionedRelationReference(WildcardRelationReference("user"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
					"condition2": {
						Name:       "condition3",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
			expectedError: fmt.Errorf("condition key 'condition2' does not match condition name 'condition3'"),
		},
		{
			name: "condition_valid",
			model: &openfgav1.AuthorizationModel{
				SchemaVersion: SchemaVersion1_1,
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "user",
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
										ConditionedRelationReference(WildcardRelationReference("user"), "condition1"),
									},
								},
							},
						},
					},
				},
				Conditions: map[string]*openfgav1.Condition{
					"condition1": {
						Name:       "condition1",
						Expression: "param1 == 'ok'",
						Parameters: map[string]*openfgav1.ConditionParamTypeRef{
							"param1": {
								TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_STRING,
							},
						},
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewAndValidate(context.Background(), test.model)
			if test.expectedError != nil {
				require.Error(t, err)
				require.EqualError(t, err, test.expectedError.Error())
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestHasTypeInfo(t *testing.T) {
	tests := []struct {
		name       string
		model      string
		objectType string
		relation   string
		expected   bool
	}{
		{
			name: "has_type_info_true",
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define allowed: [user]`,
			objectType: "folder",
			relation:   "allowed",
			expected:   true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(test.model)
			typesys := New(model)
			result, err := typesys.HasTypeInfo(test.objectType, test.relation)
			require.NoError(t, err)
			require.Equal(t, test.expected, result)
		})
	}
}

func BenchmarkNewAndValidate(b *testing.B) {
	model := testutils.MustTransformDSLToProtoWithID(`
		model
			schema 1.1
		type user
		type folder
			relations
				define parent: [folder]
				define owner: [group]
				define folder_reader: [user, group#member] or folder_reader from owner or folder_reader from parent
				define blocked: [user, user:*, group#member] or nblocked from parent
				define unblocked: [user, group#member]
				define nblocked: blocked but not unblocked
				define allowed: [user, user:*, group#member] or allowed from parent
				define super_allowed: [user, group#member] or super_allowed from parent
				define reader: folder_reader and allowed and super_allowed
				define can_read: reader but not nblocked
		type group
			relations
				define parent: [group]
				define allowed: [user, group#member] or allowed from parent
				define super_allowed: [user, group#super_allowed]
				define blocked: [user, group#member] or blocked from parent
				define og_member: [user] or member from parent
				define allowed_member: og_member and allowed and super_allowed
				define member: allowed_member but not blocked
				define folder_reader: [group#member] or folder_reader from parent`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewAndValidate(context.Background(), model)
		require.NoError(b, err)
	}
}
