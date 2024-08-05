package typesystem

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/pkg/testutils"
)

func TestTypesystemConnectedTypesAssignment(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := []struct {
		model                  string
		expectedConnectedTypes TypesystemConnectedTypes
	}{
		{
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define viewer: [user]
			`,
			expectedConnectedTypes: TypesystemConnectedTypes{
				"document": map[string]map[string][]string{
					"viewer": {
						"user": {"viewer"},
					},
				},
			},
		},
		{
			model: `
				model
					schema 1.1
				type user

				type document
					relations
						define only_with_condition: [user with cond1]
						define with_and_without_condition: [user, user with cond1, user with cond2]
						define multiple_conditions: [user with cond1, user with cond2, user with cond3]
				
				condition cond1(x: int) {
					x < 1
				}

				condition cond2(x: int) {
					x < 2
				}

				condition cond3(x: int) {
					x < 3
				}
			`,
			expectedConnectedTypes: TypesystemConnectedTypes{
				"document": map[string]map[string][]string{
					"only_with_condition": {
						"user": {"only_with_condition"},
					},
					"with_and_without_condition": {
						"user": {"with_and_without_condition"},
					},
					"multiple_conditions": {
						"user": {"multiple_conditions"},
					},
				},
			},
		},
		{
			model: `
				model
					schema 1.1
				type user

				type folder
					relations
						define viewer: editor
						define editor: owner
						define owner: [user]

				type document
					relations
						define parent: [folder]
						define can_view: viewer from parent
						define can_edit: editor from parent
						define owner: owner from parent
			`,
			expectedConnectedTypes: TypesystemConnectedTypes{
				"document": map[string]map[string][]string{
					"parent": {
						"folder": {"parent"},
					},
					"can_view": {
						"user": {"owner"},
					},
					"can_edit": {
						"user": {"owner"},
					},
					"owner": {
						"user": {"owner"},
					},
				},
				"folder": map[string]map[string][]string{
					"viewer": {
						"user": {"owner"},
					},
					"editor": {
						"user": {"owner"},
					},
					"owner": {
						"user": {"owner"},
					},
				},
			},
		},
		{
			model: `
				model
					schema 1.1
				type user
				type employee

				type group
					relations
						define admin: [user]
						define owner: [user,employee]

				type folder
					relations
						define viewer: creator
						define editor: [user] or creator
						define collaborator: viewer and editor
						define collaborator_computed: collaborator
						define restricted: viewer but not owner
						define restricted_computed: restricted
						define creator: [user]
						define owner: [group]
						define admin: admin from owner

				type document
					relations
						define parent: [folder]
						define can_view: viewer from parent
						define can_edit: editor from parent
						define public: [user:*]
						define can_share: [user, document#can_share]
			`,
			expectedConnectedTypes: TypesystemConnectedTypes{
				"document": map[string]map[string][]string{
					"can_view": {
						"user": {"creator"},
					},
					// "can_edit" directs to union, not supported yet
					"parent": {
						"folder": {"parent"},
					},
					"public": {
						"user:*": {"public"},
					},
					// "can_share" contains userset, not supported yet
				},
				"folder": map[string]map[string][]string{
					"viewer": {
						"user": {"creator"},
					},
					// "editor" contains union, not supported yet
					// "collaborator" contains intersection, not supported yet
					"creator": {
						"user": {"creator"},
					},
					"owner": {
						"group": {"owner"},
					},
					"admin": {
						"user": {"admin"},
					},
				},
				"group": map[string]map[string][]string{
					"admin": {
						"user": {"admin"},
					},
					"owner": {
						"user":     {"owner"},
						"employee": {"owner"},
					},
				},
			},
		},
		{
			model: `
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user]
				type folder
					relations
						define owner: [group]
						define viewer: member from owner
				type document
					relations
						define banned: [user]
						define owner: [folder]
						define viewer: viewer from owner
						define can_view: viewer but not banned
						define can_see: can_view
			`,
			expectedConnectedTypes: TypesystemConnectedTypes{
				"document": map[string]map[string][]string{
					"banned": {
						"user": {"banned"},
					},
					"owner": {
						"folder": {"owner"},
					},
				},
				"folder": map[string]map[string][]string{
					"owner": {
						"group": {"owner"},
					},
					"viewer": {
						"user": {"member"},
					},
				},
				"group": map[string]map[string][]string{
					"member": {
						"user": {"member"},
					},
				},
			},
		},
		{
			model: `
				model
					schema 1.1
				type user
				type folder
					relations
						define owner: [user]
						define viewer: [user, user:*] or owner
				type document
					relations
						define can_read: viewer from parent
						define parent: [document, folder]
						define viewer: [user, user:*]
			`,
			expectedConnectedTypes: TypesystemConnectedTypes{
				"document": map[string]map[string][]string{
					"can_read": {
						"user":   {"viewer"},
						"user:*": {"viewer"},
					},
					"parent": {
						"document": {"parent"},
						"folder":   {"parent"},
					},
					"viewer": {
						"user":   {"viewer"},
						"user:*": {"viewer"},
					},
				},
				"folder": map[string]map[string][]string{
					"owner": {
						"user": {"owner"},
					},
				},
			},
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("stage-%d", i), func(t *testing.T) {
			ts, err := NewAndValidate(context.Background(), testutils.MustTransformDSLToProtoWithID(test.model))
			require.NoError(t, err)
			require.Equal(t, test.expectedConnectedTypes, ts.connectedTypes)
		})
	}
}
