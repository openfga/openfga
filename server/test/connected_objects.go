package test

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func ConnectedObjectsTest(t *testing.T, ds storage.OpenFGADatastore) {

	tests := []struct {
		name             string
		model            *openfgapb.AuthorizationModel
		tuples           []*openfgapb.TupleKey
		request          *commands.ConnectedObjectsRequest
		resolveNodeLimit uint32
		limit            uint32
		expectedObjects  []string
		expectedError    error
	}{
		{
			name: "Direct relations and TTU relations with wildcard",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User:       &openfgapb.ObjectRelation{Object: "user:jon"},
				ContextualTuples: []*openfgapb.TupleKey{
					tuple.NewTupleKey("folder:folderX", "parent", "*"),
				},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.Union(
								typesystem.This(),
								typesystem.TupleToUserset("parent", "viewer"),
							),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "folder",
										},
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
			},
			expectedError: serverErrors.InvalidTuple(
				fmt.Sprintf("unexpected wildcard evaluated on relation '%s#%s'", "folder", "parent"),
				tuple.NewTupleKey("folder:folderX", "parent", tuple.Wildcard),
			),
		},
		{
			name: "Direct relations and TTU relations with strictly contextual tuples",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User:       &openfgapb.ObjectRelation{Object: "user:jon"},
				ContextualTuples: []*openfgapb.TupleKey{
					tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
					tuple.NewTupleKey("folder:folderX", "parent", "*"),
				},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.Union(
								typesystem.This(),
								typesystem.TupleToUserset("parent", "viewer"),
							),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{
											Type: "folder",
										},
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{},
			expectedError: serverErrors.InvalidTuple(
				fmt.Sprintf("unexpected wildcard evaluated on relation '%s#%s'", "folder", "parent"),
				tuple.NewTupleKey("folder:folderX", "parent", tuple.Wildcard),
			),
		},
		{
			name: "Restrict results based on limit",
			request: &commands.ConnectedObjectsRequest{
				StoreID:          ulid.Make().String(),
				ObjectType:       "folder",
				Relation:         "viewer",
				User:             &openfgapb.ObjectRelation{Object: "user:jon"},
				ContextualTuples: []*openfgapb.TupleKey{},
			},
			limit: 2,
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
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
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder2", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder3", "viewer", "user:jon"),
			},
			expectedObjects: []string{"folder:folder1", "folder:folder2"},
		},
		{
			name: "Resolve direct relationships with tuples and contextual tuples",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &openfgapb.ObjectRelation{Object: "user:jon"},
				ContextualTuples: []*openfgapb.TupleKey{
					tuple.NewTupleKey("document:doc2", "viewer", "user:bob"),
					tuple.NewTupleKey("document:doc3", "viewer", "user:jon"),
				},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
			},
			expectedObjects: []string{"document:doc1", "document:doc3"},
		},
		{
			name: "Direct relations involving relationships with users and usersets",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &openfgapb.ObjectRelation{Object: "user:jon"},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"viewer": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "viewer", "user:jon"),
				tuple.NewTupleKey("document:doc2", "viewer", "user:bob"),
				tuple.NewTupleKey("document:doc3", "viewer", "group:openfga#member"),
				tuple.NewTupleKey("group:openfga", "member", "user:jon"),
			},
			expectedObjects: []string{"document:doc1", "document:doc3"},
		},
		{
			name: "Success with direct relationships and computed usersets",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &openfgapb.ObjectRelation{Object: "user:jon"},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"owner":  typesystem.This(),
							"viewer": typesystem.ComputedUserset("owner"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"owner": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("document:doc1", "owner", "user:jon"),
				tuple.NewTupleKey("document:doc2", "owner", "user:bob"),
				tuple.NewTupleKey("document:doc3", "owner", "group:openfga#member"),
				tuple.NewTupleKey("group:openfga", "member", "user:jon"),
			},
			expectedObjects: []string{"document:doc1", "document:doc3"},
		},
		{
			name: "Success with many tuples",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "document",
				Relation:   "viewer",
				User:       &openfgapb.ObjectRelation{Object: "user:jon"},
				ContextualTuples: []*openfgapb.TupleKey{
					tuple.NewTupleKey("folder:folder5", "parent", "folder:folder4"),
					tuple.NewTupleKey("folder:folder6", "viewer", "user:bob"),
				},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.Union(
								typesystem.This(),
								typesystem.TupleToUserset("parent", "viewer"),
							),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{Type: "folder"},
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
					{
						Type: "document",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.TupleToUserset("parent", "viewer"),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{Type: "folder"},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder2", "parent", "folder:folder1"),
				tuple.NewTupleKey("folder:folder3", "parent", "folder:folder2"),
				tuple.NewTupleKey("folder:folder4", "viewer", "group:eng#member"),

				tuple.NewTupleKey("document:doc1", "parent", "folder:folder3"),
				tuple.NewTupleKey("document:doc2", "parent", "folder:folder5"),
				tuple.NewTupleKey("document:doc3", "parent", "folder:folder6"),

				tuple.NewTupleKey("group:eng", "member", "group:openfga#member"),
				tuple.NewTupleKey("group:openfga", "member", "user:jon"),
			},
			expectedObjects: []string{"document:doc1", "document:doc2"},
		},
		{
			name: "Resolve objects involved in recursive hierarchy",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User:       &openfgapb.ObjectRelation{Object: "user:jon"},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.Union(
								typesystem.This(),
								typesystem.TupleToUserset("parent", "viewer"),
							),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{Type: "folder"},
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder2", "parent", "folder:folder1"),
				tuple.NewTupleKey("folder:folder3", "parent", "folder:folder2"),
			},
			expectedObjects: []string{"folder:folder1", "folder:folder2", "folder:folder3"},
		},
		{
			name: "Resolution Depth Exceeded Failure",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "folder",
				Relation:   "viewer",
				User:       &openfgapb.ObjectRelation{Object: "user:jon"},
			},
			resolveNodeLimit: 2,
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "folder",
						Relations: map[string]*openfgapb.Userset{
							"parent": typesystem.This(),
							"viewer": typesystem.Union(
								typesystem.This(),
								typesystem.TupleToUserset("parent", "viewer"),
							),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"parent": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{Type: "folder"},
									},
								},
								"viewer": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										{Type: "user"},
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("folder:folder1", "viewer", "user:jon"),
				tuple.NewTupleKey("folder:folder2", "parent", "folder:folder1"),
				tuple.NewTupleKey("folder:folder3", "parent", "folder:folder2"),
			},
			expectedError: serverErrors.AuthorizationModelResolutionTooComplex,
		},
		{
			name: "Objects connected to a userset",
			request: &commands.ConnectedObjectsRequest{
				StoreID:    ulid.Make().String(),
				ObjectType: "group",
				Relation:   "member",
				User:       &openfgapb.ObjectRelation{Object: "group:iam", Relation: "member"},
			},
			model: &openfgapb.AuthorizationModel{
				Id:            ulid.Make().String(),
				SchemaVersion: typesystem.SchemaVersion1_1,
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "user",
					},
					{
						Type: "group",
						Relations: map[string]*openfgapb.Userset{
							"member": typesystem.This(),
						},
						Metadata: &openfgapb.Metadata{
							Relations: map[string]*openfgapb.RelationMetadata{
								"member": {
									DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
										typesystem.DirectRelationReference("user", ""),
										typesystem.DirectRelationReference("group", "member"),
									},
								},
							},
						},
					},
				},
			},
			tuples: []*openfgapb.TupleKey{
				tuple.NewTupleKey("group:auth0", "member", "group:eng#member"),
				tuple.NewTupleKey("group:eng", "member", "group:iam#member"),
				tuple.NewTupleKey("group:iam", "member", "user:jon"),
			},
			expectedObjects: []string{"group:auth0", "group:eng"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			ctx := context.Background()
			store := ulid.Make().String()
			test.request.StoreID = store

			err := ds.WriteAuthorizationModel(ctx, store, test.model)
			require.NoError(err)

			err = ds.Write(ctx, store, nil, test.tuples)
			require.NoError(err)

			if test.resolveNodeLimit == 0 {
				test.resolveNodeLimit = defaultResolveNodeLimit
			}

			connectedObjectsCmd := commands.ConnectedObjectsCommand{
				Datastore:        ds,
				Typesystem:       typesystem.New(test.model),
				ResolveNodeLimit: test.resolveNodeLimit,
				Limit:            test.limit,
			}

			resultChan := make(chan string, 100)
			done := make(chan struct{})

			var results []string
			go func() {
				for result := range resultChan {
					results = append(results, result)
				}

				done <- struct{}{}
			}()

			timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			go func() {
				err = connectedObjectsCmd.StreamedConnectedObjects(timeoutCtx, test.request, resultChan)
				require.ErrorIs(err, test.expectedError)
				close(resultChan)
			}()

			select {
			case <-timeoutCtx.Done():
				require.FailNow("timed out waiting for response")
			case <-done:
			}

			if test.expectedError == nil {
				sort.Strings(results)
				sort.Strings(test.expectedObjects)

				require.Equal(test.expectedObjects, results)
			}
		})
	}
}
