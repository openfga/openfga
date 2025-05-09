package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/wrapperspb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	"github.com/openfga/openfga/internal/authz"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/authclaims"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type storeAndModel struct {
	id      string
	modelID string
}

type authzSettings struct {
	openfga  *Server
	clientID string
	rootData *storeAndModel
	testData *storeAndModel
}

const (
	rootStoreModel = `
		  model
			schema 1.1
		
		type system
			relations
			define can_call_create_stores: [application, application:*] or admin
			define can_call_list_stores: [application, application:*] or admin
			define admin: [application]
		
		type application
		
		type module
			relations
			define can_call_write: [application] or writer or writer from store
			define store: [store]
			define writer: [application]
		
		type store
			relations
			define system: [system]
			define creator: [application]
			define can_call_delete_store: [application] or admin
			define can_call_update_store: [application] or admin
			define can_call_get_store: [application] or admin
			define can_call_check: [application] or reader
			define can_call_expand: [application] or reader
			define can_call_list_objects: [application] or reader
			define can_call_list_users: [application] or reader
			define can_call_read: [application] or reader
			define can_call_read_assertions: [application] or reader or model_writer
			define can_call_read_authorization_models: [application] or reader or model_writer
			define can_call_read_changes: [application] or reader
			define can_call_write: [application] or writer
			define can_call_write_assertions: [application] or model_writer
			define can_call_write_authorization_models: [application] or model_writer
			define model_writer: [application] or admin
			define reader: [application] or admin
			define writer: [application] or admin
			define admin: [application] or creator or admin from system
		`
)

func testStoreModelWithModule() []*openfgav1.TypeDefinition {
	// Add a module to the test store
	typeDefs := []*openfgav1.TypeDefinition{{
		Type: "user",
	}}

	// Add as many modules as necessary (we do this dynamically in case the max modules changes)
	for moduleIndex := range authz.MaxModulesInRequest + 1 {
		typeDefs = append(typeDefs, &openfgav1.TypeDefinition{
			Type: fmt.Sprintf("module%v", moduleIndex),
			Metadata: &openfgav1.Metadata{
				Module: fmt.Sprintf("module%v", moduleIndex),
				Relations: map[string]*openfgav1.RelationMetadata{
					"member": {
						DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
							{Type: "user"},
						},
					},
				},
			},
			Relations: map[string]*openfgav1.Userset{
				"member": {
					Userset: &openfgav1.Userset_This{},
				},
			},
		})
	}

	return typeDefs
}

func newSetupAuthzModelAndTuples(t *testing.T, openfga *Server, clientID string) *authzSettings {
	rootStore, err := openfga.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "root-store"})
	require.NoError(t, err)

	writeAuthzModelResp, err := openfga.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         rootStore.GetId(),
		TypeDefinitions: parser.MustTransformDSLToProto(rootStoreModel).GetTypeDefinitions(),
		SchemaVersion:   typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	rootStoreModelID := writeAuthzModelResp.GetAuthorizationModelId()

	_, err = openfga.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId:              rootStore.GetId(),
		AuthorizationModelId: rootStoreModelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey(fmt.Sprintf("store:%s", rootStore.GetId()), "admin", fmt.Sprintf("application:%s", clientID)),
			},
		},
	})
	require.NoError(t, err)

	testStore, err := openfga.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test-store"})
	require.NoError(t, err)

	writeTestStoreAuthzModelResp, err := openfga.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         testStore.GetId(),
		TypeDefinitions: testStoreModelWithModule(),
		SchemaVersion:   typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	testStoreModelID := writeTestStoreAuthzModelResp.GetAuthorizationModelId()

	return &authzSettings{
		openfga:  openfga,
		clientID: clientID,
		rootData: &storeAndModel{id: rootStore.GetId(), modelID: rootStoreModelID},
		testData: &storeAndModel{id: testStore.GetId(), modelID: testStoreModelID},
	}
}

func (s *authzSettings) writeHelper(ctx context.Context, t *testing.T, storeID, modelID string, tuple *openfgav1.TupleKey) {
	req := &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple,
			},
		},
	}
	_, err := s.openfga.Write(ctx, req)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, err := s.openfga.Write(ctx, &openfgav1.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
					{
						User:     tuple.GetUser(),
						Relation: tuple.GetRelation(),
						Object:   tuple.GetObject(),
					},
				},
			},
		})
		require.NoError(t, err)
	})
}

func (s *authzSettings) addAuthForRelation(ctx context.Context, t *testing.T, authzRelation string) {
	tuple := tuple.NewTupleKey(fmt.Sprintf("store:%s", s.testData.id), authzRelation, fmt.Sprintf("application:%s", s.clientID))

	s.writeHelper(ctx, t, s.rootData.id, s.rootData.modelID, tuple)
}

func TestListObjects(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("list_objects_no_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		_, err := openfga.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId:              settings.rootData.id,
			AuthorizationModelId: settings.rootData.modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey(fmt.Sprintf("store:%s", settings.testData.id), authz.CanCallGetStore, fmt.Sprintf("application:%s", clientID)),
				},
			},
		})
		require.NoError(t, err)

		_, err = openfga.ListObjects(context.Background(), &openfgav1.ListObjectsRequest{
			StoreId:              settings.testData.id,
			AuthorizationModelId: settings.testData.modelID,
			Type:                 "module1",
			Relation:             "member",
			User:                 "user:ben",
		})
		require.NoError(t, err)
	})

	t.Run("list_objects_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)
		_, err := openfga.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId:              settings.testData.id,
			AuthorizationModelId: settings.testData.modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("module1:1", "member", "user:ben"),
				},
			},
		})
		require.NoError(t, err)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.ListObjects(ctx, &openfgav1.ListObjectsRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Type:                 "module1",
				Relation:             "member",
				User:                 "user:ben",
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_list_objects", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallListObjects)

			listObjectsResponse, err := openfga.ListObjects(ctx, &openfgav1.ListObjectsRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Type:                 "module1",
				Relation:             "member",
				User:                 "user:ben",
			})
			require.NoError(t, err)

			require.Equal(t, []string{"module1:1"}, listObjectsResponse.GetObjects())
		})
	})
}

func TestStreamedListObjects(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("streamed_list_objects_no_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		err := openfga.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
			StoreId:              settings.testData.id,
			AuthorizationModelId: settings.testData.modelID,
			Type:                 "module1",
			Relation:             "member",
			User:                 "user:ben",
		}, NewMockStreamServer(context.Background()))
		require.NoError(t, err)
	})

	t.Run("streamed_list_objects_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)
		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)
		_, err := openfga.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId:              settings.testData.id,
			AuthorizationModelId: settings.testData.modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("module1:1", "member", "user:ben"),
				},
			},
		})
		require.NoError(t, err)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			server := NewMockStreamServer(ctx)
			err = openfga.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Type:                 "module1",
				Relation:             "member",
				User:                 "user:ben",
			}, server)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_streamed_list_objects", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallListObjects)

			server := NewMockStreamServer(ctx)
			err = openfga.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Type:                 "module1",
				Relation:             "member",
				User:                 "user:ben",
			}, server)
			require.NoError(t, err)
		})
	})
}

func TestRead(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("read_no_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		_, err := openfga.Read(context.Background(), &openfgav1.ReadRequest{
			StoreId: settings.testData.id,
			TupleKey: &openfgav1.ReadRequestTupleKey{
				User:     "user:anne",
				Relation: "member",
				Object:   "module1:1",
			},
		})
		require.NoError(t, err)
	})

	t.Run("read_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)
		_, err := openfga.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId:              settings.testData.id,
			AuthorizationModelId: settings.testData.modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("module1:1", "member", "user:ben"),
				},
			},
		})
		require.NoError(t, err)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Read(ctx, &openfgav1.ReadRequest{
				StoreId: settings.testData.id,
				TupleKey: &openfgav1.ReadRequestTupleKey{
					User:     "user:ben",
					Relation: "member",
					Object:   "module1:1",
				},
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_read", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallRead)

			readResponse, err := openfga.Read(ctx, &openfgav1.ReadRequest{
				StoreId: settings.testData.id,
				TupleKey: &openfgav1.ReadRequestTupleKey{
					User:     "user:ben",
					Relation: "member",
					Object:   "module1:1",
				},
			})
			require.NoError(t, err)

			require.Len(t, readResponse.GetTuples(), 1)
			require.Equal(t, tuple.NewTupleKey("module1:1", "member", "user:ben"), readResponse.GetTuples()[0].GetKey())
		})
	})
}

func TestWrite(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("write_no_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		_, err := openfga.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId:              settings.testData.id,
			AuthorizationModelId: settings.testData.modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("module1:1", "member", "user:ben"),
				},
			},
		})
		require.NoError(t, err)
	})

	t.Run("write_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("module1:1", "member", "user:ben"),
					},
				},
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_write", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallWrite)

			settings.writeHelper(ctx, t, settings.testData.id, settings.testData.modelID, tuple.NewTupleKey("module1:1", "member", "user:ben"))
		})

		t.Run("errors_when_not_authorized_for_all_modules", func(t *testing.T) {
			tuples := []*openfgav1.TupleKey{}
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})

			for index := range authz.MaxModulesInRequest {
				tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("module%v:1", index+1), "member", "user:ben"))
				// Keep one w/o access
				if index != 0 {
					settings.writeHelper(ctx, t, settings.rootData.id, settings.rootData.modelID, tuple.NewTupleKey(fmt.Sprintf("module:%s|%s", settings.testData.id, fmt.Sprintf("module%d", index+1)), authz.CanCallWrite, fmt.Sprintf("application:%s", clientID)))
				}
			}

			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: tuples,
				},
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_write_for_modules", func(t *testing.T) {
			tuples := []*openfgav1.TupleKey{}
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})

			for index := range authz.MaxModulesInRequest {
				tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("module%v:1", index+1), "member", "user:ben"))
				// grant access to all modules
				settings.writeHelper(ctx, t, settings.rootData.id, settings.rootData.modelID, tuple.NewTupleKey(fmt.Sprintf("module:%s|%s", settings.testData.id, fmt.Sprintf("module%d", index+1)), authz.CanCallWrite, fmt.Sprintf("application:%s", clientID)))
			}

			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: tuples,
				},
			})
			require.NoError(t, err)

			tuplesInDelete := []*openfgav1.TupleKeyWithoutCondition{}
			for _, tuple := range tuples {
				tuplesInDelete = append(tuplesInDelete, &openfgav1.TupleKeyWithoutCondition{
					User:     tuple.GetUser(),
					Relation: tuple.GetRelation(),
					Object:   tuple.GetObject(),
				})
			}

			_, err = openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: tuplesInDelete,
				},
			})
			require.NoError(t, err)
		})

		t.Run("errors_when_sending_more_than_max_modules", func(t *testing.T) {
			tuples := []*openfgav1.TupleKey{}
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})

			for index := range authz.MaxModulesInRequest + 1 {
				tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("module%d:1", index), "member", "user:ben"))
				// grant access to all modules
				settings.writeHelper(ctx, t, settings.rootData.id, settings.rootData.modelID, tuple.NewTupleKey(fmt.Sprintf("module:%s|%s", settings.testData.id, fmt.Sprintf("module%d", index+1)), authz.CanCallWrite, fmt.Sprintf("application:%s", clientID)))
			}

			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: tuples,
				},
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)

			tuplesInDelete := []*openfgav1.TupleKeyWithoutCondition{}
			for _, tuple := range tuples {
				tuplesInDelete = append(tuplesInDelete, &openfgav1.TupleKeyWithoutCondition{
					User:     tuple.GetUser(),
					Relation: tuple.GetRelation(),
					Object:   tuple.GetObject(),
				})
			}

			_, err = openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: tuplesInDelete,
				},
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("success_when_sending_more_than_max_modules_with_store_level_write_permission", func(t *testing.T) {
			tuples := []*openfgav1.TupleKey{}
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})

			// grant access to the store
			settings.writeHelper(ctx, t, settings.rootData.id, settings.rootData.modelID, tuple.NewTupleKey(fmt.Sprintf("store:%s", settings.testData.id), authz.CanCallWrite, fmt.Sprintf("application:%s", clientID)))

			for index := range authz.MaxModulesInRequest + 1 {
				tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("module%d:1", index), "member", "user:ben"))
			}

			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: tuples,
				},
			})

			require.NoError(t, err)

			tuplesInDelete := []*openfgav1.TupleKeyWithoutCondition{}
			for _, tuple := range tuples {
				tuplesInDelete = append(tuplesInDelete, &openfgav1.TupleKeyWithoutCondition{
					User:     tuple.GetUser(),
					Relation: tuple.GetRelation(),
					Object:   tuple.GetObject(),
				})
			}

			_, err = openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: tuplesInDelete,
				},
			})

			require.NoError(t, err)
		})
	})
}

func TestCheckCreateStoreAuthz(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("checkCreateStoreAuthz_no_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		err := openfga.checkCreateStoreAuthz(context.Background())
		require.NoError(t, err)
	})

	t.Run("checkCreateStoreAuthz_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("with_SkipAuthzCheckFromContext_set", func(t *testing.T) {
			ctx := authclaims.ContextWithSkipAuthzCheck(context.Background(), true)

			err := openfga.checkCreateStoreAuthz(ctx)
			require.NoError(t, err)
		})

		t.Run("error_with_no_client_id_found", func(t *testing.T) {
			err := openfga.checkCreateStoreAuthz(context.Background())

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("error_with_empty_client_id", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: ""})
			err := openfga.checkCreateStoreAuthz(ctx)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("error_check_when_not_authorized", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			err := openfga.checkCreateStoreAuthz(ctx)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("authz_is_valid", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.writeHelper(ctx, t, settings.rootData.id, settings.rootData.modelID, tuple.NewTupleKey("system:fga", authz.CanCallCreateStore, fmt.Sprintf("application:%s", settings.clientID)))

			err := openfga.checkCreateStoreAuthz(ctx)
			require.NoError(t, err)
		})
	})
}

func TestCheckAuthz(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("authz_disabled_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		err := openfga.checkAuthz(context.Background(), storeID, apimethod.Check)
		require.NoError(t, err)
	})

	t.Run("authz_enabled", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("with_SkipAuthzCheckFromContext_set", func(t *testing.T) {
			ctx := authclaims.ContextWithSkipAuthzCheck(context.Background(), true)

			err := openfga.checkAuthz(ctx, settings.testData.id, apimethod.Check)
			require.NoError(t, err)
		})

		t.Run("error_with_no_client_id_found", func(t *testing.T) {
			err := openfga.checkAuthz(context.Background(), settings.testData.id, apimethod.Check)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("error_with_empty_client_id", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: ""})
			err := openfga.checkAuthz(ctx, settings.testData.id, apimethod.Check)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("error_when_authorized_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "ID"})
			err := openfga.checkAuthz(ctx, settings.testData.id, "invalid api method")

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("error_check_when_not_authorized", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			err := openfga.checkAuthz(ctx, settings.testData.id, apimethod.Check)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("authz_is_valid", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallCheck)

			err := openfga.checkAuthz(ctx, settings.testData.id, apimethod.Check)
			require.NoError(t, err)
		})
	})
}

func TestGetAccessibleStores(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("authz_disabled_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		_, err := openfga.getAccessibleStores(context.Background())
		require.NoError(t, err)
	})

	t.Run("authz_enabled", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		mockController := gomock.NewController(t)
		defer mockController.Finish()

		mockServer := mocks.NewMockServerInterface(mockController)
		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, mockServer, openfga.logger)

		t.Run("with_SkipAuthzCheckFromContext_set", func(t *testing.T) {
			ctx := authclaims.ContextWithSkipAuthzCheck(context.Background(), true)

			_, err := openfga.getAccessibleStores(ctx)
			require.NoError(t, err)
		})

		t.Run("error_with_no_client_id_found", func(t *testing.T) {
			_, err := openfga.getAccessibleStores(context.Background())

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("error_with_empty_client_id", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: ""})
			_, err := openfga.getAccessibleStores(ctx)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("error_when_AuthorizeListStores_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			errorMessage := fmt.Errorf("error")
			mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: false}, errorMessage)

			_, err := openfga.getAccessibleStores(ctx)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("error_when_ListAuthorizedStores_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil).AnyTimes()
			errorMessage := fmt.Errorf("error")
			mockServer.EXPECT().ListObjects(gomock.Any(), gomock.Any()).Return(nil, errorMessage)

			_, err := openfga.getAccessibleStores(ctx)
			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("authz_is_valid", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			mockServer.EXPECT().Check(gomock.Any(), gomock.Any()).Return(&openfgav1.CheckResponse{Allowed: true}, nil).AnyTimes()
			mockServer.EXPECT().ListObjects(gomock.Any(), gomock.Any()).Return(nil, nil)
			_, err := settings.openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.rootData.id,
				AuthorizationModelId: settings.rootData.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("system:fga", authz.CanCallListStores, fmt.Sprintf("application:%s", settings.clientID)),
					},
				},
			})
			require.NoError(t, err)

			_, err = openfga.getAccessibleStores(ctx)
			require.NoError(t, err)
		})
	})
}

func TestCheckWriteAuthz(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	model := &openfgav1.AuthorizationModel{
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: testStoreModelWithModule(),
	}
	typesys, err := typesystem.New(model)
	require.NoError(t, err)

	t.Run("checkWriteAuthz_no_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		err := openfga.checkWriteAuthz(context.Background(), &openfgav1.WriteRequest{
			StoreId: "store-id",
			Deletes: &openfgav1.WriteRequestDeletes{
				TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
					{Object: "folder-with-module:2", Relation: "viewer", User: "user:jon"},
				},
			},
		}, typesys)
		require.NoError(t, err)
	})

	t.Run("checkWriteAuthz_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("with_SkipAuthzCheckFromContext_set", func(t *testing.T) {
			ctx := authclaims.ContextWithSkipAuthzCheck(context.Background(), true)

			err := openfga.checkWriteAuthz(ctx, &openfgav1.WriteRequest{}, typesys)
			require.NoError(t, err)
		})

		t.Run("error_when_GetModulesForWriteRequest_errors", func(t *testing.T) {
			err := openfga.checkWriteAuthz(context.Background(), &openfgav1.WriteRequest{
				StoreId: settings.testData.id,
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
						{Object: "unknown:2", Relation: "viewer", User: "user:jon"},
					},
				},
			}, typesys)
			require.Error(t, err)
		})

		t.Run("error_when_checkAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			err := openfga.checkWriteAuthz(ctx, &openfgav1.WriteRequest{
				StoreId: settings.testData.id,
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
						{Object: "module1:2", Relation: "member", User: "user:jon"},
					},
				},
			}, typesys)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("authz_is_valid", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.writeHelper(ctx, t, settings.rootData.id, settings.rootData.modelID, tuple.NewTupleKey(fmt.Sprintf("module:%s|%s", settings.testData.id, "module1"), authz.CanCallWrite, fmt.Sprintf("application:%s", clientID)))

			err := openfga.checkWriteAuthz(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Deletes: &openfgav1.WriteRequestDeletes{
					TupleKeys: []*openfgav1.TupleKeyWithoutCondition{
						{Object: "module1:2", Relation: "member", User: "user:jon"},
					},
				},
			}, typesys)
			require.NoError(t, err)
		})
	})
}

func TestCheck(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("authz_disabled_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		checkResponse, err := openfga.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId:              settings.rootData.id,
			AuthorizationModelId: settings.rootData.modelID,
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     fmt.Sprintf("application:%s", clientID),
				Relation: "admin",
				Object:   fmt.Sprintf("store:%s", settings.rootData.id),
			},
		})
		require.NoError(t, err)
		require.True(t, checkResponse.GetAllowed())
	})

	t.Run("authz_enabled", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Check(ctx, &openfgav1.CheckRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     fmt.Sprintf("application:%s", clientID),
					Relation: "reader",
					Object:   fmt.Sprintf("store:%s", settings.testData.id),
				},
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_check", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, "writer")
			settings.addAuthForRelation(ctx, t, authz.CanCallCheck)
			settings.writeHelper(ctx, t, settings.testData.id, settings.testData.modelID, tuple.NewTupleKey("module1:1", "member", "user:ben"))

			checkResponse, err := openfga.Check(ctx, &openfgav1.CheckRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:ben",
					Relation: "member",
					Object:   "module1:1",
				},
			})

			require.NoError(t, err)
			require.True(t, checkResponse.GetAllowed())
		})
	})
}

func TestExpand(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("expand_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		expandResponse, err := openfga.Expand(context.Background(), &openfgav1.ExpandRequest{
			StoreId:              settings.rootData.id,
			AuthorizationModelId: settings.rootData.modelID,
			TupleKey: &openfgav1.ExpandRequestTupleKey{
				Relation: "admin",
				Object:   fmt.Sprintf("store:%s", settings.rootData.id),
			},
		})
		require.NoError(t, err)
		require.Equal(t, &openfgav1.ExpandResponse{
			Tree: &openfgav1.UsersetTree{
				Root: &openfgav1.UsersetTree_Node{
					Name: fmt.Sprintf("store:%s#admin", settings.rootData.id),
					Value: &openfgav1.UsersetTree_Node_Union{
						Union: &openfgav1.UsersetTree_Nodes{
							Nodes: []*openfgav1.UsersetTree_Node{
								{
									Name: fmt.Sprintf("store:%s#admin", settings.rootData.id),
									Value: &openfgav1.UsersetTree_Node_Leaf{
										Leaf: &openfgav1.UsersetTree_Leaf{
											Value: &openfgav1.UsersetTree_Leaf_Users{
												Users: &openfgav1.UsersetTree_Users{
													Users: []string{fmt.Sprintf("application:%s", clientID)},
												},
											},
										},
									},
								},
								{
									Name: fmt.Sprintf("store:%s#admin", settings.rootData.id),
									Value: &openfgav1.UsersetTree_Node_Leaf{
										Leaf: &openfgav1.UsersetTree_Leaf{
											Value: &openfgav1.UsersetTree_Leaf_Computed{
												Computed: &openfgav1.UsersetTree_Computed{
													Userset: fmt.Sprintf("store:%s#creator", settings.rootData.id),
												},
											},
										},
									},
								},
								{
									Name: fmt.Sprintf("store:%s#admin", settings.rootData.id),
									Value: &openfgav1.UsersetTree_Node_Leaf{
										Leaf: &openfgav1.UsersetTree_Leaf{
											Value: &openfgav1.UsersetTree_Leaf_TupleToUserset{
												TupleToUserset: &openfgav1.UsersetTree_TupleToUserset{
													Tupleset: fmt.Sprintf("store:%s#system", settings.rootData.id),
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
		}, expandResponse)
	})

	t.Run("expand_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Expand(ctx, &openfgav1.ExpandRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				TupleKey: &openfgav1.ExpandRequestTupleKey{
					Relation: "member",
					Object:   "module1:1",
				},
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_expand", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallExpand)

			expandResponse, err := openfga.Expand(ctx, &openfgav1.ExpandRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				TupleKey: &openfgav1.ExpandRequestTupleKey{
					Relation: "member",
					Object:   "module1:1",
				},
			})
			require.NoError(t, err)
			require.Equal(t, &openfgav1.ExpandResponse{
				Tree: &openfgav1.UsersetTree{
					Root: &openfgav1.UsersetTree_Node{
						Name: "module1:1#member",
						Value: &openfgav1.UsersetTree_Node_Leaf{
							Leaf: &openfgav1.UsersetTree_Leaf{
								Value: &openfgav1.UsersetTree_Leaf_Users{
									Users: &openfgav1.UsersetTree_Users{
										Users: []string{},
									},
								},
							},
						},
					},
				},
			}, expandResponse)
		})
	})
}

func TestReadAuthorizationModel(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("readAuthorizationModel_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		readAuthorizationModelResponse, err := openfga.ReadAuthorizationModel(
			context.Background(),
			&openfgav1.ReadAuthorizationModelRequest{
				StoreId: settings.testData.id,
				Id:      settings.testData.modelID,
			},
		)
		require.NoError(t, err)
		require.Equal(t, settings.testData.modelID, readAuthorizationModelResponse.GetAuthorizationModel().GetId())
		require.Equal(t, typesystem.SchemaVersion1_1, readAuthorizationModelResponse.GetAuthorizationModel().GetSchemaVersion())
	})

	t.Run("readAuthorizationModel_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.ReadAuthorizationModel(
				ctx,
				&openfgav1.ReadAuthorizationModelRequest{
					StoreId: settings.testData.id,
					Id:      settings.testData.modelID,
				},
			)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_readAuthorizationModel", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallReadAuthorizationModels)

			readAuthorizationModelResponse, err := openfga.ReadAuthorizationModel(
				ctx,
				&openfgav1.ReadAuthorizationModelRequest{
					StoreId: settings.testData.id,
					Id:      settings.testData.modelID,
				},
			)
			require.NoError(t, err)
			require.Equal(t, settings.testData.modelID, readAuthorizationModelResponse.GetAuthorizationModel().GetId())
			require.Equal(t, typesystem.SchemaVersion1_1, readAuthorizationModelResponse.GetAuthorizationModel().GetSchemaVersion())
		})
	})
}

func TestReadAuthorizationModels(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("readAuthorizationModels_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		readAuthorizationModelResponse, err := openfga.ReadAuthorizationModels(
			context.Background(),
			&openfgav1.ReadAuthorizationModelsRequest{
				StoreId: settings.testData.id,
			},
		)
		require.NoError(t, err)
		require.Len(t, readAuthorizationModelResponse.GetAuthorizationModels(), 1)
		require.Empty(t, readAuthorizationModelResponse.GetContinuationToken(), "expected an empty continuation token")
	})

	t.Run("readAuthorizationModels_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.ReadAuthorizationModels(
				ctx,
				&openfgav1.ReadAuthorizationModelsRequest{
					StoreId: settings.testData.id,
				},
			)

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_readAuthorizationModels", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallReadAuthorizationModels)

			readAuthorizationModelResponse, err := openfga.ReadAuthorizationModels(
				ctx,
				&openfgav1.ReadAuthorizationModelsRequest{
					StoreId: settings.testData.id,
				},
			)

			require.NoError(t, err)
			require.Len(t, readAuthorizationModelResponse.GetAuthorizationModels(), 1)
			require.Empty(t, readAuthorizationModelResponse.GetContinuationToken(), "expected an empty continuation token")
		})
	})
}

func TestWriteAssertions(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("writeAssertions_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		assertions := []*openfgav1.Assertion{
			{
				TupleKey:    tuple.NewAssertionTupleKey("module1:1", "member", "user:ben"),
				Expectation: false,
			},
		}
		_, err := openfga.WriteAssertions(context.Background(), &openfgav1.WriteAssertionsRequest{
			StoreId:              settings.testData.id,
			AuthorizationModelId: settings.testData.modelID,
			Assertions:           assertions,
		})
		require.NoError(t, err)
		readAssertionsResponse, err := openfga.ReadAssertions(context.Background(), &openfgav1.ReadAssertionsRequest{
			StoreId:              settings.testData.id,
			AuthorizationModelId: settings.testData.modelID,
		})
		require.NoError(t, err)
		if diff := cmp.Diff(openfgav1.ReadAssertionsResponse{
			AuthorizationModelId: settings.testData.modelID,
			Assertions:           assertions,
		}, readAssertionsResponse, protocmp.Transform()); diff != "" {
			t.Errorf("response mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("writeAssertions_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			assertions := []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("module1:1", "member", "user:ben"),
					Expectation: false,
				},
			}
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.WriteAssertions(ctx, &openfgav1.WriteAssertionsRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Assertions:           assertions,
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_writeAssertions", func(t *testing.T) {
			assertions := []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewAssertionTupleKey("module1:1", "member", "user:ben"),
					Expectation: false,
				},
			}
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallWriteAssertions)

			_, err := openfga.WriteAssertions(ctx, &openfgav1.WriteAssertionsRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Assertions:           assertions,
			})
			require.NoError(t, err)

			settings.addAuthForRelation(ctx, t, authz.CanCallReadAssertions)
			readAssertionsResponse, err := openfga.ReadAssertions(ctx, &openfgav1.ReadAssertionsRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
			})
			require.NoError(t, err)
			if diff := cmp.Diff(openfgav1.ReadAssertionsResponse{
				AuthorizationModelId: settings.testData.modelID,
				Assertions:           assertions,
			}, readAssertionsResponse, protocmp.Transform()); diff != "" {
				t.Errorf("response mismatch (-want +got):\n%s", diff)
			}
		})
	})
}

func TestReadAssertions(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("readAssertions_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		readAssertionsResponse, err := openfga.ReadAssertions(context.Background(), &openfgav1.ReadAssertionsRequest{
			StoreId:              settings.testData.id,
			AuthorizationModelId: settings.testData.modelID,
		})
		require.NoError(t, err)
		if diff := cmp.Diff(openfgav1.ReadAssertionsResponse{
			AuthorizationModelId: settings.testData.modelID,
			Assertions:           []*openfgav1.Assertion{},
		}, readAssertionsResponse, protocmp.Transform()); diff != "" {
			t.Errorf("response mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("readAssertions_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.ReadAssertions(ctx, &openfgav1.ReadAssertionsRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_readAssertions", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallReadAssertions)

			readAssertionsResponse, err := openfga.ReadAssertions(ctx, &openfgav1.ReadAssertionsRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
			})

			require.NoError(t, err)
			if diff := cmp.Diff(openfgav1.ReadAssertionsResponse{
				AuthorizationModelId: settings.testData.modelID,
				Assertions:           []*openfgav1.Assertion{},
			}, readAssertionsResponse, protocmp.Transform()); diff != "" {
				t.Errorf("response mismatch (-want +got):\n%s", diff)
			}
		})
	})
}

func TestReadChanges(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("readChanges_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		readChangesResponse, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  settings.testData.id,
			Type:     "user",
			PageSize: wrapperspb.Int32(50),
		})
		require.NoError(t, err)
		if diff := cmp.Diff(&openfgav1.ReadChangesResponse{
			Changes:           []*openfgav1.TupleChange{},
			ContinuationToken: "",
		}, readChangesResponse, protocmp.Transform()); diff != "" {
			t.Errorf("response mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("readChanges_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.ReadChanges(ctx, &openfgav1.ReadChangesRequest{
				StoreId:  settings.testData.id,
				Type:     "user",
				PageSize: wrapperspb.Int32(50),
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_readChanges", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallReadChanges)

			readChangesResponse, err := openfga.ReadChanges(ctx, &openfgav1.ReadChangesRequest{
				StoreId:  settings.testData.id,
				Type:     "user",
				PageSize: wrapperspb.Int32(50),
			})

			require.NoError(t, err)
			if diff := cmp.Diff(&openfgav1.ReadChangesResponse{
				Changes:           []*openfgav1.TupleChange{},
				ContinuationToken: "",
			}, readChangesResponse, protocmp.Transform()); diff != "" {
				t.Errorf("response mismatch (-want +got):\n%s", diff)
			}
		})
	})
}

func TestCreateStore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("createStore_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		_ = newSetupAuthzModelAndTuples(t, openfga, clientID)

		name := "new store"
		readChangesResponse, err := openfga.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
			Name: name,
		})
		require.NoError(t, err)
		require.Equal(t, name, readChangesResponse.GetName())
	})

	t.Run("createStore_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			name := "new store"
			_, err := openfga.CreateStore(ctx, &openfgav1.CreateStoreRequest{
				Name: name,
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_createStore", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.writeHelper(ctx, t, settings.rootData.id, settings.rootData.modelID, tuple.NewTupleKey("system:fga", authz.CanCallCreateStore, fmt.Sprintf("application:%s", settings.clientID)))

			name := "new store"
			readChangesResponse, err := openfga.CreateStore(ctx, &openfgav1.CreateStoreRequest{
				Name: name,
			})

			require.NoError(t, err)
			require.Equal(t, name, readChangesResponse.GetName())
		})
	})
}

func TestUpdateStore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("UpdateStore_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		_, err := openfga.UpdateStore(context.Background(), &openfgav1.UpdateStoreRequest{
			StoreId: settings.testData.id,
			Name:    "updated store",
		})
		require.NoError(t, err)
	})

	t.Run("UpdateStore_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.UpdateStore(ctx, &openfgav1.UpdateStoreRequest{
				StoreId: settings.testData.id,
				Name:    "updated store",
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_UpdateStore", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallUpdateStore)

			_, err := openfga.UpdateStore(ctx, &openfgav1.UpdateStoreRequest{
				StoreId: settings.testData.id,
				Name:    "updated store",
			})

			require.NoError(t, err)
		})
	})
}

func TestDeleteStore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("deleteStore_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		_, err := openfga.DeleteStore(context.Background(), &openfgav1.DeleteStoreRequest{
			StoreId: settings.testData.id,
		})
		require.NoError(t, err)
	})

	t.Run("deleteStore_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.DeleteStore(ctx, &openfgav1.DeleteStoreRequest{
				StoreId: settings.testData.id,
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_deleteStore", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallDeleteStore)

			_, err := openfga.DeleteStore(ctx, &openfgav1.DeleteStoreRequest{
				StoreId: settings.testData.id,
			})

			require.NoError(t, err)
		})
	})
}

func TestGetStore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("getStore_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		getStoreResponse, err := openfga.GetStore(context.Background(), &openfgav1.GetStoreRequest{
			StoreId: settings.testData.id,
		})
		require.NoError(t, err)
		require.Equal(t, settings.testData.id, getStoreResponse.GetId())
	})

	t.Run("getStore_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.GetStore(ctx, &openfgav1.GetStoreRequest{
				StoreId: settings.testData.id,
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_getStore", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(ctx, t, authz.CanCallGetStore)

			getStoreResponse, err := openfga.GetStore(ctx, &openfgav1.GetStoreRequest{
				StoreId: settings.testData.id,
			})

			require.NoError(t, err)
			require.Equal(t, settings.testData.id, getStoreResponse.GetId())
		})
	})
}

func TestListStores(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("listStores_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		newSetupAuthzModelAndTuples(t, openfga, clientID)

		_, err := openfga.ListStores(context.Background(), &openfgav1.ListStoresRequest{
			PageSize:          wrapperspb.Int32(1),
			ContinuationToken: "",
		})
		require.NoError(t, err)
	})

	t.Run("listStores_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_getAccessibleStores_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.ListStores(ctx, &openfgav1.ListStoresRequest{
				PageSize:          wrapperspb.Int32(1),
				ContinuationToken: "",
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_listStores", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := settings.openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.rootData.id,
				AuthorizationModelId: settings.rootData.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("system:fga", authz.CanCallListStores, fmt.Sprintf("application:%s", settings.clientID)),
					},
				},
			})
			require.NoError(t, err)

			_, err = openfga.ListStores(ctx, &openfgav1.ListStoresRequest{
				PageSize:          wrapperspb.Int32(1),
				ContinuationToken: "",
			})
			require.NoError(t, err)
		})
	})
}

func TestListUsers(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("listUsers_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		_, err := openfga.ListUsers(context.Background(), &openfgav1.ListUsersRequest{
			StoreId:              settings.testData.id,
			AuthorizationModelId: settings.testData.modelID,
			Object: &openfgav1.Object{
				Type: "module1",
				Id:   "1",
			},
			Relation: "member",
			UserFilters: []*openfgav1.UserTypeFilter{
				{Type: "user"},
			},
		})
		require.NoError(t, err)
	})

	t.Run("listUsers_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.rootData.id, ModelID: settings.rootData.modelID}, openfga, openfga.logger)

		t.Run("error_when_check_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.ListUsers(ctx, &openfgav1.ListUsersRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Object: &openfgav1.Object{
					Type: "module1",
					Id:   "1",
				},
				Relation: "member",
				UserFilters: []*openfgav1.UserTypeFilter{
					{Type: "user"},
				},
			})

			require.ErrorIs(t, err, authz.ErrUnauthorizedResponse)
		})

		t.Run("successfully_call_listUsers", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := settings.openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.rootData.id,
				AuthorizationModelId: settings.rootData.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey(fmt.Sprintf("store:%s", settings.testData.id), authz.CanCallListUsers, fmt.Sprintf("application:%s", settings.clientID)),
					},
				},
			})
			require.NoError(t, err)

			_, err = openfga.ListUsers(ctx, &openfgav1.ListUsersRequest{
				StoreId:              settings.testData.id,
				AuthorizationModelId: settings.testData.modelID,
				Object: &openfgav1.Object{
					Type: "module1",
					Id:   "1",
				},
				Relation: "member",
				UserFilters: []*openfgav1.UserTypeFilter{
					{Type: "user"},
				},
			})
			require.NoError(t, err)
		})
	})
}
