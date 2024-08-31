package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	language "github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/authclaims"
	"github.com/openfga/openfga/pkg/authz"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

type store struct {
	id      string
	modelID string
}

type authzSettings struct {
	openfga  *Server
	clientID string
	root     *store
	test     *store
}

func newSetupAuthzModelAndTuples(t *testing.T, openfga *Server, clientID string) *authzSettings {
	rootStore, err := openfga.CreateStore(context.Background(),
		&openfgav1.CreateStoreRequest{Name: "root-store"})

	writeAuthzModelResp, err := openfga.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId: rootStore.Id,
		TypeDefinitions: language.MustTransformDSLToProto(`
		  model
			schema 1.1
		
		type system
			relations
			define can_call_create_stores: [application, application:*] or admin
			define admin: [application]
		
		type application
		
		type module
			relations
			define can_call_write: writer or writer from store
			define store: [store]
			define writer: [application]
		
		type store
			relations
			define system: [system]
			define creator: [application]
			define can_call_delete_store: [application] or admin
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
		`).GetTypeDefinitions(),
		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	rootStoreModelID := writeAuthzModelResp.GetAuthorizationModelId()

	_, err = openfga.Write(context.Background(), &openfgav1.WriteRequest{
		StoreId:              rootStore.Id,
		AuthorizationModelId: rootStoreModelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey(fmt.Sprintf("store:%s", rootStore.Id), "admin", fmt.Sprintf("application:%s", clientID)),
			},
		},
	})
	require.NoError(t, err)

	testStore, err := openfga.CreateStore(context.Background(),
		&openfgav1.CreateStoreRequest{Name: "test-store"})

	writeTestStoreAuthzModelResp, err := openfga.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId: testStore.Id,
		TypeDefinitions: language.MustTransformDSLToProto(`
			model
				schema 1.1

			type channel
			relations
				define commenter: [user, workspace#member] or writer
				define parent_workspace: [workspace]
				define writer: [user, workspace#member]

			type user

			type workspace
			relations
				define channels_admin: [user] or legacy_admin
				define guest: [user]
				define legacy_admin: [user]
				define member: [user] or legacy_admin or channels_admin
		`).GetTypeDefinitions(),
		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	testStoreModelID := writeTestStoreAuthzModelResp.GetAuthorizationModelId()

	return &authzSettings{
		openfga:  openfga,
		clientID: clientID,
		root:     &store{id: rootStore.Id, modelID: rootStoreModelID},
		test:     &store{id: testStore.Id, modelID: testStoreModelID},
	}
}

func (s *authzSettings) addAuthForRelation(t *testing.T, ctx context.Context, authzRelation string) {
	_, err := s.openfga.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              s.root.id,
		AuthorizationModelId: s.root.modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				tuple.NewTupleKey(fmt.Sprintf("store:%s", s.test.id), authzRelation, fmt.Sprintf("application:%s", s.clientID)),
			},
		},
	})
	require.NoError(t, err)
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
			StoreId:              settings.root.id,
			AuthorizationModelId: settings.root.modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey(fmt.Sprintf("store:%s", settings.test.id), authz.CanCallGetStore, fmt.Sprintf("application:%s", clientID)),
				},
			},
		})
		require.NoError(t, err)

		_, err = openfga.ListObjects(context.Background(), &openfgav1.ListObjectsRequest{
			StoreId:              settings.test.id,
			AuthorizationModelId: settings.test.modelID,
			Type:                 "workspace",
			Relation:             "guest",
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
			StoreId:              settings.test.id,
			AuthorizationModelId: settings.test.modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("workspace:1", "guest", "user:ben"),
				},
			},
		})
		require.NoError(t, err)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.root.id, ModelID: settings.root.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.ListObjects(ctx, &openfgav1.ListObjectsRequest{
				StoreId:              settings.test.id,
				AuthorizationModelId: settings.test.modelID,
				Type:                 "workspace",
				Relation:             "guest",
				User:                 "user:ben",
			})
			require.Error(t, err)
			require.Equal(t, "rpc error: code = PermissionDenied desc = permission denied", err.Error())
		})

		t.Run("successfully_call_list_objects", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(t, ctx, authz.CanCallListObjects)

			listObjectsResponse, err := openfga.ListObjects(ctx, &openfgav1.ListObjectsRequest{
				StoreId:              settings.test.id,
				AuthorizationModelId: settings.test.modelID,
				Type:                 "workspace",
				Relation:             "guest",
				User:                 "user:ben",
			})
			require.NoError(t, err)

			require.Equal(t, listObjectsResponse.GetObjects(), []string([]string{"workspace:1"}))
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
			StoreId:              settings.test.id,
			AuthorizationModelId: settings.test.modelID,
			Type:                 "workspace",
			Relation:             "guest",
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
			StoreId:              settings.test.id,
			AuthorizationModelId: settings.test.modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("workspace:1", "guest", "user:ben"),
				},
			},
		})
		require.NoError(t, err)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.root.id, ModelID: settings.root.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			server := NewMockStreamServer(ctx)
			err = openfga.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
				StoreId:              settings.test.id,
				AuthorizationModelId: settings.test.modelID,
				Type:                 "workspace",
				Relation:             "guest",
				User:                 "user:ben",
			}, server)
			require.Error(t, err)

			require.Equal(t, "rpc error: code = PermissionDenied desc = permission denied", err.Error())
		})

		t.Run("successfully_call_streamed_list_objects", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(t, ctx, authz.CanCallListObjects)

			server := NewMockStreamServer(ctx)
			err = openfga.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
				StoreId:              settings.test.id,
				AuthorizationModelId: settings.test.modelID,
				Type:                 "workspace",
				Relation:             "guest",
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
			StoreId: settings.test.id,
			TupleKey: &openfgav1.ReadRequestTupleKey{
				User:     "user:anne",
				Relation: "guest",
				Object:   "workspace:1",
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
			StoreId:              settings.test.id,
			AuthorizationModelId: settings.test.modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("workspace:1", "guest", "user:ben"),
				},
			},
		})
		require.NoError(t, err)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.root.id, ModelID: settings.root.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Read(ctx, &openfgav1.ReadRequest{
				StoreId: settings.test.id,
				TupleKey: &openfgav1.ReadRequestTupleKey{
					User:     "user:ben",
					Relation: "guest",
					Object:   "workspace:1",
				},
			})
			require.Error(t, err)
			require.Equal(t, "rpc error: code = PermissionDenied desc = permission denied", err.Error())
		})

		t.Run("successfully_call_read", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(t, ctx, authz.CanCallRead)

			readResponse, err := openfga.Read(ctx, &openfgav1.ReadRequest{
				StoreId: settings.test.id,
				TupleKey: &openfgav1.ReadRequestTupleKey{
					User:     "user:ben",
					Relation: "guest",
					Object:   "workspace:1",
				},
			})
			require.NoError(t, err)

			require.Equal(t, len(readResponse.GetTuples()), 1)
			require.Equal(t, readResponse.GetTuples()[0].Key, tuple.NewTupleKey("workspace:1", "guest", "user:ben"))
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
			StoreId:              settings.test.id,
			AuthorizationModelId: settings.test.modelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("workspace:1", "guest", "user:ben"),
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

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.root.id, ModelID: settings.root.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.test.id,
				AuthorizationModelId: settings.test.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("workspace:1", "guest", "user:ben"),
					},
				},
			})
			require.Error(t, err)
			require.Equal(t, "rpc error: code = PermissionDenied desc = permission denied", err.Error())
		})

		t.Run("successfully_call_write", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(t, ctx, authz.CanCallWrite)

			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.test.id,
				AuthorizationModelId: settings.test.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("workspace:1", "guest", "user:ben"),
					},
				},
			})
			require.NoError(t, err)
		})
	})
}

func TestCheckAuthzListStores(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}

func TestCheckCreateStoreAuthz(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}

func TestCheckAuthz(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("check_no_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		err := openfga.CheckAuthz(context.Background(), storeID, authz.Check)
		require.NoError(t, err)
	})

	t.Run("check_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.root.id, ModelID: settings.root.modelID}, openfga, openfga.logger)

		t.Run("with_SkipAuthzCheckFromContext_set", func(t *testing.T) {
			fmt.Printf("%v", openfga.authorizer)
			ctx := authz.ContextWithSkipAuthzCheck(context.Background(), true)

			err := openfga.CheckAuthz(ctx, settings.test.id, authz.Check)
			require.NoError(t, err)
		})

		t.Run("error_with_no_client_id_found", func(t *testing.T) {
			err := openfga.CheckAuthz(context.Background(), settings.test.id, authz.Check)
			require.Error(t, err)
			require.Equal(t, "rpc error: code = Internal desc = client ID not found in context", err.Error())
		})

		t.Run("error_with_empty_client_id", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: ""})
			err := openfga.CheckAuthz(ctx, settings.test.id, authz.Check)
			require.Error(t, err)
			require.Equal(t, "rpc error: code = Internal desc = client ID not found in context", err.Error())
		})

		t.Run("error_when_authorized_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "ID"})
			err := openfga.CheckAuthz(ctx, settings.test.id, "invalid api method")
			require.Error(t, err)
			require.Equal(t, "unknown api method: invalid api method", err.Error())
		})

		t.Run("error_check_when_not_authorized", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			err := openfga.CheckAuthz(ctx, settings.test.id, authz.Check)
			require.Error(t, err)
			require.Equal(t, "rpc error: code = PermissionDenied desc = permission denied", err.Error())
		})

		t.Run("authz_is_valid", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(t, ctx, authz.CanCallCheck)

			err := openfga.CheckAuthz(ctx, settings.test.id, authz.Check)
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

	t.Run("check_no_authz_should_succeed", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"

		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		checkResponse, err := openfga.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId:              settings.root.id,
			AuthorizationModelId: settings.root.modelID,
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     fmt.Sprintf("application:%s", clientID),
				Relation: "admin",
				Object:   fmt.Sprintf("store:%s", settings.root.id),
			},
		})
		require.NoError(t, err)
		require.True(t, checkResponse.GetAllowed())
	})

	t.Run("check_with_authz", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		clientID := "validclientid"
		settings := newSetupAuthzModelAndTuples(t, openfga, clientID)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: settings.root.id, ModelID: settings.root.modelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Check(ctx, &openfgav1.CheckRequest{
				StoreId:              settings.test.id,
				AuthorizationModelId: settings.test.modelID,
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     fmt.Sprintf("application:%s", clientID),
					Relation: "reader",
					Object:   fmt.Sprintf("store:%s", settings.test.id),
				},
			})
			require.Error(t, err)
			require.Equal(t, "rpc error: code = PermissionDenied desc = permission denied", err.Error())
		})

		t.Run("successfully_call_check", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			settings.addAuthForRelation(t, ctx, "writer")
			settings.addAuthForRelation(t, ctx, authz.CanCallCheck)
			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              settings.test.id,
				AuthorizationModelId: settings.test.modelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("workspace:1", "guest", "user:ben"),
					},
				},
			})
			require.NoError(t, err)

			checkResponse, err := openfga.Check(ctx, &openfgav1.CheckRequest{
				StoreId:              settings.test.id,
				AuthorizationModelId: settings.test.modelID,
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:ben",
					Relation: "guest",
					Object:   "workspace:1",
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
}

func TestReadAuthorizationModel(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}

func TestReadAuthorizationModels(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}

func TestWriteAssertions(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}

func TestReadAssertions(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}

func TestReadChanges(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}

func TestCreateStore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}

func TestDeleteStore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}

func TestGetStore(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}

func TestListStores(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)
}
