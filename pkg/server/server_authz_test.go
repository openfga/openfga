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

func setupAuthzModelAndTuples(t *testing.T, openfga *Server, clientID string) (rootStoreID, rootModelID, testStoreID, testModelID string) {
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

	return rootStore.Id, rootStoreModelID, testStore.Id, testStoreModelID
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
		rootStoreID, rootModelID, testStoreID, testModelID := setupAuthzModelAndTuples(t, openfga, clientID)

		_, err := openfga.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId:              rootStoreID,
			AuthorizationModelId: rootModelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey(fmt.Sprintf("store:%s", testStoreID), authz.CanCallGetStore, fmt.Sprintf("application:%s", clientID)),
				},
			},
		})
		require.NoError(t, err)

		_, err = openfga.ListObjects(context.Background(), &openfgav1.ListObjectsRequest{
			StoreId:              testStoreID,
			AuthorizationModelId: testModelID,
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
		rootStoreID, rootModelID, testStoreID, testModelID := setupAuthzModelAndTuples(t, openfga, clientID)
		_, err := openfga.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId:              testStoreID,
			AuthorizationModelId: testModelID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("workspace:1", "guest", "user:ben"),
				},
			},
		})
		require.NoError(t, err)

		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: rootStoreID, ModelID: rootModelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.ListObjects(ctx, &openfgav1.ListObjectsRequest{
				StoreId:              testStoreID,
				AuthorizationModelId: testModelID,
				Type:                 "workspace",
				Relation:             "guest",
				User:                 "user:ben",
			})
			require.Error(t, err)
			require.Equal(t, "rpc error: code = PermissionDenied desc = permission denied", err.Error())
		})

		t.Run("successfully_call_list_objects", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              rootStoreID,
				AuthorizationModelId: rootModelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey(fmt.Sprintf("store:%s", testStoreID), authz.CanCallListObjects, fmt.Sprintf("application:%s", clientID)),
					},
				},
			})
			require.NoError(t, err)
			listObjectsResponse, err := openfga.ListObjects(ctx, &openfgav1.ListObjectsRequest{
				StoreId:              testStoreID,
				AuthorizationModelId: testModelID,
				Type:                 "workspace",
				Relation:             "guest",
				User:                 "user:ben",
			})
			require.NoError(t, err)

			require.Equal(t, listObjectsResponse.GetObjects(), []string([]string{"workspace:1"}))
		})
	})
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
		rootStoreID, rootModelID, testStoreID, _ := setupAuthzModelAndTuples(t, openfga, clientID)
		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: rootStoreID, ModelID: rootModelID}, openfga, openfga.logger)

		t.Run("with_SkipAuthzCheckFromContext_set", func(t *testing.T) {
			fmt.Printf("%v", openfga.authorizer)
			ctx := authz.ContextWithSkipAuthzCheck(context.Background(), true)

			err := openfga.CheckAuthz(ctx, testStoreID, authz.Check)
			require.NoError(t, err)
		})

		t.Run("error_with_no_client_id_found", func(t *testing.T) {
			err := openfga.CheckAuthz(context.Background(), testStoreID, authz.Check)
			require.Error(t, err)
			require.Equal(t, "rpc error: code = Internal desc = client ID not found in context", err.Error())
		})

		t.Run("error_with_empty_client_id", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: ""})
			err := openfga.CheckAuthz(ctx, testStoreID, authz.Check)
			require.Error(t, err)
			require.Equal(t, "rpc error: code = Internal desc = client ID not found in context", err.Error())
		})

		t.Run("error_when_authorized_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: "ID"})
			err := openfga.CheckAuthz(ctx, testStoreID, "invalid api method")
			require.Error(t, err)
			require.Equal(t, "unknown api method: invalid api method", err.Error())
		})

		t.Run("error_check_when_not_authorized", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			err := openfga.CheckAuthz(ctx, testStoreID, authz.Check)
			require.Error(t, err)
			require.Equal(t, "rpc error: code = PermissionDenied desc = permission denied", err.Error())
		})

		t.Run("authz_is_valid", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              rootStoreID,
				AuthorizationModelId: rootModelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey(fmt.Sprintf("store:%s", testStoreID), authz.CanCallCheck, fmt.Sprintf("application:%s", clientID)),
					},
				},
			})
			require.NoError(t, err)

			err = openfga.CheckAuthz(ctx, testStoreID, authz.Check)
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

		rootStoreID, rootModelID, _, _ := setupAuthzModelAndTuples(t, openfga, clientID)

		checkResponse, err := openfga.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId:              rootStoreID,
			AuthorizationModelId: rootModelID, // optional, but recommended for speed
			TupleKey: &openfgav1.CheckRequestTupleKey{
				User:     fmt.Sprintf("application:%s", clientID),
				Relation: "admin",
				Object:   fmt.Sprintf("store:%s", rootStoreID),
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
		rootStoreID, rootModelID, testStoreID, testModelID := setupAuthzModelAndTuples(t, openfga, clientID)
		openfga.authorizer = authz.NewAuthorizer(&authz.Config{StoreID: rootStoreID, ModelID: rootModelID}, openfga, openfga.logger)

		t.Run("error_when_CheckAuthz_errors", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Check(ctx, &openfgav1.CheckRequest{
				StoreId:              testStoreID,
				AuthorizationModelId: rootModelID,
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     fmt.Sprintf("application:%s", clientID),
					Relation: "reader",
					Object:   fmt.Sprintf("store:%s", testStoreID),
				},
			})
			require.Error(t, err)
			require.Equal(t, "rpc error: code = PermissionDenied desc = permission denied", err.Error())
		})

		t.Run("successfully_call_check", func(t *testing.T) {
			ctx := authclaims.ContextWithAuthClaims(context.Background(), &authclaims.AuthClaims{ClientID: clientID})
			_, err := openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              rootStoreID,
				AuthorizationModelId: rootModelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey(fmt.Sprintf("store:%s", testStoreID), "writer", fmt.Sprintf("application:%s", clientID)),
					},
				},
			})
			require.NoError(t, err)
			_, err = openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              rootStoreID,
				AuthorizationModelId: rootModelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey(fmt.Sprintf("store:%s", testStoreID), authz.CanCallCheck, fmt.Sprintf("application:%s", clientID)),
					},
				},
			})
			require.NoError(t, err)
			_, err = openfga.Write(ctx, &openfgav1.WriteRequest{
				StoreId:              testStoreID,
				AuthorizationModelId: testModelID,
				Writes: &openfgav1.WriteRequestWrites{
					TupleKeys: []*openfgav1.TupleKey{
						tuple.NewTupleKey("workspace:1", "guest", "user:ben"),
					},
				},
			})
			require.NoError(t, err)

			checkResponse, err := openfga.Check(ctx, &openfgav1.CheckRequest{
				StoreId:              testStoreID,
				AuthorizationModelId: testModelID,
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

// t.Run("list_objects_valid_check_authz", func(t *testing.T) {
// 	storeID := ulid.Make().String()

// 	writeAuthzModelResp, err := openfga.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
// 		StoreId: storeID,
// 		TypeDefinitions: language.MustTransformDSLToProto(`
// 		model
// 			schema 1.1

// 		type user

// 		type document
// 			relations
// 				define editor: [user]`).GetTypeDefinitions(),
// 		SchemaVersion: typesystem.SchemaVersion1_1,
// 	})
// 	require.NoError(t, err)

// 	modelID := writeAuthzModelResp.GetAuthorizationModelId()

// 	numTuples := 10
// 	tuples := make([]*openfgav1.TupleKey, 0, numTuples)
// 	for i := 0; i < numTuples; i++ {
// 		tk := tuple.NewTupleKey(fmt.Sprintf("document:%d", i), "editor", "user:jon")

// 		tuples = append(tuples, tk)
// 	}

// 	_, err = openfga.Write(context.Background(), &openfgav1.WriteRequest{
// 		StoreId:              storeID,
// 		AuthorizationModelId: modelID,
// 		Writes: &openfgav1.WriteRequestWrites{
// 			TupleKeys: tuples,
// 		},
// 	})
// 	require.NoError(t, err)

// 	_, err = openfga.ListObjects(context.Background(), &openfgav1.ListObjectsRequest{
// 		StoreId:              storeID,
// 		AuthorizationModelId: modelID,
// 		Type:                 "document",
// 		Relation:             "editor",
// 		User:                 "user:jon",
// 	})
// 	require.NoError(t, err)
// })
