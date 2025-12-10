package grpc

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	storagev1 "github.com/openfga/api/proto/storage/v1beta1"

	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/test"
)

const bufSize = 1024 * 1024

// TestGRPCDatastore runs the complete test suite against the gRPC datastore client.
// This ensures the gRPC datastore passes the same tests as all SQL datastores.
func TestGRPCDatastore(t *testing.T) {
	client, _, cleanup := SetupTestClientServer(t)
	defer cleanup()
	test.RunAllTests(t, client)
}

func TestClientMaxTypesPerAuthorizationModel(t *testing.T) {
	client, _, cleanup := SetupTestClientServer(t)
	defer cleanup()

	maxTypes := client.MaxTypesPerAuthorizationModel()
	require.Equal(t, storage.DefaultMaxTypesPerAuthorizationModel, maxTypes)
}

func TestNewClient_TLS_Error(t *testing.T) {
	config := ClientConfig{
		Addr:        "localhost:8080",
		TLSCertPath: "/path/to/cert",
		// Missing TLSKeyPath
	}

	client, err := NewClient(config)
	require.Error(t, err)
	require.ErrorContains(t, err, "both TLS certificate and key paths must be provided together")
	require.Nil(t, client)
}

// mockErrorDatastore is a datastore that returns specific errors for testing error conversion.
type mockErrorDatastore struct {
	storage.OpenFGADatastore
	errorToReturn error
}

func (m *mockErrorDatastore) IsReady(ctx context.Context) (storage.ReadinessStatus, error) {
	if m.errorToReturn != nil {
		return storage.ReadinessStatus{}, m.errorToReturn
	}
	return storage.ReadinessStatus{IsReady: true}, nil
}

func (m *mockErrorDatastore) Read(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadOptions) (storage.TupleIterator, error) {
	if m.errorToReturn != nil {
		return nil, m.errorToReturn
	}
	return &mockEmptyIterator{}, nil
}

func (m *mockErrorDatastore) ReadPage(ctx context.Context, store string, filter storage.ReadFilter, options storage.ReadPageOptions) ([]*openfgav1.Tuple, string, error) {
	if m.errorToReturn != nil {
		return nil, "", m.errorToReturn
	}
	return nil, "", nil
}

func (m *mockErrorDatastore) ReadUserTuple(ctx context.Context, store string, filter storage.ReadUserTupleFilter, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
	if m.errorToReturn != nil {
		return nil, m.errorToReturn
	}
	return nil, storage.ErrNotFound
}

func (m *mockErrorDatastore) ReadUsersetTuples(ctx context.Context, store string, filter storage.ReadUsersetTuplesFilter, options storage.ReadUsersetTuplesOptions) (storage.TupleIterator, error) {
	if m.errorToReturn != nil {
		return nil, m.errorToReturn
	}
	return &mockEmptyIterator{}, nil
}

func (m *mockErrorDatastore) ReadStartingWithUser(ctx context.Context, store string, filter storage.ReadStartingWithUserFilter, options storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
	if m.errorToReturn != nil {
		return nil, m.errorToReturn
	}
	return &mockEmptyIterator{}, nil
}

func (m *mockErrorDatastore) Write(ctx context.Context, store string, deletes storage.Deletes, writes storage.Writes, opts ...storage.TupleWriteOption) error {
	return m.errorToReturn
}

func (m *mockErrorDatastore) ReadAuthorizationModel(ctx context.Context, store string, id string) (*openfgav1.AuthorizationModel, error) {
	if m.errorToReturn != nil {
		return nil, m.errorToReturn
	}
	return nil, storage.ErrNotFound
}

func (m *mockErrorDatastore) ReadAuthorizationModels(ctx context.Context, store string, options storage.ReadAuthorizationModelsOptions) ([]*openfgav1.AuthorizationModel, string, error) {
	if m.errorToReturn != nil {
		return nil, "", m.errorToReturn
	}
	return nil, "", nil
}

func (m *mockErrorDatastore) FindLatestAuthorizationModel(ctx context.Context, store string) (*openfgav1.AuthorizationModel, error) {
	if m.errorToReturn != nil {
		return nil, m.errorToReturn
	}
	return nil, storage.ErrNotFound
}

func (m *mockErrorDatastore) WriteAuthorizationModel(ctx context.Context, store string, model *openfgav1.AuthorizationModel) error {
	return m.errorToReturn
}

func (m *mockErrorDatastore) CreateStore(ctx context.Context, store *openfgav1.Store) (*openfgav1.Store, error) {
	if m.errorToReturn != nil {
		return nil, m.errorToReturn
	}
	return store, nil
}

func (m *mockErrorDatastore) GetStore(ctx context.Context, id string) (*openfgav1.Store, error) {
	if m.errorToReturn != nil {
		return nil, m.errorToReturn
	}
	return nil, storage.ErrNotFound
}

func (m *mockErrorDatastore) ListStores(ctx context.Context, options storage.ListStoresOptions) ([]*openfgav1.Store, string, error) {
	if m.errorToReturn != nil {
		return nil, "", m.errorToReturn
	}
	return nil, "", nil
}

func (m *mockErrorDatastore) DeleteStore(ctx context.Context, id string) error {
	return m.errorToReturn
}

func (m *mockErrorDatastore) ReadChanges(ctx context.Context, store string, filter storage.ReadChangesFilter, options storage.ReadChangesOptions) ([]*openfgav1.TupleChange, string, error) {
	if m.errorToReturn != nil {
		return nil, "", m.errorToReturn
	}
	return nil, "", storage.ErrNotFound
}

func (m *mockErrorDatastore) WriteAssertions(ctx context.Context, store, modelID string, assertions []*openfgav1.Assertion) error {
	return m.errorToReturn
}

func (m *mockErrorDatastore) ReadAssertions(ctx context.Context, store, modelID string) ([]*openfgav1.Assertion, error) {
	if m.errorToReturn != nil {
		return nil, m.errorToReturn
	}
	return []*openfgav1.Assertion{}, nil
}

type mockEmptyIterator struct{}

func (m *mockEmptyIterator) Next(ctx context.Context) (*openfgav1.Tuple, error) {
	return nil, storage.ErrIteratorDone
}

func (m *mockEmptyIterator) Stop() {}

func (m *mockEmptyIterator) Head(ctx context.Context) (*openfgav1.Tuple, error) {
	return nil, storage.ErrIteratorDone
}

// setupTestClientServerWithErrorDatastore sets up a test client/server with a mock error datastore.
func setupTestClientServerWithErrorDatastore(t *testing.T, errorToReturn error) (*Client, func()) {
	// Create mock backend
	backend := &mockErrorDatastore{errorToReturn: errorToReturn}

	// Set up gRPC server with bufconn
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	storageServer := NewServer(backend)
	storagev1.RegisterStorageServiceServer(grpcServer, storageServer)

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			t.Logf("Server exited with error: %v", err)
		}
	}()

	// Create client
	bufDialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(bufDialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	client := &Client{
		conn:                   conn,
		client:                 storagev1.NewStorageServiceClient(conn),
		maxTuplesPerWriteField: storage.DefaultMaxTuplesPerWrite,
		maxTypesPerModelField:  storage.DefaultMaxTypesPerAuthorizationModel,
	}

	cleanup := func() {
		client.Close()
		grpcServer.Stop()
	}

	return client, cleanup
}

func TestClientErrorHandling(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name       string
		storeError error
	}{
		{
			name:       "ErrNotFound is preserved",
			storeError: storage.ErrNotFound,
		},
		{
			name:       "ErrInvalidWriteInput is preserved",
			storeError: storage.ErrInvalidWriteInput,
		},
		{
			name:       "ErrTransactionThrottled is preserved",
			storeError: storage.ErrTransactionThrottled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := setupTestClientServerWithErrorDatastore(t, tt.storeError)
			defer cleanup()

			t.Run("IsReady", func(t *testing.T) {
				_, err := client.IsReady(ctx)
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("ReadPage", func(t *testing.T) {
				_, _, err := client.ReadPage(ctx, "test-store", storage.ReadFilter{}, storage.ReadPageOptions{
					Pagination: storage.PaginationOptions{PageSize: 10},
				})
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("ReadUserTuple", func(t *testing.T) {
				_, err := client.ReadUserTuple(ctx, "test-store", storage.ReadUserTupleFilter{
					Object:   "doc:1",
					Relation: "viewer",
					User:     "user:anne",
				}, storage.ReadUserTupleOptions{})
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("Write", func(t *testing.T) {
				err := client.Write(ctx, "test-store", nil, []*openfgav1.TupleKey{
					{Object: "doc:1", Relation: "viewer", User: "user:anne"},
				})
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("ReadAuthorizationModel", func(t *testing.T) {
				_, err := client.ReadAuthorizationModel(ctx, "test-store", "model-id")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("ReadAuthorizationModels", func(t *testing.T) {
				_, _, err := client.ReadAuthorizationModels(ctx, "test-store", storage.ReadAuthorizationModelsOptions{
					Pagination: storage.PaginationOptions{PageSize: 10},
				})
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("FindLatestAuthorizationModel", func(t *testing.T) {
				_, err := client.FindLatestAuthorizationModel(ctx, "test-store")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("WriteAuthorizationModel", func(t *testing.T) {
				model := &openfgav1.AuthorizationModel{
					Id:            "test-model",
					SchemaVersion: "1.1",
					TypeDefinitions: []*openfgav1.TypeDefinition{
						{Type: "user"},
					},
				}
				err := client.WriteAuthorizationModel(ctx, "test-store", model)
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("CreateStore", func(t *testing.T) {
				_, err := client.CreateStore(ctx, &openfgav1.Store{
					Id:   "test-store",
					Name: "Test Store",
				})
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("GetStore", func(t *testing.T) {
				_, err := client.GetStore(ctx, "test-store")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("ListStores", func(t *testing.T) {
				_, _, err := client.ListStores(ctx, storage.ListStoresOptions{
					Pagination: storage.PaginationOptions{PageSize: 10},
				})
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("DeleteStore", func(t *testing.T) {
				err := client.DeleteStore(ctx, "test-store")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("ReadChanges", func(t *testing.T) {
				_, _, err := client.ReadChanges(ctx, "test-store", storage.ReadChangesFilter{}, storage.ReadChangesOptions{
					Pagination: storage.PaginationOptions{PageSize: 10},
				})
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("WriteAssertions", func(t *testing.T) {
				err := client.WriteAssertions(ctx, "test-store", "test-model", []*openfgav1.Assertion{})
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})

			t.Run("ReadAssertions", func(t *testing.T) {
				_, err := client.ReadAssertions(ctx, "test-store", "test-model")
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})
		})
	}
}
