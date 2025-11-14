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

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
	"github.com/openfga/openfga/pkg/storage/memory"
)

const bufSize = 1024 * 1024

// setupTestClientServer sets up a test gRPC server and client using an in-memory backend.
func setupTestClientServer(t *testing.T) (*Client, storage.OpenFGADatastore, func()) {
	// Create in-memory backend
	backend := memory.New()

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

	return client, backend, cleanup
}

func TestClientIsReady(t *testing.T) {
	client, _, cleanup := setupTestClientServer(t)
	defer cleanup()

	status, err := client.IsReady(context.Background())
	require.NoError(t, err)
	require.True(t, status.IsReady)
}

func TestClientReadPage(t *testing.T) {
	client, _, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()

	// Read from empty store - should return empty results
	tuples, token, err := client.ReadPage(ctx, "test-store", storage.ReadFilter{}, storage.ReadPageOptions{
		Pagination: storage.PaginationOptions{
			PageSize: 10,
		},
	})
	require.NoError(t, err)
	require.Empty(t, tuples)
	require.Empty(t, token)
}

func TestClientReadUserTupleNotFound(t *testing.T) {
	client, _, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()

	tupleKey := &openfgav1.TupleKey{
		Object:   "doc:1",
		Relation: "viewer",
		User:     "user:anne",
	}

	tuple, err := client.ReadUserTuple(ctx, "test-store", tupleKey, storage.ReadUserTupleOptions{})
	require.Error(t, err)
	require.Equal(t, storage.ErrNotFound, err)
	require.Nil(t, tuple)
}

func TestClientRead(t *testing.T) {
	client, _, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()

	// Read from empty store - iterator should complete immediately
	iter, err := client.Read(ctx, "test-store", storage.ReadFilter{}, storage.ReadOptions{})
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Stop()

	// Should get ErrIteratorDone immediately
	tuple, err := iter.Next(ctx)
	require.Error(t, err)
	require.Equal(t, storage.ErrIteratorDone, err)
	require.Nil(t, tuple)
}

func TestClientReadUsersetTuples(t *testing.T) {
	client, _, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()

	filter := storage.ReadUsersetTuplesFilter{
		Object:   "doc:1",
		Relation: "viewer",
	}

	iter, err := client.ReadUsersetTuples(ctx, "test-store", filter, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Stop()

	// Should get ErrIteratorDone for empty store
	tuple, err := iter.Next(ctx)
	require.Error(t, err)
	require.Equal(t, storage.ErrIteratorDone, err)
	require.Nil(t, tuple)
}

func TestClientReadStartingWithUser(t *testing.T) {
	client, _, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()

	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		ObjectIDs:  storage.NewSortedSet(),
	}

	iter, err := client.ReadStartingWithUser(ctx, "test-store", filter, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Stop()

	// Should get ErrIteratorDone for empty store
	tuple, err := iter.Next(ctx)
	require.Error(t, err)
	require.Equal(t, storage.ErrIteratorDone, err)
	require.Nil(t, tuple)
}

func TestClientMaxTuplesPerWrite(t *testing.T) {
	client, _, cleanup := setupTestClientServer(t)
	defer cleanup()

	maxTuples := client.MaxTuplesPerWrite()
	require.Equal(t, storage.DefaultMaxTuplesPerWrite, maxTuples)
}

func TestClientMaxTypesPerAuthorizationModel(t *testing.T) {
	client, _, cleanup := setupTestClientServer(t)
	defer cleanup()

	maxTypes := client.MaxTypesPerAuthorizationModel()
	require.Equal(t, storage.DefaultMaxTypesPerAuthorizationModel, maxTypes)
}

func TestClientReadWithData(t *testing.T) {
	client, backend, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	// Write some test data using the backend directly
	err := backend.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
		{Object: "doc:1", Relation: "viewer", User: "user:anne"},
		{Object: "doc:2", Relation: "viewer", User: "user:bob"},
		{Object: "doc:1", Relation: "editor", User: "user:charlie"},
	})
	require.NoError(t, err)

	t.Run("read_all", func(t *testing.T) {
		iter, err := client.Read(ctx, storeID, storage.ReadFilter{}, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		tuples := []*openfgav1.Tuple{}
		for {
			tuple, err := iter.Next(ctx)
			if err == storage.ErrIteratorDone {
				break
			}
			require.NoError(t, err)
			tuples = append(tuples, tuple)
		}

		require.Len(t, tuples, 3)
	})

	t.Run("read_with_filter", func(t *testing.T) {
		iter, err := client.Read(ctx, storeID, storage.ReadFilter{
			Object: "doc:1",
		}, storage.ReadOptions{})
		require.NoError(t, err)
		defer iter.Stop()

		tuples := []*openfgav1.Tuple{}
		for {
			tuple, err := iter.Next(ctx)
			if err == storage.ErrIteratorDone {
				break
			}
			require.NoError(t, err)
			tuples = append(tuples, tuple)
		}

		require.Len(t, tuples, 2)
		for _, tuple := range tuples {
			require.Equal(t, "doc:1", tuple.GetKey().GetObject())
		}
	})
}

func TestClientReadPageWithData(t *testing.T) {
	client, backend, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	// Write test data
	err := backend.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
		{Object: "doc:1", Relation: "viewer", User: "user:anne"},
		{Object: "doc:2", Relation: "viewer", User: "user:bob"},
		{Object: "doc:3", Relation: "viewer", User: "user:charlie"},
	})
	require.NoError(t, err)

	// Read with pagination
	tuples, token, err := client.ReadPage(ctx, storeID, storage.ReadFilter{}, storage.ReadPageOptions{
		Pagination: storage.PaginationOptions{
			PageSize: 2,
		},
	})
	require.NoError(t, err)
	require.Len(t, tuples, 2)
	require.NotEmpty(t, token)

	// Read next page
	tuples2, token2, err := client.ReadPage(ctx, storeID, storage.ReadFilter{}, storage.ReadPageOptions{
		Pagination: storage.PaginationOptions{
			PageSize: 2,
			From:     token,
		},
	})
	require.NoError(t, err)
	require.Len(t, tuples2, 1)
	require.Empty(t, token2)
}

func TestClientReadUserTupleWithData(t *testing.T) {
	client, backend, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	// Write test data
	tupleKey := &openfgav1.TupleKey{
		Object:   "doc:1",
		Relation: "viewer",
		User:     "user:anne",
	}
	err := backend.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tupleKey})
	require.NoError(t, err)

	// Read the tuple back
	tuple, err := client.ReadUserTuple(ctx, storeID, tupleKey, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.NotNil(t, tuple)
	require.Equal(t, tupleKey.GetObject(), tuple.GetKey().GetObject())
	require.Equal(t, tupleKey.GetRelation(), tuple.GetKey().GetRelation())
	require.Equal(t, tupleKey.GetUser(), tuple.GetKey().GetUser())
}

func TestClientReadUsersetTuplesWithData(t *testing.T) {
	client, backend, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	// Write test data with userset tuples
	err := backend.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
		{Object: "doc:1", Relation: "viewer", User: "group:eng#member"},
		{Object: "doc:1", Relation: "viewer", User: "user:anne"},
		{Object: "doc:2", Relation: "viewer", User: "group:sales#member"},
	})
	require.NoError(t, err)

	filter := storage.ReadUsersetTuplesFilter{
		Object:   "doc:1",
		Relation: "viewer",
	}

	iter, err := client.ReadUsersetTuples(ctx, storeID, filter, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)
	defer iter.Stop()

	tuples := []*openfgav1.Tuple{}
	for {
		tuple, err := iter.Next(ctx)
		if err == storage.ErrIteratorDone {
			break
		}
		require.NoError(t, err)
		tuples = append(tuples, tuple)
	}

	// Should get only the userset tuple
	require.Len(t, tuples, 1)
	require.Equal(t, "group:eng#member", tuples[0].GetKey().GetUser())
}

func TestClientReadStartingWithUserWithData(t *testing.T) {
	client, backend, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	// Write test data
	err := backend.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
		{Object: "document:1", Relation: "viewer", User: "user:anne"},
		{Object: "document:2", Relation: "viewer", User: "user:anne"},
		{Object: "document:3", Relation: "viewer", User: "user:bob"},
	})
	require.NoError(t, err)

	objectIDs := storage.NewSortedSet()
	objectIDs.Add("1")
	objectIDs.Add("2")

	filter := storage.ReadStartingWithUserFilter{
		ObjectType: "document",
		Relation:   "viewer",
		UserFilter: []*openfgav1.ObjectRelation{{Object: "user:anne"}},
		ObjectIDs:  objectIDs,
	}

	iter, err := client.ReadStartingWithUser(ctx, storeID, filter, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
	defer iter.Stop()

	tuples := []*openfgav1.Tuple{}
	for {
		tuple, err := iter.Next(ctx)
		if err == storage.ErrIteratorDone {
			break
		}
		require.NoError(t, err)
		tuples = append(tuples, tuple)
	}

	require.Len(t, tuples, 2)
	for _, tuple := range tuples {
		require.Equal(t, "user:anne", tuple.GetKey().GetUser())
	}
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

func (m *mockErrorDatastore) ReadUserTuple(ctx context.Context, store string, tupleKey *openfgav1.TupleKey, options storage.ReadUserTupleOptions) (*openfgav1.Tuple, error) {
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

			// Test ReadUserTuple
			t.Run("ReadUserTuple", func(t *testing.T) {
				_, err := client.ReadUserTuple(ctx, "test-store", &openfgav1.TupleKey{
					Object:   "doc:1",
					Relation: "viewer",
					User:     "user:anne",
				}, storage.ReadUserTupleOptions{})
				require.Error(t, err)
				require.ErrorIs(t, err, tt.storeError)
			})
		})
	}
}
