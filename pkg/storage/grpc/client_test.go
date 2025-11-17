package grpc

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
	"github.com/openfga/openfga/pkg/storage/memory"
)

const bufSize = 1024 * 1024

// setupTestClientServer sets up a test gRPC server and client using an in-memory datastore.
func setupTestClientServer(t *testing.T) (*Client, storage.OpenFGADatastore, func()) {
	// Create in-memory datastore
	datastore := memory.New()

	// Set up gRPC server with bufconn
	lis := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	storageServer := NewServer(datastore)
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

	return client, datastore, cleanup
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
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	// Write some test data using the datastore directly
	err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
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
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	// Write test data
	err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
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
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	// Write test data
	tupleKey := &openfgav1.TupleKey{
		Object:   "doc:1",
		Relation: "viewer",
		User:     "user:anne",
	}
	err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tupleKey})
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
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	// Write test data with userset tuples
	err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
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
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	// Write test data
	err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
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

func TestClientWrite(t *testing.T) {
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	t.Run("write_tuples", func(t *testing.T) {
		err := client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "doc:1", Relation: "viewer", User: "user:anne"},
			{Object: "doc:2", Relation: "editor", User: "user:bob"},
		})
		require.NoError(t, err)

		// Verify correct tuples were written
		tuples, _, err := datastore.ReadPage(ctx, storeID, storage.ReadFilter{}, storage.ReadPageOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Len(t, tuples, 2)

		// Verify first tuple
		tuple1, err := datastore.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
			Object: "doc:1", Relation: "viewer", User: "user:anne",
		}, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.Equal(t, "doc:1", tuple1.GetKey().GetObject())
		require.Equal(t, "viewer", tuple1.GetKey().GetRelation())
		require.Equal(t, "user:anne", tuple1.GetKey().GetUser())

		// Verify second tuple
		tuple2, err := datastore.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
			Object: "doc:2", Relation: "editor", User: "user:bob",
		}, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.Equal(t, "doc:2", tuple2.GetKey().GetObject())
		require.Equal(t, "editor", tuple2.GetKey().GetRelation())
		require.Equal(t, "user:bob", tuple2.GetKey().GetUser())
	})

	t.Run("delete_tuples", func(t *testing.T) {
		// Write a tuple first
		err := client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "doc:3", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		// Delete it
		err = client.Write(ctx, storeID, []*openfgav1.TupleKeyWithoutCondition{
			{Object: "doc:3", Relation: "viewer", User: "user:charlie"},
		}, nil)
		require.NoError(t, err)

		// Verify it was deleted
		_, err = datastore.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
			Object: "doc:3", Relation: "viewer", User: "user:charlie",
		}, storage.ReadUserTupleOptions{})
		require.Error(t, err)
		require.Equal(t, storage.ErrNotFound, err)
	})

	t.Run("write_and_delete_together", func(t *testing.T) {
		// Write and delete in the same operation
		err := client.Write(ctx, storeID,
			[]*openfgav1.TupleKeyWithoutCondition{
				{Object: "doc:1", Relation: "viewer", User: "user:anne"},
			},
			[]*openfgav1.TupleKey{
				{Object: "doc:1", Relation: "editor", User: "user:anne"},
			},
		)
		require.NoError(t, err)

		// Verify the viewer relation was deleted
		_, err = datastore.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
			Object: "doc:1", Relation: "viewer", User: "user:anne",
		}, storage.ReadUserTupleOptions{})
		require.Error(t, err)

		// Verify the editor relation was written
		tuple, err := datastore.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
			Object: "doc:1", Relation: "editor", User: "user:anne",
		}, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.NotNil(t, tuple)
	})

	t.Run("write_with_duplicate_insert_ignore", func(t *testing.T) {
		// Write a tuple
		err := client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "doc:4", Relation: "viewer", User: "user:dave"},
		})
		require.NoError(t, err)

		// Try to write it again with ignore duplicate option
		err = client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "doc:4", Relation: "viewer", User: "user:dave"},
		}, storage.WithOnDuplicateInsert(storage.OnDuplicateInsertIgnore))
		require.NoError(t, err)
	})

	t.Run("write_with_duplicate_insert_error", func(t *testing.T) {
		// Write a tuple
		err := client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "doc:duplicate", Relation: "viewer", User: "user:test"},
		})
		require.NoError(t, err)

		// Try to write it again with explicit error option - should fail
		err = client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "doc:duplicate", Relation: "viewer", User: "user:test"},
		}, storage.WithOnDuplicateInsert(storage.OnDuplicateInsertError))
		require.Error(t, err)
		require.ErrorIs(t, err, storage.ErrInvalidWriteInput)
	})

	t.Run("write_with_duplicate_insert_error_default", func(t *testing.T) {
		// Write a tuple
		err := client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "doc:duplicate-default", Relation: "viewer", User: "user:test"},
		})
		require.NoError(t, err)

		// Try to write it again without options - should fail (default is ERROR)
		err = client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "doc:duplicate-default", Relation: "viewer", User: "user:test"},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, storage.ErrInvalidWriteInput)
	})

	t.Run("write_with_missing_delete_ignore", func(t *testing.T) {
		// Try to delete a tuple that doesn't exist with ignore option
		err := client.Write(ctx, storeID, []*openfgav1.TupleKeyWithoutCondition{
			{Object: "doc:nonexistent", Relation: "viewer", User: "user:nobody"},
		}, nil, storage.WithOnMissingDelete(storage.OnMissingDeleteIgnore))
		require.NoError(t, err)
	})

	t.Run("write_with_missing_delete_error", func(t *testing.T) {
		// Try to delete a tuple that doesn't exist with explicit error option - should fail
		err := client.Write(ctx, storeID, []*openfgav1.TupleKeyWithoutCondition{
			{Object: "doc:nonexistent-error", Relation: "viewer", User: "user:nobody"},
		}, nil, storage.WithOnMissingDelete(storage.OnMissingDeleteError))
		require.Error(t, err)
		require.ErrorIs(t, err, storage.ErrInvalidWriteInput)
	})

	t.Run("write_with_missing_delete_error_default", func(t *testing.T) {
		// Try to delete a tuple that doesn't exist without options - should fail (default is ERROR)
		err := client.Write(ctx, storeID, []*openfgav1.TupleKeyWithoutCondition{
			{Object: "doc:nonexistent-default", Relation: "viewer", User: "user:nobody"},
		}, nil)
		require.Error(t, err)
		require.ErrorIs(t, err, storage.ErrInvalidWriteInput)
	})

	t.Run("write_tuples_with_conditions", func(t *testing.T) {
		err := client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{
				Object:   "doc:5",
				Relation: "viewer",
				User:     "user:conditional",
				Condition: &openfgav1.RelationshipCondition{
					Name: "is_valid",
				},
			},
		})
		require.NoError(t, err)

		// Verify tuple with condition was written
		tuple, err := datastore.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
			Object:   "doc:5",
			Relation: "viewer",
			User:     "user:conditional",
		}, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.NotNil(t, tuple.GetKey().GetCondition())
		require.Equal(t, "is_valid", tuple.GetKey().GetCondition().GetName())
	})

	t.Run("delete_multiple_tuples", func(t *testing.T) {
		// Write multiple tuples
		err := client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "doc:6", Relation: "viewer", User: "user:alice"},
			{Object: "doc:7", Relation: "viewer", User: "user:alice"},
			{Object: "doc:8", Relation: "viewer", User: "user:alice"},
		})
		require.NoError(t, err)

		// Delete all of them
		err = client.Write(ctx, storeID, []*openfgav1.TupleKeyWithoutCondition{
			{Object: "doc:6", Relation: "viewer", User: "user:alice"},
			{Object: "doc:7", Relation: "viewer", User: "user:alice"},
			{Object: "doc:8", Relation: "viewer", User: "user:alice"},
		}, nil)
		require.NoError(t, err)

		// Verify all were deleted
		tuples, _, err := datastore.ReadPage(ctx, storeID, storage.ReadFilter{
			User: "user:alice",
		}, storage.ReadPageOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Empty(t, tuples)
	})

	t.Run("write_empty_arrays", func(t *testing.T) {
		// Should handle empty writes and deletes gracefully
		err := client.Write(ctx, storeID, nil, nil)
		require.NoError(t, err)
	})
}

func TestClientWriteWithConditions(t *testing.T) {
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store-conditions"

	// Write tuple with complex condition
	tupleWithCondition := &openfgav1.TupleKey{
		Object:   "document:sensitive",
		Relation: "viewer",
		User:     "user:contractor",
		Condition: &openfgav1.RelationshipCondition{
			Name: "valid_ip_range",
			Context: &structpb.Struct{
				Fields: map[string]*structpb.Value{
					"allowed_ips": structpb.NewListValue(&structpb.ListValue{
						Values: []*structpb.Value{
							structpb.NewStringValue("10.0.0.0/8"),
							structpb.NewStringValue("192.168.0.0/16"),
						},
					}),
				},
			},
		},
	}

	err := client.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tupleWithCondition})
	require.NoError(t, err)

	// Read it back and verify condition was preserved
	tuple, err := datastore.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
		Object:   "document:sensitive",
		Relation: "viewer",
		User:     "user:contractor",
	}, storage.ReadUserTupleOptions{})
	require.NoError(t, err)
	require.NotNil(t, tuple)
	require.NotNil(t, tuple.GetKey().GetCondition())
	require.Equal(t, "valid_ip_range", tuple.GetKey().GetCondition().GetName())
	require.NotNil(t, tuple.GetKey().GetCondition().GetContext())

	// Verify condition context fields and values
	fields := tuple.GetKey().GetCondition().GetContext().GetFields()
	require.Contains(t, fields, "allowed_ips")

	allowedIps := fields["allowed_ips"].GetListValue()
	require.NotNil(t, allowedIps)
	require.Len(t, allowedIps.GetValues(), 2)
	require.Equal(t, "10.0.0.0/8", allowedIps.GetValues()[0].GetStringValue())
	require.Equal(t, "192.168.0.0/16", allowedIps.GetValues()[1].GetStringValue())
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

func TestClientReadAuthorizationModel(t *testing.T) {
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	t.Run("not_found", func(t *testing.T) {
		_, err := client.ReadAuthorizationModel(ctx, storeID, "non-existent-id")
		require.Error(t, err)
		require.Equal(t, storage.ErrNotFound, err)
	})

	// Write a model to the datastore
	model := &openfgav1.AuthorizationModel{
		Id:            "01HXQZ9F8G7YRTXMN50BQP6XQZ",
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {
						Userset: &openfgav1.Userset_This{},
					},
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

	err := datastore.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(t, err)

	t.Run("read_existing", func(t *testing.T) {
		readModel, err := client.ReadAuthorizationModel(ctx, storeID, model.GetId())
		require.NoError(t, err)
		require.NotNil(t, readModel)
		require.Equal(t, model.GetId(), readModel.GetId())
		require.Equal(t, model.GetSchemaVersion(), readModel.GetSchemaVersion())
		require.Len(t, readModel.GetTypeDefinitions(), 2)
	})
}

func TestClientReadAuthorizationModels(t *testing.T) {
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	t.Run("empty_store", func(t *testing.T) {
		models, token, err := client.ReadAuthorizationModels(ctx, storeID, storage.ReadAuthorizationModelsOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Empty(t, models)
		require.Empty(t, token)
	})

	// Write multiple models
	model1 := &openfgav1.AuthorizationModel{
		Id:            "01HXQZ9F8G7YRTXMN50BQP6XQZ",
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
		},
	}
	model2 := &openfgav1.AuthorizationModel{
		Id:            "01HXQZ9F8G7YRTXMN50BQP6XRA",
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{Type: "document"},
		},
	}

	err := datastore.WriteAuthorizationModel(ctx, storeID, model1)
	require.NoError(t, err)
	err = datastore.WriteAuthorizationModel(ctx, storeID, model2)
	require.NoError(t, err)

	t.Run("read_all", func(t *testing.T) {
		models, token, err := client.ReadAuthorizationModels(ctx, storeID, storage.ReadAuthorizationModelsOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Len(t, models, 2)
		require.Empty(t, token)
		// Models should be returned newest first
		require.Equal(t, model2.GetId(), models[0].GetId())
		require.Equal(t, model1.GetId(), models[1].GetId())
	})

	t.Run("pagination", func(t *testing.T) {
		// First page
		models, token, err := client.ReadAuthorizationModels(ctx, storeID, storage.ReadAuthorizationModelsOptions{
			Pagination: storage.PaginationOptions{PageSize: 1},
		})
		require.NoError(t, err)
		require.Len(t, models, 1)
		require.NotEmpty(t, token)

		// Second page
		models2, token2, err := client.ReadAuthorizationModels(ctx, storeID, storage.ReadAuthorizationModelsOptions{
			Pagination: storage.PaginationOptions{PageSize: 1, From: token},
		})
		require.NoError(t, err)
		require.Len(t, models2, 1)
		require.Empty(t, token2)
	})
}

func TestClientFindLatestAuthorizationModel(t *testing.T) {
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	t.Run("not_found", func(t *testing.T) {
		_, err := client.FindLatestAuthorizationModel(ctx, storeID)
		require.Error(t, err)
		require.Equal(t, storage.ErrNotFound, err)
	})

	// Write models - the second one should be the latest
	model1 := &openfgav1.AuthorizationModel{
		Id:            "01HXQZ9F8G7YRTXMN50BQP6XQZ",
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
		},
	}
	model2 := &openfgav1.AuthorizationModel{
		Id:            "01HXQZ9F8G7YRTXMN50BQP6XRA",
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{Type: "user"},
			{Type: "document"},
		},
	}

	err := datastore.WriteAuthorizationModel(ctx, storeID, model1)
	require.NoError(t, err)
	err = datastore.WriteAuthorizationModel(ctx, storeID, model2)
	require.NoError(t, err)

	t.Run("find_latest", func(t *testing.T) {
		latest, err := client.FindLatestAuthorizationModel(ctx, storeID)
		require.NoError(t, err)
		require.NotNil(t, latest)
		require.Equal(t, model2.GetId(), latest.GetId())
		require.Len(t, latest.GetTypeDefinitions(), 2)
	})
}

func TestClientWriteAuthorizationModel(t *testing.T) {
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()
	storeID := "test-store"

	model := &openfgav1.AuthorizationModel{
		Id:            "01HXQZ9F8G7YRTXMN50BQP6XQZ",
		SchemaVersion: "1.1",
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {
						Userset: &openfgav1.Userset_This{},
					},
					"editor": {
						Userset: &openfgav1.Userset_This{},
					},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
						"editor": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								{Type: "user"},
							},
						},
					},
				},
			},
		},
		Conditions: map[string]*openfgav1.Condition{
			"is_valid": {
				Name:       "is_valid",
				Expression: "param.x < 100",
				Parameters: map[string]*openfgav1.ConditionParamTypeRef{
					"x": {
						TypeName: openfgav1.ConditionParamTypeRef_TYPE_NAME_INT,
					},
				},
			},
		},
	}

	err := client.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(t, err)

	// Verify it was written
	readModel, err := datastore.ReadAuthorizationModel(ctx, storeID, model.GetId())
	require.NoError(t, err)
	require.NotNil(t, readModel)
	require.Equal(t, model.GetId(), readModel.GetId())
	require.Equal(t, model.GetSchemaVersion(), readModel.GetSchemaVersion())
	require.Len(t, readModel.GetTypeDefinitions(), 2)
	require.Len(t, readModel.GetConditions(), 1)

	// Verify document type details
	var docType *openfgav1.TypeDefinition
	for _, td := range readModel.GetTypeDefinitions() {
		if td.GetType() == "document" {
			docType = td
			break
		}
	}
	require.NotNil(t, docType)
	require.Len(t, docType.GetRelations(), 2)
	require.Contains(t, docType.GetRelations(), "viewer")
	require.Contains(t, docType.GetRelations(), "editor")

	// Verify condition details
	require.Contains(t, readModel.GetConditions(), "is_valid")
	condition := readModel.GetConditions()["is_valid"]
	require.Equal(t, "is_valid", condition.GetName())
	require.Equal(t, "param.x < 100", condition.GetExpression())
	require.Len(t, condition.GetParameters(), 1)
	require.Contains(t, condition.GetParameters(), "x")
}

func TestClientCreateStore(t *testing.T) {
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()

	store := &openfgav1.Store{
		Id:   "test-store-id",
		Name: "Test Store",
	}

	createdStore, err := client.CreateStore(ctx, store)
	require.NoError(t, err)
	require.NotNil(t, createdStore)
	require.Equal(t, store.GetId(), createdStore.GetId())
	require.Equal(t, store.GetName(), createdStore.GetName())
	require.NotNil(t, createdStore.GetCreatedAt())
	require.NotNil(t, createdStore.GetUpdatedAt())

	// Verify it was created in the datastore
	retrievedStore, err := datastore.GetStore(ctx, store.GetId())
	require.NoError(t, err)
	require.Equal(t, store.GetId(), retrievedStore.GetId())
	require.Equal(t, store.GetName(), retrievedStore.GetName())
}

func TestClientGetStore(t *testing.T) {
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("not_found", func(t *testing.T) {
		_, err := client.GetStore(ctx, "non-existent-store")
		require.Error(t, err)
		require.Equal(t, storage.ErrNotFound, err)
	})

	// Create a store directly in the datastore
	store := &openfgav1.Store{
		Id:   "existing-store",
		Name: "Existing Store",
	}
	_, err := datastore.CreateStore(ctx, store)
	require.NoError(t, err)

	t.Run("get_existing", func(t *testing.T) {
		retrievedStore, err := client.GetStore(ctx, store.GetId())
		require.NoError(t, err)
		require.NotNil(t, retrievedStore)
		require.Equal(t, store.GetId(), retrievedStore.GetId())
		require.Equal(t, store.GetName(), retrievedStore.GetName())
	})
}

func TestClientListStores(t *testing.T) {
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()

	t.Run("empty_list", func(t *testing.T) {
		stores, token, err := client.ListStores(ctx, storage.ListStoresOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Empty(t, stores)
		require.Empty(t, token)
	})

	// Create multiple stores
	store1 := &openfgav1.Store{Id: "store-1", Name: "Store One"}
	store2 := &openfgav1.Store{Id: "store-2", Name: "Store Two"}
	store3 := &openfgav1.Store{Id: "store-3", Name: "Store Three"}

	_, err := datastore.CreateStore(ctx, store1)
	require.NoError(t, err)
	_, err = datastore.CreateStore(ctx, store2)
	require.NoError(t, err)
	_, err = datastore.CreateStore(ctx, store3)
	require.NoError(t, err)

	t.Run("list_all", func(t *testing.T) {
		stores, token, err := client.ListStores(ctx, storage.ListStoresOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Len(t, stores, 3)
		require.Empty(t, token)
	})

	t.Run("pagination", func(t *testing.T) {
		// First page
		stores, token, err := client.ListStores(ctx, storage.ListStoresOptions{
			Pagination: storage.PaginationOptions{PageSize: 2},
		})
		require.NoError(t, err)
		require.Len(t, stores, 2)
		require.NotEmpty(t, token)

		// Second page
		stores2, token2, err := client.ListStores(ctx, storage.ListStoresOptions{
			Pagination: storage.PaginationOptions{PageSize: 2, From: token},
		})
		require.NoError(t, err)
		require.Len(t, stores2, 1)
		require.Empty(t, token2)
	})

	t.Run("filter_by_name", func(t *testing.T) {
		stores, token, err := client.ListStores(ctx, storage.ListStoresOptions{
			Name:       "Store Two",
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Len(t, stores, 1)
		require.Equal(t, "store-2", stores[0].GetId())
		require.Empty(t, token)
	})

	t.Run("filter_by_ids", func(t *testing.T) {
		stores, token, err := client.ListStores(ctx, storage.ListStoresOptions{
			IDs:        []string{"store-1", "store-3"},
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Len(t, stores, 2)
		require.Empty(t, token)
	})
}

func TestClientDeleteStore(t *testing.T) {
	client, datastore, cleanup := setupTestClientServer(t)
	defer cleanup()

	ctx := context.Background()

	// Create a store
	store := &openfgav1.Store{
		Id:   "store-to-delete",
		Name: "Store To Delete",
	}
	_, err := datastore.CreateStore(ctx, store)
	require.NoError(t, err)

	// Delete it
	err = client.DeleteStore(ctx, store.GetId())
	require.NoError(t, err)

	// Verify it was deleted
	_, err = datastore.GetStore(ctx, store.GetId())
	require.Error(t, err)
	require.Equal(t, storage.ErrNotFound, err)
}

func TestClientReadChanges(t *testing.T) {
	t.Run("no_changes_returns_not_found", func(t *testing.T) {
		client, datastore, cleanup := setupTestClientServer(t)
		defer cleanup()

		ctx := context.Background()
		storeID := "empty-store"

		_, _, err := client.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.Error(t, err)
		require.Equal(t, storage.ErrNotFound, err)

		// Verify datastore is empty
		_, _, err = datastore.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.Equal(t, storage.ErrNotFound, err)
	})

	t.Run("read_all_changes", func(t *testing.T) {
		client, datastore, cleanup := setupTestClientServer(t)
		defer cleanup()

		ctx := context.Background()
		storeID := "test-store-all"

		// Write some tuples to generate changes
		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "folder:1", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		changes, token, err := client.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Len(t, changes, 3)
		require.NotEmpty(t, token) // Always returns a continuation token
	})

	t.Run("filter_by_object_type", func(t *testing.T) {
		client, datastore, cleanup := setupTestClientServer(t)
		defer cleanup()

		ctx := context.Background()
		storeID := "test-store-filter"

		// Write tuples with different object types
		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "folder:1", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		changes, token, err := client.ReadChanges(ctx, storeID, storage.ReadChangesFilter{
			ObjectType: "document",
		}, storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Len(t, changes, 2)
		require.NotEmpty(t, token)
		for _, change := range changes {
			require.Contains(t, change.GetTupleKey().GetObject(), "document:")
		}
	})

	t.Run("pagination", func(t *testing.T) {
		client, datastore, cleanup := setupTestClientServer(t)
		defer cleanup()

		ctx := context.Background()
		storeID := "test-store-pagination"

		// Write tuples to generate changes
		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "document:3", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		// First page
		changes, token, err := client.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{PageSize: 2},
		})
		require.NoError(t, err)
		require.Len(t, changes, 2)
		require.NotEmpty(t, token)

		// Second page
		changes2, token2, err := client.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{PageSize: 2, From: token},
		})
		require.NoError(t, err)
		require.Len(t, changes2, 1)
		require.NotEmpty(t, token2)
	})

	t.Run("sort_descending", func(t *testing.T) {
		client, datastore, cleanup := setupTestClientServer(t)
		defer cleanup()

		ctx := context.Background()
		storeID := "test-store-desc"

		// Write tuples to generate changes
		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "document:3", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		changes, token, err := client.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
			SortDesc:   true,
		})
		require.NoError(t, err)
		require.Len(t, changes, 3)
		require.NotEmpty(t, token)
	})

	t.Run("includes_deletes", func(t *testing.T) {
		client, datastore, cleanup := setupTestClientServer(t)
		defer cleanup()

		ctx := context.Background()
		storeID := "test-store-deletes"

		// Write tuples
		err := datastore.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "folder:1", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		// Delete a tuple to test delete operations
		err = datastore.Write(ctx, storeID, []*openfgav1.TupleKeyWithoutCondition{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
		}, nil)
		require.NoError(t, err)

		changes, token, err := client.ReadChanges(ctx, storeID, storage.ReadChangesFilter{}, storage.ReadChangesOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Len(t, changes, 4) // 3 writes + 1 delete
		require.NotEmpty(t, token)

		// Find the delete operation
		var foundDelete bool
		for _, change := range changes {
			if change.GetOperation() == openfgav1.TupleOperation_TUPLE_OPERATION_DELETE {
				foundDelete = true
				require.Equal(t, "document:1", change.GetTupleKey().GetObject())
				break
			}
		}
		require.True(t, foundDelete, "Should find delete operation")
	})
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
				_, err := client.ReadUserTuple(ctx, "test-store", &openfgav1.TupleKey{
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
		})
	}
}
