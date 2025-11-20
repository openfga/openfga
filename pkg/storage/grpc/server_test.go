package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage"
	storagev1 "github.com/openfga/openfga/pkg/storage/grpc/proto/storage/v1"
	"github.com/openfga/openfga/pkg/storage/memory"
)

func TestNewServer(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	require.NotNil(t, server)
}

func TestServerIsReady(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)

	resp, err := server.IsReady(context.Background(), &storagev1.IsReadyRequest{})
	require.NoError(t, err)
	require.True(t, resp.GetIsReady())
}

func TestServerReadUserTupleNotFound(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)

	req := &storagev1.ReadUserTupleRequest{
		Store: "nonexistent-store",
		Filter: &storagev1.ReadFilter{
			Object:   "doc:1",
			Relation: "viewer",
			User:     "user:anne",
		},
		Consistency: &storagev1.ConsistencyOptions{},
	}

	_, err := server.ReadUserTuple(context.Background(), req)
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.NotFound, st.Code())
}

// mockStreamServer implements the streaming server interface for testing.
type mockReadStreamServer struct {
	results []*storagev1.ReadResponse
	ctx     context.Context
}

func (m *mockReadStreamServer) Send(resp *storagev1.ReadResponse) error {
	m.results = append(m.results, resp)
	return nil
}

func (m *mockReadStreamServer) Context() context.Context {
	return m.ctx
}

func (m *mockReadStreamServer) SendMsg(msg interface{}) error   { return nil }
func (m *mockReadStreamServer) RecvMsg(msg interface{}) error   { return nil }
func (m *mockReadStreamServer) SetHeader(md metadata.MD) error  { return nil }
func (m *mockReadStreamServer) SendHeader(md metadata.MD) error { return nil }
func (m *mockReadStreamServer) SetTrailer(md metadata.MD)       {}

func TestServerReadWithData(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"

	// Write test data
	err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
		{Object: "doc:1", Relation: "viewer", User: "user:anne"},
		{Object: "doc:2", Relation: "viewer", User: "user:bob"},
		{Object: "doc:1", Relation: "editor", User: "user:charlie"},
	})
	require.NoError(t, err)

	t.Run("read_all", func(t *testing.T) {
		stream := &mockReadStreamServer{ctx: ctx}
		req := &storagev1.ReadRequest{
			Store:       storeID,
			Filter:      &storagev1.ReadFilter{},
			Consistency: &storagev1.ConsistencyOptions{},
		}

		err := server.Read(req, stream)
		require.NoError(t, err)
		require.Len(t, stream.results, 3)
	})

	t.Run("read_with_filter", func(t *testing.T) {
		stream := &mockReadStreamServer{ctx: ctx}
		req := &storagev1.ReadRequest{
			Store: storeID,
			Filter: &storagev1.ReadFilter{
				Object: "doc:1",
			},
			Consistency: &storagev1.ConsistencyOptions{},
		}

		err := server.Read(req, stream)
		require.NoError(t, err)
		require.Len(t, stream.results, 2)
		for _, result := range stream.results {
			require.Equal(t, "doc:1", result.GetTuple().GetKey().GetObject())
		}
	})
}

func TestServerReadPageWithData(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"

	// Write test data
	err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
		{Object: "doc:1", Relation: "viewer", User: "user:anne"},
		{Object: "doc:2", Relation: "viewer", User: "user:bob"},
		{Object: "doc:3", Relation: "viewer", User: "user:charlie"},
	})
	require.NoError(t, err)

	// Read with pagination
	req := &storagev1.ReadPageRequest{
		Store:  storeID,
		Filter: &storagev1.ReadFilter{},
		Pagination: &storagev1.PaginationOptions{
			PageSize: 2,
		},
		Consistency: &storagev1.ConsistencyOptions{},
	}

	resp, err := server.ReadPage(ctx, req)
	require.NoError(t, err)
	require.Len(t, resp.GetTuples(), 2)
	require.NotEmpty(t, resp.GetContinuationToken())

	// Read next page
	req2 := &storagev1.ReadPageRequest{
		Store:  storeID,
		Filter: &storagev1.ReadFilter{},
		Pagination: &storagev1.PaginationOptions{
			PageSize: 2,
			From:     resp.GetContinuationToken(),
		},
		Consistency: &storagev1.ConsistencyOptions{},
	}

	resp2, err := server.ReadPage(ctx, req2)
	require.NoError(t, err)
	require.Len(t, resp2.GetTuples(), 1)
	require.Empty(t, resp2.GetContinuationToken())
}

func TestServerReadUserTupleWithData(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"

	// Write test data
	tupleKey := &openfgav1.TupleKey{
		Object:   "doc:1",
		Relation: "viewer",
		User:     "user:anne",
	}
	err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{tupleKey})
	require.NoError(t, err)

	// Read the tuple back
	req := &storagev1.ReadUserTupleRequest{
		Store: storeID,
		Filter: &storagev1.ReadFilter{
			Object:   tupleKey.GetObject(),
			Relation: tupleKey.GetRelation(),
			User:     tupleKey.GetUser(),
		},
		Consistency: &storagev1.ConsistencyOptions{},
	}

	resp, err := server.ReadUserTuple(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp.GetTuple())
	require.Equal(t, tupleKey.GetObject(), resp.GetTuple().GetKey().GetObject())
	require.Equal(t, tupleKey.GetRelation(), resp.GetTuple().GetKey().GetRelation())
	require.Equal(t, tupleKey.GetUser(), resp.GetTuple().GetKey().GetUser())
}

func TestServerReadUsersetTuplesWithData(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"

	// Write test data with userset tuples
	err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
		{Object: "doc:1", Relation: "viewer", User: "group:eng#member"},
		{Object: "doc:1", Relation: "viewer", User: "user:anne"},
		{Object: "doc:2", Relation: "viewer", User: "group:sales#member"},
	})
	require.NoError(t, err)

	stream := &mockReadStreamServer{ctx: ctx}
	req := &storagev1.ReadUsersetTuplesRequest{
		Store: storeID,
		Filter: &storagev1.ReadUsersetTuplesFilter{
			Object:   "doc:1",
			Relation: "viewer",
		},
		Consistency: &storagev1.ConsistencyOptions{},
	}

	err = server.ReadUsersetTuples(req, stream)
	require.NoError(t, err)

	// Should get only the userset tuple
	require.Len(t, stream.results, 1)
	require.Equal(t, "group:eng#member", stream.results[0].GetTuple().GetKey().GetUser())
}

func TestServerReadStartingWithUserWithData(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"

	// Write test data
	err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
		{Object: "document:1", Relation: "viewer", User: "user:anne"},
		{Object: "document:2", Relation: "viewer", User: "user:anne"},
		{Object: "document:3", Relation: "viewer", User: "user:bob"},
	})
	require.NoError(t, err)

	stream := &mockReadStreamServer{ctx: ctx}
	req := &storagev1.ReadStartingWithUserRequest{
		Store: storeID,
		Filter: &storagev1.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "viewer",
			UserFilter: []*storagev1.ObjectRelation{{Object: "user:anne"}},
			ObjectIds:  []string{"1", "2"},
		},
		Consistency: &storagev1.ConsistencyOptions{},
	}

	err = server.ReadStartingWithUser(req, stream)
	require.NoError(t, err)

	require.Len(t, stream.results, 2)
	for _, result := range stream.results {
		require.Equal(t, "user:anne", result.GetTuple().GetKey().GetUser())
	}
}

func TestServerReadEmptyStore(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()

	t.Run("read_page_empty", func(t *testing.T) {
		req := &storagev1.ReadPageRequest{
			Store:  "empty-store",
			Filter: &storagev1.ReadFilter{},
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
			Consistency: &storagev1.ConsistencyOptions{},
		}

		resp, err := server.ReadPage(ctx, req)
		require.NoError(t, err)
		require.Empty(t, resp.GetTuples())
		require.Empty(t, resp.GetContinuationToken())
	})

	t.Run("read_stream_empty", func(t *testing.T) {
		stream := &mockReadStreamServer{ctx: ctx}
		req := &storagev1.ReadRequest{
			Store:       "empty-store",
			Filter:      &storagev1.ReadFilter{},
			Consistency: &storagev1.ConsistencyOptions{},
		}

		err := server.Read(req, stream)
		require.NoError(t, err)
		require.Empty(t, stream.results)
	})
}

func TestServerWrite(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"

	t.Run("write_tuples", func(t *testing.T) {
		req := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{Object: "doc:1", Relation: "viewer", User: "user:anne"},
				{Object: "doc:2", Relation: "editor", User: "user:bob"},
			},
		}

		resp, err := server.Write(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify correct tuples were written
		tuples, _, err := ds.ReadPage(ctx, storeID, storage.ReadFilter{}, storage.ReadPageOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Len(t, tuples, 2)

		// Verify first tuple
		tuple1, err := ds.ReadUserTuple(ctx, storeID, storage.ReadUserTupleFilter{
			Object: "doc:1", Relation: "viewer", User: "user:anne",
		}, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.Equal(t, "doc:1", tuple1.GetKey().GetObject())
		require.Equal(t, "viewer", tuple1.GetKey().GetRelation())
		require.Equal(t, "user:anne", tuple1.GetKey().GetUser())

		// Verify second tuple
		tuple2, err := ds.ReadUserTuple(ctx, storeID, storage.ReadUserTupleFilter{
			Object: "doc:2", Relation: "editor", User: "user:bob",
		}, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.Equal(t, "doc:2", tuple2.GetKey().GetObject())
		require.Equal(t, "editor", tuple2.GetKey().GetRelation())
		require.Equal(t, "user:bob", tuple2.GetKey().GetUser())
	})

	t.Run("delete_tuples", func(t *testing.T) {
		// Write a tuple first
		writeReq := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{Object: "doc:3", Relation: "viewer", User: "user:charlie"},
			},
		}
		_, err := server.Write(ctx, writeReq)
		require.NoError(t, err)

		// Delete it
		deleteReq := &storagev1.WriteRequest{
			Store: storeID,
			Deletes: []*storagev1.TupleKey{
				{Object: "doc:3", Relation: "viewer", User: "user:charlie"},
			},
		}
		resp, err := server.Write(ctx, deleteReq)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify it was deleted
		_, err = ds.ReadUserTuple(ctx, storeID, storage.ReadUserTupleFilter{
			Object: "doc:3", Relation: "viewer", User: "user:charlie",
		}, storage.ReadUserTupleOptions{})
		require.Error(t, err)
		require.Equal(t, storage.ErrNotFound, err)
	})

	t.Run("write_and_delete_together", func(t *testing.T) {
		// First write the viewer tuple
		writeReq := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{Object: "doc:4", Relation: "viewer", User: "user:dave"},
			},
		}
		_, err := server.Write(ctx, writeReq)
		require.NoError(t, err)

		// Write and delete in the same operation
		req := &storagev1.WriteRequest{
			Store: storeID,
			Deletes: []*storagev1.TupleKey{
				{Object: "doc:4", Relation: "viewer", User: "user:dave"},
			},
			Writes: []*storagev1.TupleKey{
				{Object: "doc:4", Relation: "editor", User: "user:dave"},
			},
		}
		resp, err := server.Write(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify the viewer relation was deleted
		_, err = ds.ReadUserTuple(ctx, storeID, storage.ReadUserTupleFilter{
			Object: "doc:4", Relation: "viewer", User: "user:dave",
		}, storage.ReadUserTupleOptions{})
		require.Error(t, err)

		// Verify the editor relation was written
		tuple, err := ds.ReadUserTuple(ctx, storeID, storage.ReadUserTupleFilter{
			Object: "doc:4", Relation: "editor", User: "user:dave",
		}, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.NotNil(t, tuple)
	})

	t.Run("write_with_duplicate_insert_ignore", func(t *testing.T) {
		// Write a tuple
		writeReq := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{Object: "doc:5", Relation: "viewer", User: "user:eve"},
			},
		}
		_, err := server.Write(ctx, writeReq)
		require.NoError(t, err)

		// Try to write it again with ignore duplicate option
		writeReqWithOpts := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{Object: "doc:5", Relation: "viewer", User: "user:eve"},
			},
			Options: &storagev1.TupleWriteOptions{
				OnDuplicateInsert: storagev1.OnDuplicateInsert_ON_DUPLICATE_INSERT_IGNORE,
			},
		}
		resp, err := server.Write(ctx, writeReqWithOpts)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("write_with_duplicate_insert_error", func(t *testing.T) {
		// Write a tuple
		writeReq := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{Object: "doc:duplicate-server", Relation: "viewer", User: "user:test"},
			},
		}
		_, err := server.Write(ctx, writeReq)
		require.NoError(t, err)

		// Try to write it again with explicit error option - should fail
		writeReqWithOpts := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{Object: "doc:duplicate-server", Relation: "viewer", User: "user:test"},
			},
			Options: &storagev1.TupleWriteOptions{
				OnDuplicateInsert: storagev1.OnDuplicateInsert_ON_DUPLICATE_INSERT_ERROR,
			},
		}
		_, err = server.Write(ctx, writeReqWithOpts)
		require.Error(t, err)
		// The error should be converted to gRPC error with InvalidArgument code
		require.Contains(t, err.Error(), "cannot write a tuple which already exists")
	})

	t.Run("write_with_duplicate_insert_error_default", func(t *testing.T) {
		// Write a tuple
		writeReq := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{Object: "doc:duplicate-default-server", Relation: "viewer", User: "user:test"},
			},
		}
		_, err := server.Write(ctx, writeReq)
		require.NoError(t, err)

		// Try to write it again without options - should fail (default is ERROR)
		writeReqNoOpts := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{Object: "doc:duplicate-default-server", Relation: "viewer", User: "user:test"},
			},
		}
		_, err = server.Write(ctx, writeReqNoOpts)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot write a tuple which already exists")
	})

	t.Run("write_with_missing_delete_ignore", func(t *testing.T) {
		// Try to delete a tuple that doesn't exist with ignore option
		req := &storagev1.WriteRequest{
			Store: storeID,
			Deletes: []*storagev1.TupleKey{
				{Object: "doc:nonexistent", Relation: "viewer", User: "user:nobody"},
			},
			Options: &storagev1.TupleWriteOptions{
				OnMissingDelete: storagev1.OnMissingDelete_ON_MISSING_DELETE_IGNORE,
			},
		}
		resp, err := server.Write(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("write_with_missing_delete_error", func(t *testing.T) {
		// Try to delete a tuple that doesn't exist with explicit error option - should fail
		req := &storagev1.WriteRequest{
			Store: storeID,
			Deletes: []*storagev1.TupleKey{
				{Object: "doc:nonexistent-error-server", Relation: "viewer", User: "user:nobody"},
			},
			Options: &storagev1.TupleWriteOptions{
				OnMissingDelete: storagev1.OnMissingDelete_ON_MISSING_DELETE_ERROR,
			},
		}
		_, err := server.Write(ctx, req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot delete a tuple which does not exist")
	})

	t.Run("write_with_missing_delete_error_default", func(t *testing.T) {
		// Try to delete a tuple that doesn't exist without options - should fail (default is ERROR)
		req := &storagev1.WriteRequest{
			Store: storeID,
			Deletes: []*storagev1.TupleKey{
				{Object: "doc:nonexistent-default-server", Relation: "viewer", User: "user:nobody"},
			},
		}
		_, err := server.Write(ctx, req)
		require.Error(t, err)
		require.Contains(t, err.Error(), "cannot delete a tuple which does not exist")
	})

	t.Run("write_tuples_with_conditions", func(t *testing.T) {
		req := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{
					Object:   "doc:6",
					Relation: "viewer",
					User:     "user:conditional",
					Condition: &storagev1.RelationshipCondition{
						Name: "is_valid",
					},
				},
			},
		}
		resp, err := server.Write(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify tuple with condition was written
		tuple, err := ds.ReadUserTuple(ctx, storeID, storage.ReadUserTupleFilter{
			Object:   "doc:6",
			Relation: "viewer",
			User:     "user:conditional",
		}, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.NotNil(t, tuple.GetKey().GetCondition())
		require.Equal(t, "is_valid", tuple.GetKey().GetCondition().GetName())
	})

	t.Run("delete_multiple_tuples", func(t *testing.T) {
		// Write multiple tuples
		writeReq := &storagev1.WriteRequest{
			Store: storeID,
			Writes: []*storagev1.TupleKey{
				{Object: "doc:7", Relation: "viewer", User: "user:multi"},
				{Object: "doc:8", Relation: "viewer", User: "user:multi"},
				{Object: "doc:9", Relation: "viewer", User: "user:multi"},
			},
		}
		_, err := server.Write(ctx, writeReq)
		require.NoError(t, err)

		// Delete all of them
		deleteReq := &storagev1.WriteRequest{
			Store: storeID,
			Deletes: []*storagev1.TupleKey{
				{Object: "doc:7", Relation: "viewer", User: "user:multi"},
				{Object: "doc:8", Relation: "viewer", User: "user:multi"},
				{Object: "doc:9", Relation: "viewer", User: "user:multi"},
			},
		}
		resp, err := server.Write(ctx, deleteReq)
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Verify all were deleted
		tuples, _, err := ds.ReadPage(ctx, storeID, storage.ReadFilter{
			User: "user:multi",
		}, storage.ReadPageOptions{
			Pagination: storage.PaginationOptions{PageSize: 10},
		})
		require.NoError(t, err)
		require.Empty(t, tuples)
	})

	t.Run("write_empty_arrays", func(t *testing.T) {
		// Should handle empty writes and deletes gracefully
		req := &storagev1.WriteRequest{
			Store: storeID,
		}
		resp, err := server.Write(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
	})
}

func TestServerReadAuthorizationModel(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"

	t.Run("not_found", func(t *testing.T) {
		req := &storagev1.ReadAuthorizationModelRequest{
			Store: storeID,
			Id:    "non-existent-id",
		}
		_, err := server.ReadAuthorizationModel(ctx, req)
		require.Error(t, err)
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

	err := ds.WriteAuthorizationModel(ctx, storeID, model)
	require.NoError(t, err)

	t.Run("read_existing", func(t *testing.T) {
		req := &storagev1.ReadAuthorizationModelRequest{
			Store: storeID,
			Id:    model.GetId(),
		}
		resp, err := server.ReadAuthorizationModel(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp.GetModel())
		require.Equal(t, model.GetId(), resp.GetModel().GetId())
		require.Equal(t, model.GetSchemaVersion(), resp.GetModel().GetSchemaVersion())
		require.Len(t, resp.GetModel().GetTypeDefinitions(), 2)
	})
}

func TestServerReadAuthorizationModels(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"

	t.Run("empty_store", func(t *testing.T) {
		req := &storagev1.ReadAuthorizationModelsRequest{
			Store: storeID,
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		resp, err := server.ReadAuthorizationModels(ctx, req)
		require.NoError(t, err)
		require.Empty(t, resp.GetModels())
		require.Empty(t, resp.GetContinuationToken())
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

	err := ds.WriteAuthorizationModel(ctx, storeID, model1)
	require.NoError(t, err)
	err = ds.WriteAuthorizationModel(ctx, storeID, model2)
	require.NoError(t, err)

	t.Run("read_all", func(t *testing.T) {
		req := &storagev1.ReadAuthorizationModelsRequest{
			Store: storeID,
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		resp, err := server.ReadAuthorizationModels(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetModels(), 2)
		require.Empty(t, resp.GetContinuationToken())
		// Models should be returned newest first
		require.Equal(t, model2.GetId(), resp.GetModels()[0].GetId())
		require.Equal(t, model1.GetId(), resp.GetModels()[1].GetId())
	})

	t.Run("pagination", func(t *testing.T) {
		// First page
		req := &storagev1.ReadAuthorizationModelsRequest{
			Store: storeID,
			Pagination: &storagev1.PaginationOptions{
				PageSize: 1,
			},
		}
		resp, err := server.ReadAuthorizationModels(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetModels(), 1)
		require.NotEmpty(t, resp.GetContinuationToken())

		// Second page
		req2 := &storagev1.ReadAuthorizationModelsRequest{
			Store: storeID,
			Pagination: &storagev1.PaginationOptions{
				PageSize: 1,
				From:     resp.GetContinuationToken(),
			},
		}
		resp2, err := server.ReadAuthorizationModels(ctx, req2)
		require.NoError(t, err)
		require.Len(t, resp2.GetModels(), 1)
		require.Empty(t, resp2.GetContinuationToken())
	})
}

func TestServerFindLatestAuthorizationModel(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"

	t.Run("not_found", func(t *testing.T) {
		req := &storagev1.FindLatestAuthorizationModelRequest{
			Store: storeID,
		}
		_, err := server.FindLatestAuthorizationModel(ctx, req)
		require.Error(t, err)
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

	err := ds.WriteAuthorizationModel(ctx, storeID, model1)
	require.NoError(t, err)
	err = ds.WriteAuthorizationModel(ctx, storeID, model2)
	require.NoError(t, err)

	t.Run("find_latest", func(t *testing.T) {
		req := &storagev1.FindLatestAuthorizationModelRequest{
			Store: storeID,
		}
		resp, err := server.FindLatestAuthorizationModel(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp.GetModel())
		require.Equal(t, model2.GetId(), resp.GetModel().GetId())
		require.Len(t, resp.GetModel().GetTypeDefinitions(), 2)
	})
}

func TestServerWriteAuthorizationModel(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
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

	req := &storagev1.WriteAuthorizationModelRequest{
		Store: storeID,
		Model: toStorageAuthorizationModel(model),
	}

	resp, err := server.WriteAuthorizationModel(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify it was written
	readModel, err := ds.ReadAuthorizationModel(ctx, storeID, model.GetId())
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

func TestServerCreateStore(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()

	store := &openfgav1.Store{
		Id:   "test-store-id",
		Name: "Test Store",
	}

	req := &storagev1.CreateStoreRequest{
		Store: toStorageStore(store),
	}

	resp, err := server.CreateStore(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp.GetStore())
	require.Equal(t, store.GetId(), resp.GetStore().GetId())
	require.Equal(t, store.GetName(), resp.GetStore().GetName())
	require.NotNil(t, resp.GetStore().GetCreatedAt())
	require.NotNil(t, resp.GetStore().GetUpdatedAt())

	// Verify it was created in the datastore
	retrievedStore, err := ds.GetStore(ctx, store.GetId())
	require.NoError(t, err)
	require.Equal(t, store.GetId(), retrievedStore.GetId())
	require.Equal(t, store.GetName(), retrievedStore.GetName())
}

func TestServerGetStore(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()

	t.Run("not_found", func(t *testing.T) {
		req := &storagev1.GetStoreRequest{
			Id: "non-existent-store",
		}
		_, err := server.GetStore(ctx, req)
		require.Error(t, err)
	})

	// Create a store directly in the datastore
	store := &openfgav1.Store{
		Id:   "existing-store",
		Name: "Existing Store",
	}
	_, err := ds.CreateStore(ctx, store)
	require.NoError(t, err)

	t.Run("get_existing", func(t *testing.T) {
		req := &storagev1.GetStoreRequest{
			Id: store.GetId(),
		}
		resp, err := server.GetStore(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp.GetStore())
		require.Equal(t, store.GetId(), resp.GetStore().GetId())
		require.Equal(t, store.GetName(), resp.GetStore().GetName())
	})
}

func TestServerListStores(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()

	t.Run("empty_list", func(t *testing.T) {
		req := &storagev1.ListStoresRequest{
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		resp, err := server.ListStores(ctx, req)
		require.NoError(t, err)
		require.Empty(t, resp.GetStores())
		require.Empty(t, resp.GetContinuationToken())
	})

	// Create multiple stores
	store1 := &openfgav1.Store{Id: "store-1", Name: "Store One"}
	store2 := &openfgav1.Store{Id: "store-2", Name: "Store Two"}
	store3 := &openfgav1.Store{Id: "store-3", Name: "Store Three"}

	_, err := ds.CreateStore(ctx, store1)
	require.NoError(t, err)
	_, err = ds.CreateStore(ctx, store2)
	require.NoError(t, err)
	_, err = ds.CreateStore(ctx, store3)
	require.NoError(t, err)

	t.Run("list_all", func(t *testing.T) {
		req := &storagev1.ListStoresRequest{
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		resp, err := server.ListStores(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetStores(), 3)
		require.Empty(t, resp.GetContinuationToken())
	})

	t.Run("pagination", func(t *testing.T) {
		// First page
		req := &storagev1.ListStoresRequest{
			Pagination: &storagev1.PaginationOptions{
				PageSize: 2,
			},
		}
		resp, err := server.ListStores(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetStores(), 2)
		require.NotEmpty(t, resp.GetContinuationToken())

		// Second page
		req2 := &storagev1.ListStoresRequest{
			Pagination: &storagev1.PaginationOptions{
				PageSize: 2,
				From:     resp.GetContinuationToken(),
			},
		}
		resp2, err := server.ListStores(ctx, req2)
		require.NoError(t, err)
		require.Len(t, resp2.GetStores(), 1)
		require.Empty(t, resp2.GetContinuationToken())
	})

	t.Run("filter_by_name", func(t *testing.T) {
		req := &storagev1.ListStoresRequest{
			Name: "Store Two",
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		resp, err := server.ListStores(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetStores(), 1)
		require.Equal(t, "store-2", resp.GetStores()[0].GetId())
		require.Empty(t, resp.GetContinuationToken())
	})

	t.Run("filter_by_ids", func(t *testing.T) {
		req := &storagev1.ListStoresRequest{
			Ids: []string{"store-1", "store-3"},
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		resp, err := server.ListStores(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetStores(), 2)
		require.Empty(t, resp.GetContinuationToken())
	})
}

func TestServerDeleteStore(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()

	// Create a store
	store := &openfgav1.Store{
		Id:   "store-to-delete",
		Name: "Store To Delete",
	}
	_, err := ds.CreateStore(ctx, store)
	require.NoError(t, err)

	// Delete it
	req := &storagev1.DeleteStoreRequest{
		Id: store.GetId(),
	}
	resp, err := server.DeleteStore(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify it was deleted
	_, err = ds.GetStore(ctx, store.GetId())
	require.Error(t, err)
	require.Equal(t, storage.ErrNotFound, err)
}

func TestServerReadChanges(t *testing.T) {
	t.Run("no_changes_returns_not_found", func(t *testing.T) {
		ds := memory.New()
		server := NewServer(ds)
		ctx := context.Background()
		storeID := "empty-store"

		req := &storagev1.ReadChangesRequest{
			Store:  storeID,
			Filter: &storagev1.ReadChangesFilter{},
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		_, err := server.ReadChanges(ctx, req)
		require.Error(t, err)
	})

	t.Run("read_all_changes", func(t *testing.T) {
		ds := memory.New()
		server := NewServer(ds)
		ctx := context.Background()
		storeID := "test-store-all"

		// Write some tuples to generate changes
		err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "folder:1", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		req := &storagev1.ReadChangesRequest{
			Store:  storeID,
			Filter: &storagev1.ReadChangesFilter{},
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		resp, err := server.ReadChanges(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetChanges(), 3)
		require.NotEmpty(t, resp.GetContinuationToken())
	})

	t.Run("filter_by_object_type", func(t *testing.T) {
		ds := memory.New()
		server := NewServer(ds)
		ctx := context.Background()
		storeID := "test-store-filter"

		// Write tuples with different object types
		err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "folder:1", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		req := &storagev1.ReadChangesRequest{
			Store: storeID,
			Filter: &storagev1.ReadChangesFilter{
				ObjectType: "document",
			},
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		resp, err := server.ReadChanges(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetChanges(), 2)
		require.NotEmpty(t, resp.GetContinuationToken())
		for _, change := range resp.GetChanges() {
			require.Contains(t, change.GetTupleKey().GetObject(), "document:")
		}
	})

	t.Run("pagination", func(t *testing.T) {
		ds := memory.New()
		server := NewServer(ds)
		ctx := context.Background()
		storeID := "test-store-pagination"

		// Write tuples to generate changes
		err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "document:3", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		// First page
		req := &storagev1.ReadChangesRequest{
			Store:  storeID,
			Filter: &storagev1.ReadChangesFilter{},
			Pagination: &storagev1.PaginationOptions{
				PageSize: 2,
			},
		}
		resp, err := server.ReadChanges(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetChanges(), 2)
		require.NotEmpty(t, resp.GetContinuationToken())

		// Second page
		req2 := &storagev1.ReadChangesRequest{
			Store:  storeID,
			Filter: &storagev1.ReadChangesFilter{},
			Pagination: &storagev1.PaginationOptions{
				PageSize: 2,
				From:     resp.GetContinuationToken(),
			},
		}
		resp2, err := server.ReadChanges(ctx, req2)
		require.NoError(t, err)
		require.Len(t, resp2.GetChanges(), 1)
		require.NotEmpty(t, resp2.GetContinuationToken())
	})

	t.Run("sort_descending", func(t *testing.T) {
		ds := memory.New()
		server := NewServer(ds)
		ctx := context.Background()
		storeID := "test-store-desc"

		// Write tuples to generate changes
		err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "document:3", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		req := &storagev1.ReadChangesRequest{
			Store:    storeID,
			Filter:   &storagev1.ReadChangesFilter{},
			SortDesc: true,
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		resp, err := server.ReadChanges(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetChanges(), 3)
		require.NotEmpty(t, resp.GetContinuationToken())
	})

	t.Run("includes_deletes", func(t *testing.T) {
		ds := memory.New()
		server := NewServer(ds)
		ctx := context.Background()
		storeID := "test-store-deletes"

		// Write tuples
		err := ds.Write(ctx, storeID, nil, []*openfgav1.TupleKey{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
			{Object: "document:2", Relation: "viewer", User: "user:bob"},
			{Object: "folder:1", Relation: "viewer", User: "user:charlie"},
		})
		require.NoError(t, err)

		// Delete a tuple to test delete operations
		err = ds.Write(ctx, storeID, []*openfgav1.TupleKeyWithoutCondition{
			{Object: "document:1", Relation: "viewer", User: "user:anne"},
		}, nil)
		require.NoError(t, err)

		req := &storagev1.ReadChangesRequest{
			Store:  storeID,
			Filter: &storagev1.ReadChangesFilter{},
			Pagination: &storagev1.PaginationOptions{
				PageSize: 10,
			},
		}
		resp, err := server.ReadChanges(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetChanges(), 4) // 3 writes + 1 delete
		require.NotEmpty(t, resp.GetContinuationToken())

		// Find the delete operation
		var foundDelete bool
		for _, change := range resp.GetChanges() {
			if change.GetOperation() == storagev1.TupleOperation_TUPLE_OPERATION_DELETE {
				foundDelete = true
				require.Equal(t, "document:1", change.GetTupleKey().GetObject())
				break
			}
		}
		require.True(t, foundDelete, "Should find delete operation")
	})
}

func TestServerWriteAssertions(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"
	modelID := "test-model"

	assertions := []*openfgav1.Assertion{
		{
			TupleKey: &openfgav1.AssertionTupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:anne",
			},
			Expectation: true,
		},
		{
			TupleKey: &openfgav1.AssertionTupleKey{
				Object:   "document:1",
				Relation: "editor",
				User:     "user:bob",
			},
			Expectation: false,
		},
	}

	req := &storagev1.WriteAssertionsRequest{
		Store:      storeID,
		ModelId:    modelID,
		Assertions: toStorageAssertions(assertions),
	}

	resp, err := server.WriteAssertions(ctx, req)
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify they were written
	readAssertions, err := ds.ReadAssertions(ctx, storeID, modelID)
	require.NoError(t, err)
	require.Len(t, readAssertions, 2)
}

func TestServerReadAssertions(t *testing.T) {
	ds := memory.New()
	server := NewServer(ds)
	ctx := context.Background()
	storeID := "test-store"
	modelID := "test-model"

	t.Run("empty_assertions", func(t *testing.T) {
		req := &storagev1.ReadAssertionsRequest{
			Store:   storeID,
			ModelId: modelID,
		}
		resp, err := server.ReadAssertions(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp.GetAssertions())
		require.Empty(t, resp.GetAssertions())
	})

	// Write assertions directly in the datastore
	assertions := []*openfgav1.Assertion{
		{
			TupleKey: &openfgav1.AssertionTupleKey{
				Object:   "document:1",
				Relation: "viewer",
				User:     "user:anne",
			},
			Expectation: true,
		},
		{
			TupleKey: &openfgav1.AssertionTupleKey{
				Object:   "document:2",
				Relation: "editor",
				User:     "user:bob",
			},
			Expectation: false,
		},
	}

	err := ds.WriteAssertions(ctx, storeID, modelID, assertions)
	require.NoError(t, err)

	t.Run("read_existing_assertions", func(t *testing.T) {
		req := &storagev1.ReadAssertionsRequest{
			Store:   storeID,
			ModelId: modelID,
		}
		resp, err := server.ReadAssertions(ctx, req)
		require.NoError(t, err)
		require.Len(t, resp.GetAssertions(), 2)

		// Verify first assertion
		require.Equal(t, "document:1", resp.GetAssertions()[0].GetTupleKey().GetObject())
		require.Equal(t, "viewer", resp.GetAssertions()[0].GetTupleKey().GetRelation())
		require.Equal(t, "user:anne", resp.GetAssertions()[0].GetTupleKey().GetUser())
		require.True(t, resp.GetAssertions()[0].GetExpectation())

		// Verify second assertion
		require.Equal(t, "document:2", resp.GetAssertions()[1].GetTupleKey().GetObject())
		require.Equal(t, "editor", resp.GetAssertions()[1].GetTupleKey().GetRelation())
		require.Equal(t, "user:bob", resp.GetAssertions()[1].GetTupleKey().GetUser())
		require.False(t, resp.GetAssertions()[1].GetExpectation())
	})
}
