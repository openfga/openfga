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
		TupleKey: &storagev1.TupleKey{
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
		Store:       storeID,
		TupleKey:    toStorageTupleKey(tupleKey),
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
		tuple1, err := ds.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
			Object: "doc:1", Relation: "viewer", User: "user:anne",
		}, storage.ReadUserTupleOptions{})
		require.NoError(t, err)
		require.Equal(t, "doc:1", tuple1.GetKey().GetObject())
		require.Equal(t, "viewer", tuple1.GetKey().GetRelation())
		require.Equal(t, "user:anne", tuple1.GetKey().GetUser())

		// Verify second tuple
		tuple2, err := ds.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
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
		_, err = ds.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
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
		_, err = ds.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
			Object: "doc:4", Relation: "viewer", User: "user:dave",
		}, storage.ReadUserTupleOptions{})
		require.Error(t, err)

		// Verify the editor relation was written
		tuple, err := ds.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
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
		tuple, err := ds.ReadUserTuple(ctx, storeID, &openfgav1.TupleKey{
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
