package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

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
	require.True(t, resp.IsReady)
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

// mockStreamServer implements the streaming server interface for testing
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
			require.Equal(t, "doc:1", result.Tuple.Key.Object)
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
	require.Len(t, resp.Tuples, 2)
	require.NotEmpty(t, resp.ContinuationToken)

	// Read next page
	req2 := &storagev1.ReadPageRequest{
		Store:  storeID,
		Filter: &storagev1.ReadFilter{},
		Pagination: &storagev1.PaginationOptions{
			PageSize: 2,
			From:     resp.ContinuationToken,
		},
		Consistency: &storagev1.ConsistencyOptions{},
	}

	resp2, err := server.ReadPage(ctx, req2)
	require.NoError(t, err)
	require.Len(t, resp2.Tuples, 1)
	require.Empty(t, resp2.ContinuationToken)
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
	require.NotNil(t, resp.Tuple)
	require.Equal(t, tupleKey.Object, resp.Tuple.Key.Object)
	require.Equal(t, tupleKey.Relation, resp.Tuple.Key.Relation)
	require.Equal(t, tupleKey.User, resp.Tuple.Key.User)
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
	require.Equal(t, "group:eng#member", stream.results[0].Tuple.Key.User)
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
		require.Equal(t, "user:anne", result.Tuple.Key.User)
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
		require.Empty(t, resp.Tuples)
		require.Empty(t, resp.ContinuationToken)
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
