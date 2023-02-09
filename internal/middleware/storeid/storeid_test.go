package storeid

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
)

func TestUnaryStoreIDInterceptor(t *testing.T) {
	t.Run("unary_interceptor_with_no_storeID_in_request", func(t *testing.T) {
		interceptor := NewStoreIDInterceptor()

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			storeID, ok := StoreIDFromContext(ctx)
			require.False(t, ok)
			require.Empty(t, storeID)

			return nil, nil
		}

		_, err := interceptor(context.Background(), nil, nil, handler)
		require.NoError(t, err)
	})

	t.Run("unary_interceptor_with_storeID_in_request", func(t *testing.T) {
		storeID := "abc"
		interceptor := NewStoreIDInterceptor()

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			got, ok := StoreIDFromContext(ctx)
			require.True(t, ok)
			require.Equal(t, storeID, got)

			return nil, nil
		}

		_, err := interceptor(context.Background(), &openfgapb.CheckRequest{StoreId: storeID}, nil, handler)
		require.NoError(t, err)
	})
}

type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *mockServerStream) Context() context.Context {
	return s.ctx
}

func (s *mockServerStream) RecvMsg(m interface{}) error {
	return nil
}

func TestStreamingStoreIDInterceptor(t *testing.T) {
	t.Run("streaming_interceptor_with_no_GetStoreId_in_request", func(t *testing.T) {
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			err := stream.RecvMsg(nil)
			require.NoError(t, err)

			storeID, ok := StoreIDFromContext(stream.Context())
			require.False(t, ok)
			require.Empty(t, storeID)

			return nil
		}

		ss := &mockServerStream{ctx: context.Background()}
		err := NewStreamingStoreIDInterceptor()(nil, ss, nil, handler)
		require.NoError(t, err)
	})

	t.Run("streaming_interceptor_with_GetStoreId_in_request", func(t *testing.T) {
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			err := stream.RecvMsg(&openfgapb.CheckRequest{StoreId: "abc"})
			require.NoError(t, err)

			got, ok := StoreIDFromContext(stream.Context())
			require.True(t, ok)
			require.Equal(t, "abc", got)

			return nil
		}

		ss := &mockServerStream{ctx: context.Background()}
		err := NewStreamingStoreIDInterceptor()(nil, ss, nil, handler)
		require.NoError(t, err)
	})
}
