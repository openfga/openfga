package storeid

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

func TestUnaryInterceptor(t *testing.T) {
	t.Run("unary_interceptor_with_no_storeID_in_request", func(t *testing.T) {
		interceptor := NewUnaryInterceptor()

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			storeID, ok := StoreIDFromContext(ctx)
			require.True(t, ok)
			require.Empty(t, storeID)

			return nil, nil
		}

		_, err := interceptor(context.Background(), nil, &grpc.UnaryServerInfo{}, handler)
		require.NoError(t, err)
	})

	t.Run("unary_interceptor_with_storeID_in_request", func(t *testing.T) {
		storeID := "abc"
		interceptor := NewUnaryInterceptor()

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			got, ok := StoreIDFromContext(ctx)
			require.True(t, ok)
			require.Equal(t, storeID, got)

			return nil, nil
		}

		_, err := interceptor(context.Background(), &openfgav1.CheckRequest{StoreId: storeID}, &grpc.UnaryServerInfo{}, handler)
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

func (s *mockServerStream) RecvMsg(interface{}) error {
	return nil
}

func TestStreamingInterceptor(t *testing.T) {
	t.Run("streaming_interceptor_with_no_GetStoreId_in_request", func(t *testing.T) {
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			err := stream.RecvMsg(nil)
			require.NoError(t, err)

			storeID, ok := StoreIDFromContext(stream.Context())
			require.True(t, ok)
			require.Empty(t, storeID)

			return nil
		}

		ss := &mockServerStream{ctx: context.Background()}
		err := NewStreamingInterceptor()(nil, ss, &grpc.StreamServerInfo{}, handler)
		require.NoError(t, err)
	})

	t.Run("streaming_interceptor_with_GetStoreId_in_request", func(t *testing.T) {
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			err := stream.RecvMsg(&openfgav1.CheckRequest{StoreId: "abc"})
			require.NoError(t, err)

			got, ok := StoreIDFromContext(stream.Context())
			require.True(t, ok)
			require.Equal(t, "abc", got)

			return nil
		}

		ss := &mockServerStream{ctx: context.Background()}
		err := NewStreamingInterceptor()(nil, ss, &grpc.StreamServerInfo{}, handler)
		require.NoError(t, err)
	})
}
