package middleware

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
)

var ds = memory.New(10, 10)

func TestUnaryModelIDInterceptor(t *testing.T) {
	t.Run("empty_modelID_and_store_id_in_request", func(t *testing.T) {
		interceptor := NewModelIDInterceptor(ds)

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return nil, nil
		}

		_, err := interceptor(context.Background(), nil, nil, handler)
		require.NoError(t, err)
	})

	t.Run("no_modelID_in_request", func(t *testing.T) {
		interceptor := NewModelIDInterceptor(ds)

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			modelID, ok := ModelIDFromContext(ctx)
			require.False(t, ok)
			require.Empty(t, modelID)

			return nil, nil
		}

		_, err := interceptor(context.Background(), &openfgapb.CheckRequest{StoreId: ulid.Make().String()}, nil, handler)
		require.Error(t, err)
	})

	t.Run("empty_modelID_in_request_but_not_in_ds", func(t *testing.T) {
		interceptor := NewModelIDInterceptor(ds)

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return nil, nil
		}

		_, err := interceptor(context.Background(), &openfgapb.CheckRequest{StoreId: ulid.Make().String()}, nil, handler)
		require.Error(t, err)
	})

	t.Run("empty_modelID_in_request_should_find_from_ds", func(t *testing.T) {
		storeID := ulid.Make().String()
		modelID := ulid.Make().String()
		ctx := context.Background()

		err := ds.WriteAuthorizationModel(ctx, storeID, &openfgapb.AuthorizationModel{
			Id:              modelID,
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{{Type: "user"}},
		})
		require.NoError(t, err)

		interceptor := NewModelIDInterceptor(ds)

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			got, ok := ModelIDFromContext(ctx)
			require.True(t, ok)
			require.Equal(t, modelID, got)

			return nil, nil
		}

		_, err = interceptor(ctx, &openfgapb.CheckRequest{StoreId: storeID}, nil, handler)
		require.NoError(t, err)
	})

	t.Run("modelID_in_request", func(t *testing.T) {
		modelID := ulid.Make().String()
		interceptor := NewModelIDInterceptor(ds)

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			got, ok := ModelIDFromContext(ctx)
			require.True(t, ok)
			require.Equal(t, modelID, got)

			return nil, nil
		}

		_, err := interceptor(context.Background(), &openfgapb.CheckRequest{AuthorizationModelId: modelID}, nil, handler)
		require.NoError(t, err)
	})
}

func TestStreamingModelIDInterceptor(t *testing.T) {
	t.Run("no_modelId_in_request", func(t *testing.T) {
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			err := stream.RecvMsg(nil)
			require.NoError(t, err)

			modelID, ok := ModelIDFromContext(stream.Context())
			require.False(t, ok)
			require.Empty(t, modelID)

			return nil
		}

		ss := &mockServerStream{ctx: context.Background()}
		err := NewStreamingModelIDInterceptor(ds)(nil, ss, nil, handler)
		require.NoError(t, err)
	})

	t.Run("empty_modelId_in_request_should_read_from_ds", func(t *testing.T) {
		storeID := ulid.Make().String()
		modelID := ulid.Make().String()
		ctx := context.Background()

		err := ds.WriteAuthorizationModel(ctx, storeID, &openfgapb.AuthorizationModel{
			Id:              modelID,
			SchemaVersion:   typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgapb.TypeDefinition{{Type: "user"}},
		})
		require.NoError(t, err)

		handler := func(srv interface{}, stream grpc.ServerStream) error {
			err := stream.RecvMsg(&openfgapb.CheckRequest{StoreId: storeID})
			require.NoError(t, err)

			got, ok := ModelIDFromContext(stream.Context())
			require.True(t, ok)
			require.Equal(t, modelID, got)

			return nil
		}

		ss := &mockServerStream{ctx: ctx}
		err = NewStreamingModelIDInterceptor(ds)(nil, ss, nil, handler)
		require.NoError(t, err)
	})

	t.Run("modelID_in_request", func(t *testing.T) {
		storeID := ulid.Make().String()
		modelID := ulid.Make().String()

		handler := func(srv interface{}, stream grpc.ServerStream) error {
			err := stream.RecvMsg(&openfgapb.CheckRequest{
				StoreId:              storeID,
				AuthorizationModelId: modelID,
			})
			require.NoError(t, err)

			got, ok := ModelIDFromContext(stream.Context())
			require.True(t, ok)
			require.Equal(t, modelID, got)

			return nil
		}

		ss := &mockServerStream{ctx: context.Background()}
		err := NewStreamingModelIDInterceptor(ds)(nil, ss, nil, handler)
		require.NoError(t, err)
	})
}
