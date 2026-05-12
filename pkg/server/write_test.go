package server

import (
	"context"
	"strings"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
)

func TestCorrelationIDPattern(t *testing.T) {
	valid := []struct {
		name  string
		input string
	}{
		{"single_char", "a"},
		{"two_chars", "ab"},
		{"alphanumeric_with_hyphens", "abc-123"},
		{"prefixed", "req-abc-123"},
		{"uppercase", "A1"},
		{"max_length_36", "123456789012345678901234567890123456"},
	}
	for _, tc := range valid {
		t.Run("valid/"+tc.name, func(t *testing.T) {
			require.True(t, correlationIDPattern.MatchString(tc.input))
		})
	}

	invalid := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"leading_hyphen", "-leading-hyphen"},
		{"trailing_hyphen", "trailing-hyphen-"},
		{"spaces", "has spaces"},
		{"underscore", "has_underscore"},
		{"too_long_37", "1234567890123456789012345678901234567"},
		{"special_chars", "special!chars"},
	}
	for _, tc := range invalid {
		t.Run("invalid/"+tc.name, func(t *testing.T) {
			require.False(t, correlationIDPattern.MatchString(tc.input))
		})
	}
}

func TestWriteCorrelationIDServerHandler(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	const minimalModel = `
		model
			schema 1.1
		type user
		type document
			relations
				define viewer: [user]`

	t.Run("invalid_correlation_id_returns_invalid_argument", func(t *testing.T) {
		openfga := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(openfga.Close)

		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			"x-correlation-id": "!!!invalid!!!",
		}))

		resp, err := openfga.Write(ctx, &openfgav1.WriteRequest{
			StoreId: ulid.Make().String(),
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:alice"),
				},
			},
		})

		require.Nil(t, resp)
		require.Error(t, err)
		s, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, s.Code())
		require.Contains(t, s.Message(), "invalid X-Correlation-ID header value")
	})

	t.Run("valid_correlation_id_write_succeeds", func(t *testing.T) {
		openfga := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(openfga.Close)

		createStoreResp, err := openfga.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		model := testutils.MustTransformDSLToProtoWithID(minimalModel)
		_, err = openfga.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:         storeID,
			SchemaVersion:   model.GetSchemaVersion(),
			TypeDefinitions: model.GetTypeDefinitions(),
		})
		require.NoError(t, err)

		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			"x-correlation-id": "req-abc-123",
		}))

		resp, err := openfga.Write(ctx, &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:alice"),
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("no_correlation_id_header_write_succeeds", func(t *testing.T) {
		openfga := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(openfga.Close)

		createStoreResp, err := openfga.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		model := testutils.MustTransformDSLToProtoWithID(minimalModel)
		_, err = openfga.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:         storeID,
			SchemaVersion:   model.GetSchemaVersion(),
			TypeDefinitions: model.GetTypeDefinitions(),
		})
		require.NoError(t, err)

		resp, err := openfga.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:alice"),
				},
			},
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("write_with_correlation_id_round_trip_via_read_changes", func(t *testing.T) {
		openfga := MustNewServerWithOpts(WithDatastore(ds))
		t.Cleanup(openfga.Close)

		createStoreResp, err := openfga.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{Name: "test"})
		require.NoError(t, err)
		storeID := createStoreResp.GetId()

		model := testutils.MustTransformDSLToProtoWithID(minimalModel)
		_, err = openfga.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:         storeID,
			SchemaVersion:   model.GetSchemaVersion(),
			TypeDefinitions: model.GetTypeDefinitions(),
		})
		require.NoError(t, err)

		correlationID := "req-roundtrip-1"
		ctxWithCorrelation := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			"x-correlation-id": correlationID,
		}))

		_, err = openfga.Write(ctxWithCorrelation, &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:alice"),
				},
			},
		})
		require.NoError(t, err)

		_, err = openfga.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.WriteRequestWrites{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:2", "viewer", "user:bob"),
				},
			},
		})
		require.NoError(t, err)

		readResp, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:       storeID,
			CorrelationId: correlationID,
		})
		require.NoError(t, err)
		require.Len(t, readResp.GetChanges(), 1)
		require.Equal(t, correlationID, readResp.GetChanges()[0].GetCorrelationId())
		require.Equal(t, "document:1", readResp.GetChanges()[0].GetTupleKey().GetObject())
	})
}

func TestGetCorrelationIDFromMetadata(t *testing.T) {
	t.Run("returns_value_when_header_present", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			strings.ToLower(CorrelationIDHeader): "abc-123",
		}))
		require.Equal(t, "abc-123", getCorrelationIDFromMetadata(ctx))
	})

	t.Run("returns_trimmed_value", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			strings.ToLower(CorrelationIDHeader): "  trimmed  ",
		}))
		require.Equal(t, "trimmed", getCorrelationIDFromMetadata(ctx))
	})

	t.Run("returns_empty_when_header_not_present", func(t *testing.T) {
		require.Empty(t, getCorrelationIDFromMetadata(context.Background()))
	})

	t.Run("returns_empty_when_header_value_is_empty", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			strings.ToLower(CorrelationIDHeader): "",
		}))
		require.Empty(t, getCorrelationIDFromMetadata(ctx))
	})

	t.Run("header_key_is_case_insensitive", func(t *testing.T) {
		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(map[string]string{
			"x-correlation-id": "case-check",
		}))
		require.Equal(t, "case-check", getCorrelationIDFromMetadata(ctx))
	})
}
