package server

import (
	"context"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"google.golang.org/protobuf/types/known/wrapperspb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/storage/memory"
)

func TestReadChangesPageSizeValidation(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	ds := memory.New()
	t.Cleanup(ds.Close)

	t.Run("page_size_not_provided", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		// When PageSize is not provided, should use default and not fail validation
		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId: storeID,
			Type:    "user",
			// PageSize is nil (not provided)
		})
		// Should succeed (no page size error, though store may not exist)
		require.NoError(t, err)
	})

	t.Run("page_size_within_default_limit", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(50),
		})
		// Should succeed (no page size error, though store may not exist)
		require.NoError(t, err)
	})

	t.Run("page_size_at_default_limit", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(100), // at default limit
		})
		// Should succeed (no page size error, though store may not exist)
		require.NoError(t, err)
	})

	t.Run("page_size_exceeds_default_limit", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(101), // exceeds default limit of 100
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid ReadChangesRequest.PageSize: value must be inside range [1, 100]")
	})

	t.Run("page_size_with_lower_custom_limit", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
			WithReadChangesMaxPageSize(50),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		// Within custom limit - should succeed
		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(30),
		})
		require.NoError(t, err)
	})

	t.Run("page_size_at_lower_custom_limit", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
			WithReadChangesMaxPageSize(50),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		// At custom limit - should succeed
		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(50),
		})
		require.NoError(t, err)
	})

	t.Run("page_size_exceeds_lower_custom_limit", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
			WithReadChangesMaxPageSize(50),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(51),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid ReadChangesRequest.PageSize: value must be inside range [1, 50]")
	})

	t.Run("page_size_with_higher_custom_limit", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
			WithReadChangesMaxPageSize(200),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		// Within custom limit higher than old proto max - should succeed
		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(150),
		})
		require.NoError(t, err)
	})

	t.Run("page_size_at_higher_custom_limit", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
			WithReadChangesMaxPageSize(200),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		// At custom limit higher than old proto max - should succeed
		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(200),
		})
		require.NoError(t, err)
	})

	t.Run("page_size_exceeds_higher_custom_limit", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
			WithReadChangesMaxPageSize(200),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(201),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid ReadChangesRequest.PageSize: value must be inside range [1, 200]")
	})

	t.Run("error_message_reflects_configured_max_for_higher_limit", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
			WithReadChangesMaxPageSize(500),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(501),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid ReadChangesRequest.PageSize: value must be inside range [1, 500]")
	})

	t.Run("page_size_zero", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(0),
		})
		require.Error(t, err)
		// Proto validation catches this before server-side validation
		require.Contains(t, err.Error(), "invalid ReadChangesRequest.PageSize: value must be greater than or equal to 1")
	})

	t.Run("page_size_negative", func(t *testing.T) {
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(-1),
		})
		require.Error(t, err)
		// Proto validation catches this before server-side validation
		require.Contains(t, err.Error(), "invalid ReadChangesRequest.PageSize: value must be greater than or equal to 1")
	})

	t.Run("invalid_max_falls_back_to_default", func(t *testing.T) {
		// WithReadChangesMaxPageSize(0) should fall back to default of 100
		openfga := MustNewServerWithOpts(
			WithDatastore(ds),
			WithReadChangesMaxPageSize(0),
		)
		t.Cleanup(openfga.Close)

		storeID := ulid.Make().String()

		// Should succeed with page size 100 (default limit)
		_, err := openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(100),
		})
		require.NoError(t, err)

		// Should fail with page size 101 (exceeds default limit)
		_, err = openfga.ReadChanges(context.Background(), &openfgav1.ReadChangesRequest{
			StoreId:  storeID,
			Type:     "user",
			PageSize: wrapperspb.Int32(101),
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid ReadChangesRequest.PageSize: value must be inside range [1, 100]")
	})
}
