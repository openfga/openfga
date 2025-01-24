package graph

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestSimpleRecursiveObjectProvider(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	storeID := ulid.Make().String()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)

	t.Run("New", func(t *testing.T) {
		_, err := newSimpleRecursiveObjectProvider(nil, mockDatastore)
		require.Error(t, err)
	})

	t.Run("on_supported_model", func(t *testing.T) {
		model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define admin: [user] or admin from parent
						define parent: [document]
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		req, err := NewResolveCheckRequest(ResolveCheckRequestParams{
			StoreID:              storeID,
			AuthorizationModelID: ulid.Make().String(),
			TupleKey: &openfgav1.TupleKey{
				Object:   "document:abc",
				Relation: "admin",
				User:     "user:XYZ",
			},
		})
		require.NoError(t, err)

		t.Run("when_empty_req", func(t *testing.T) {
			c, err := newSimpleRecursiveObjectProvider(ts, mockDatastore)
			require.NoError(t, err)
			t.Cleanup(c.End)

			_, err = c.Begin(context.Background(), nil)
			require.Error(t, err)
		})

		t.Run("when_empty_iterator", func(t *testing.T) {
			mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
				Times(1).Return(storage.NewStaticTupleIterator(nil), nil)

			c, err := newSimpleRecursiveObjectProvider(ts, mockDatastore)
			require.NoError(t, err)
			t.Cleanup(c.End)

			ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
			channel, err := c.Begin(ctx, req)
			require.NoError(t, err)

			actualMessages := make([]usersetMessage, 0)
			for msg := range channel {
				actualMessages = append(actualMessages, msg)
			}

			require.Empty(t, actualMessages)
		})

		t.Run("when_iterator_returns_one_result", func(t *testing.T) {
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
				Times(1).
				Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
					{Key: tuple.NewTupleKey("document:1", "admin", "user:XYZ")},
				}), nil)

			c, err := newSimpleRecursiveObjectProvider(ts, mockDatastore)
			require.NoError(t, err)
			t.Cleanup(c.End)

			ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
			channel, err := c.Begin(ctx, req)
			require.NoError(t, err)

			actualMessages := make([]usersetMessage, 0)
			for msg := range channel {
				actualMessages = append(actualMessages, msg)
			}

			require.Len(t, actualMessages, 1)
			require.Equal(t, "document:1", actualMessages[0].userset)
		})

		t.Run("when_iterator_errors", func(t *testing.T) {
			mockDatastore.EXPECT().
				ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
				Times(1).
				Return(nil, fmt.Errorf("error"))

			c, err := newSimpleRecursiveObjectProvider(ts, mockDatastore)
			require.NoError(t, err)
			t.Cleanup(c.End)

			ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
			channel, err := c.Begin(ctx, req)
			require.Nil(t, channel)
			require.Error(t, err)
		})
	})
}

func TestComplexRecursiveObjectProvider(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	storeID := ulid.Make().String()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)

	t.Run("New", func(t *testing.T) {
		_, err := newComplexRecursiveObjectProvider(1, nil)
		require.Error(t, err)
	})

	t.Run("Begin_And_End", func(t *testing.T) {
		t.Run("on_unsupported_model_returns_error", func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define admin: [user]
			`)

			ts, err := typesystem.New(model)
			require.NoError(t, err)

			req, err := NewResolveCheckRequest(ResolveCheckRequestParams{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "admin",
					User:     "user:XYZ",
				},
			})
			require.NoError(t, err)

			c, err := newComplexRecursiveObjectProvider(1, ts)
			require.NoError(t, err)
			t.Cleanup(c.End)

			_, err = c.Begin(context.Background(), req)
			require.ErrorContains(t, err, "unsupported model")
		})

		t.Run("on_supported_model", func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define admin: [user] or admin from parent
						define parent: [document]
			`)

			ts, err := typesystem.New(model)
			require.NoError(t, err)

			req, err := NewResolveCheckRequest(ResolveCheckRequestParams{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey: &openfgav1.TupleKey{
					Object:   "document:abc",
					Relation: "admin",
					User:     "user:XYZ",
				},
			})
			require.NoError(t, err)

			t.Run("when_empty_req", func(t *testing.T) {
				c, err := newComplexRecursiveObjectProvider(1, ts)
				require.NoError(t, err)
				t.Cleanup(c.End)

				_, err = c.Begin(context.Background(), nil)
				require.Error(t, err)
			})

			t.Run("when_empty_iterator", func(t *testing.T) {
				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).Return(storage.NewStaticTupleIterator(nil), nil)

				c, err := newComplexRecursiveObjectProvider(1, ts)
				require.NoError(t, err)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				channel, err := c.Begin(ctx, req)
				require.NoError(t, err)

				actualMessages := make([]usersetMessage, 0)
				for msg := range channel {
					actualMessages = append(actualMessages, msg)
				}

				require.Empty(t, actualMessages)
			})

			t.Run("when_iterator_returns_one_result", func(t *testing.T) {
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("document:1", "admin", "user:XYZ")},
					}), nil)

				c, err := newComplexRecursiveObjectProvider(1, ts)
				require.NoError(t, err)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				channel, err := c.Begin(ctx, req)
				require.NoError(t, err)

				actualMessages := make([]usersetMessage, 0)
				for msg := range channel {
					actualMessages = append(actualMessages, msg)
				}

				require.Len(t, actualMessages, 1)
				require.Equal(t, "document:1", actualMessages[0].userset)
			})

			t.Run("when_fastPathRewrite_errors", func(t *testing.T) {
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).
					Return(nil, fmt.Errorf("error"))

				c, err := newComplexRecursiveObjectProvider(1, ts)
				require.NoError(t, err)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				channel, err := c.Begin(ctx, req)
				require.NoError(t, err)

				actualMessages := make([]usersetMessage, 0)
				for msg := range channel {
					actualMessages = append(actualMessages, msg)
				}

				require.Len(t, actualMessages, 1)
				require.Empty(t, actualMessages[0].userset)
				require.Error(t, actualMessages[0].err)
			})

			t.Run("when_iterator_errors", func(t *testing.T) {
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).
					DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter, _ storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
						iterator := mocks.NewErrorTupleIterator([]*openfgav1.Tuple{
							{Key: tuple.NewTupleKey("document:1", "admin", "user:XYZ")},
						})
						return iterator, nil
					})

				c, err := newComplexRecursiveObjectProvider(1, ts)
				require.NoError(t, err)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				channel, err := c.Begin(ctx, req)
				require.NoError(t, err)

				actualMessages := make([]usersetMessage, 0, 2)
				for res := range channel {
					actualMessages = append(actualMessages, res)
				}

				require.Len(t, actualMessages, 2)
				require.NotEmpty(t, actualMessages[0].userset)
				require.NoError(t, actualMessages[0].err)
				require.Empty(t, actualMessages[1].userset)
				require.Error(t, actualMessages[1].err)
			})

			t.Run("when_context_cancelled", func(t *testing.T) {
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					MaxTimes(1).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("document:1", "admin", "user:XYZ")},
					}), nil)

				c, err := newComplexRecursiveObjectProvider(1, ts)
				require.NoError(t, err)
				t.Cleanup(c.End)

				ctx, cancel := context.WithCancel(setRequestContext(context.Background(), ts, mockDatastore, nil))
				cancel()
				channel, err := c.Begin(ctx, req)
				require.NoError(t, err)

				actualMessages := make([]usersetMessage, 0, 1)
				for res := range channel {
					actualMessages = append(actualMessages, res)
				}

				require.LessOrEqual(t, len(actualMessages), 1)
				if len(actualMessages) == 1 {
					require.Empty(t, actualMessages[0].userset)
					require.Error(t, actualMessages[0].err)
				}
			})
		})
	})
}
