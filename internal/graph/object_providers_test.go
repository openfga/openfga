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

const concurrencyLimit = 50

func TestRecursiveObjectProvider(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	storeID := ulid.Make().String()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)

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

		t.Run("when_empty_iterator", func(t *testing.T) {
			mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
				Times(1).Return(storage.NewStaticTupleIterator(nil), nil)

			c := newRecursiveObjectProvider(ts, mockDatastore)
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

			c := newRecursiveObjectProvider(ts, mockDatastore)
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

			c := newRecursiveObjectProvider(ts, mockDatastore)
			t.Cleanup(c.End)

			ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
			channel, err := c.Begin(ctx, req)
			require.Nil(t, channel)
			require.Error(t, err)
		})
	})
}

func TestRecursiveTTUObjectProvider(t *testing.T) {
	storeID := ulid.Make().String()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)

	t.Run("Begin_And_End", func(t *testing.T) {
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
			ttu := typesystem.TupleToUserset("parent", "admin").GetTupleToUserset()

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

			t.Run("when_invalid_req", func(t *testing.T) {
				c := newRecursiveTTUObjectProvider(ts, ttu, concurrencyLimit)
				t.Cleanup(c.End)

				invalidReq, err := NewResolveCheckRequest(ResolveCheckRequestParams{
					StoreID:              storeID,
					AuthorizationModelID: ulid.Make().String(),
					TupleKey: &openfgav1.TupleKey{
						Object:   "unknown:abc",
						Relation: "admin",
						User:     "user:XYZ",
					},
				})
				require.NoError(t, err)

				_, err = c.Begin(context.Background(), invalidReq)
				require.ErrorContains(t, err, "is an undefined object type")
			})

			t.Run("when_empty_iterator", func(t *testing.T) {
				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).Return(storage.NewStaticTupleIterator(nil), nil)

				c := newRecursiveTTUObjectProvider(ts, ttu, concurrencyLimit)
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

			t.Run("when_iterator_returns_results", func(t *testing.T) {
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("document:1", "admin", "user:XYZ")},
						{Key: tuple.NewTupleKey("document:2", "admin", "user:XYZ")},
						{Key: tuple.NewTupleKey("document:3", "admin", "user:XYZ")},
					}), nil)

				c := newRecursiveTTUObjectProvider(ts, ttu, concurrencyLimit)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				channel, err := c.Begin(ctx, req)
				require.NoError(t, err)

				actualMessages := make([]usersetMessage, 0, 3)
				for msg := range channel {
					actualMessages = append(actualMessages, msg)
				}

				require.Len(t, actualMessages, 3)
				require.Equal(t, "document:1", actualMessages[0].userset)
				require.Equal(t, "document:2", actualMessages[1].userset)
				require.Equal(t, "document:3", actualMessages[2].userset)
			})

			t.Run("when_fastPathRewrite_errors", func(t *testing.T) {
				mockError := fmt.Errorf("error")
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).
					Return(nil, mockError)

				c := newRecursiveTTUObjectProvider(ts, ttu, concurrencyLimit)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				out, err := c.Begin(ctx, req)
				require.NoError(t, err)
				msg := <-out
				require.ErrorIs(t, msg.err, mockError)
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

				c := newRecursiveTTUObjectProvider(ts, ttu, concurrencyLimit)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				channel, err := c.Begin(ctx, req)
				require.NoError(t, err)

				actualMessages := make([]usersetMessage, 0, 1)
				for res := range channel {
					actualMessages = append(actualMessages, res)
				}

				require.Len(t, actualMessages, 1)
				require.Empty(t, actualMessages[0].userset)
				require.ErrorIs(t, actualMessages[0].err, mocks.ErrSimulatedError)
			})

			t.Run("when_context_cancelled", func(t *testing.T) {
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					MaxTimes(1).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("document:1", "admin", "user:XYZ")},
					}), nil)

				c := newRecursiveTTUObjectProvider(ts, ttu, concurrencyLimit)
				t.Cleanup(c.End)

				ctx, cancel := context.WithCancel(setRequestContext(context.Background(), ts, mockDatastore, nil))
				cancel()
				channel, err := c.Begin(ctx, req)
				if err != nil {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					actualMessages := make([]usersetMessage, 0, 1)
					for res := range channel {
						actualMessages = append(actualMessages, res)
					}
					require.Empty(t, actualMessages)
				}
			})
		})
	})
}

func TestRecursiveUsersetObjectProvider(t *testing.T) {
	storeID := ulid.Make().String()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)

	t.Run("Begin_And_End", func(t *testing.T) {
		t.Run("on_supported_model", func(t *testing.T) {
			model := testutils.MustTransformDSLToProtoWithID(`
				model
					schema 1.1
				type user
				type document
					relations
						define admin: [document#admin, user] 
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

			t.Run("when_empty_iterator", func(t *testing.T) {
				mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).Return(storage.NewStaticTupleIterator(nil), nil)

				c := newRecursiveUsersetObjectProvider(ts, concurrencyLimit)
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

			t.Run("when_iterator_returns_results", func(t *testing.T) {
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("document:1", "admin", "user:XYZ")},
						{Key: tuple.NewTupleKey("document:2", "admin", "user:XYZ")},
						{Key: tuple.NewTupleKey("document:3", "admin", "user:XYZ")},
					}), nil)

				c := newRecursiveUsersetObjectProvider(ts, concurrencyLimit)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				channel, err := c.Begin(ctx, req)
				require.NoError(t, err)

				actualMessages := make([]usersetMessage, 0, 3)
				for msg := range channel {
					actualMessages = append(actualMessages, msg)
				}

				require.Len(t, actualMessages, 3)
				require.Equal(t, "document:1", actualMessages[0].userset)
				require.Equal(t, "document:2", actualMessages[1].userset)
				require.Equal(t, "document:3", actualMessages[2].userset)
			})

			t.Run("when_fastPathRewrite_errors", func(t *testing.T) {
				mockError := fmt.Errorf("error")
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).
					Return(nil, mockError)

				c := newRecursiveUsersetObjectProvider(ts, concurrencyLimit)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				out, err := c.Begin(ctx, req)
				require.NoError(t, err)
				msg := <-out
				require.ErrorIs(t, mockError, msg.err)
			})

			t.Run("when_iterator_errors", func(t *testing.T) {
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					Times(1).
					DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter, _ storage.ReadStartingWithUserOptions) (storage.TupleIterator, error) {
						iterator := mocks.NewErrorTupleIterator([]*openfgav1.Tuple{
							{Key: tuple.NewTupleKey("document:1", "parent", "user:XYZ")},
						})
						return iterator, nil
					})

				c := newRecursiveUsersetObjectProvider(ts, concurrencyLimit)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				channel, err := c.Begin(ctx, req)
				require.NoError(t, err)

				actualMessages := make([]usersetMessage, 0, 1)
				for res := range channel {
					actualMessages = append(actualMessages, res)
				}

				require.Len(t, actualMessages, 1)
				require.Empty(t, actualMessages[0].userset)
				require.ErrorIs(t, actualMessages[0].err, mocks.ErrSimulatedError)
			})

			t.Run("when_context_cancelled", func(t *testing.T) {
				mockDatastore.EXPECT().
					ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).
					MaxTimes(1).
					Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
						{Key: tuple.NewTupleKey("document:1", "parent", "user:XYZ")},
					}), nil)

				c := newRecursiveUsersetObjectProvider(ts, concurrencyLimit)

				t.Cleanup(c.End)

				ctx, cancel := context.WithCancel(setRequestContext(context.Background(), ts, mockDatastore, nil))
				cancel()
				channel, err := c.Begin(ctx, req)
				// the context cancellation might not be propagated before the creation of ReadStartingWithUser, so the cancellation might happen before yielding tuples.
				if err != nil {
					require.ErrorIs(t, err, context.Canceled)
				} else {
					actualMessages := make([]usersetMessage, 0, 1)
					for res := range channel {
						actualMessages = append(actualMessages, res)
					}
					require.Empty(t, actualMessages)
				}
			})
		})
	})
}
