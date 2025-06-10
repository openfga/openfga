package graph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

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
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

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
				c := newRecursiveTTUObjectProvider(ts, ttu)
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

				c := newRecursiveTTUObjectProvider(ts, ttu)
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

				c := newRecursiveTTUObjectProvider(ts, ttu)
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

				c := newRecursiveTTUObjectProvider(ts, ttu)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				_, err = c.Begin(ctx, req)
				require.ErrorIs(t, err, mockError)
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

				c := newRecursiveTTUObjectProvider(ts, ttu)
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

				c := newRecursiveTTUObjectProvider(ts, ttu)
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
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

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

				c := newRecursiveUsersetObjectProvider(ts)
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

				c := newRecursiveUsersetObjectProvider(ts)
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

				c := newRecursiveUsersetObjectProvider(ts)
				t.Cleanup(c.End)

				ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)
				_, err = c.Begin(ctx, req)
				require.ErrorIs(t, err, mockError)
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

				c := newRecursiveUsersetObjectProvider(ts)
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

				c := newRecursiveUsersetObjectProvider(ts)

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

func TestIteratorToUserset(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		chans := make([]chan *iterator.Msg, 0, 1)
		outChan := make(chan usersetMessage, 1)
		ctx := context.Background()
		go iteratorsToUserset(ctx, chans, outChan)
		for _ = range outChan {
			require.Fail(t, "should not receive any messages")
		}
	})
	t.Run("returns_results", func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		chans := make([]chan *iterator.Msg, 0, 5)
		for i := 1; i <= 5; i++ {
			iterChan := make(chan *iterator.Msg, 1)
			iterChan <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{strconv.Itoa(i)})}
			close(iterChan)
			chans = append(chans, iterChan)
		}
		outChan := make(chan usersetMessage, len(chans))
		ctx := context.Background()
		go iteratorsToUserset(ctx, chans, outChan)
		count := 0
		for msg := range outChan {
			id, err := strconv.Atoi(msg.userset)
			require.NoError(t, err)
			require.Positive(t, id)
			require.LessOrEqual(t, id, len(chans))
			count++
		}
		require.Equal(t, count, len(chans))
	})
	t.Run("cancellation", func(t *testing.T) {
		t.Cleanup(func() {
			// this is the expected goroutine
			goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/openfga/openfga/internal/iterator.Drain.func1"))
		})
		chans := make([]chan *iterator.Msg, 0, 5)
		for i := 1; i <= 5; i++ {
			iterChan := make(chan *iterator.Msg, 1)
			iterChan <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{strconv.Itoa(i)})}
			//close(iterChan) -> by not closing, ctx.Done() is the exit clause
			chans = append(chans, iterChan)
		}
		outChan := make(chan usersetMessage, len(chans))
		ctx := context.Background()
		ctx, cancel := context.WithCancel(ctx)
		cancel()
		go iteratorsToUserset(ctx, chans, outChan)

		count := 0
		for msg := range outChan {
			id, err := strconv.Atoi(msg.userset)
			require.NoError(t, err)
			require.Positive(t, id)
			require.LessOrEqual(t, id, 5)
			count++
		}
		require.LessOrEqual(t, count, 5)
	})
	t.Run("handles_errors", func(t *testing.T) {
		t.Cleanup(func() {
			// this is the expected goroutine due to "iterator error"
			goleak.VerifyNone(t, goleak.IgnoreTopFunction("github.com/openfga/openfga/internal/iterator.Drain.func1"))
		})
		chans := make([]chan *iterator.Msg, 0, 2)
		iterChan1 := make(chan *iterator.Msg, 1)
		iterChan1 <- &iterator.Msg{Err: errors.New("iterator error")}
		close(iterChan1)
		chans = append(chans, iterChan1)
		iterChan2 := make(chan *iterator.Msg, 1)
		iterChan2 <- &iterator.Msg{Iter: mocks.NewErrorIterator[string]([]string{"1"})}
		close(iterChan2)
		chans = append(chans, iterChan2)

		outChan := make(chan usersetMessage, len(chans))
		ctx := context.Background()
		go iteratorsToUserset(ctx, chans, outChan)

		count := 0
		for msg := range outChan {
			if msg.err != nil {
				count++
			}
		}
		require.Equal(t, count, 2)
	})
}
