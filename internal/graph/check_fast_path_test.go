package graph

import (
	"context"
	"errors"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestIteratorStreams(t *testing.T) {
	t.Run("getActiveStreams", func(t *testing.T) {
		t.Cleanup(func() {
			goleak.VerifyNone(t)
		})
		t.Run("should_exit_on_context_cancelled", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			stream := iteratorStreams{streams: []*iteratorStream{{done: false}}}
			_, err := stream.getActiveStreams(ctx)
			require.Equal(t, context.Canceled, err)
		})
		t.Run("should_error_out_with_producer_errors", func(t *testing.T) {
			ctx := context.Background()
			c := make(chan *iteratorMsg, 1)
			expectedErr := errors.New("boom")
			c <- &iteratorMsg{err: expectedErr}
			stream := iteratorStreams{streams: []*iteratorStream{{done: true}, {done: false, source: c}}}
			_, err := stream.getActiveStreams(ctx)
			require.Equal(t, expectedErr, err)
		})
		t.Run("should_filter_out_drained_producers", func(t *testing.T) {
			ctx := context.Background()
			streams := make([]*iteratorStream, 0)
			expectedLen := 0
			for i := 0; i < 5; i++ {
				c := make(chan *iteratorMsg, 1)
				producer := &iteratorStream{source: c}
				if i%2 == 0 {
					close(c)
				} else {
					expectedLen++
					c <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(nil)}
				}
				streams = append(streams, producer)
			}
			stream := iteratorStreams{streams: streams}
			res, err := stream.getActiveStreams(ctx)
			require.NoError(t, err)
			require.Len(t, res, expectedLen)
		})
		t.Run("should_return_empty_when_fully_drained", func(t *testing.T) {
			ctx := context.Background()
			streams := make([]*iteratorStream, 0)
			for i := 0; i < 5; i++ {
				producer := &iteratorStream{buffer: nil, done: true}
				streams = append(streams, producer)
			}
			stream := iteratorStreams{streams: streams}
			res, err := stream.getActiveStreams(ctx)
			require.NoError(t, err)
			require.Empty(t, res)
		})
	})
}

func TestFastPathDirect(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("should_return_iterator_through_channel", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "admin",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		checker := NewLocalChecker()
		ctx := context.Background()

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

		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)

		c, err := checker.fastPathDirect(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "admin", "user:1"),
		})
		require.NoError(t, err)
		msg, ok := <-c
		require.True(t, ok)
		require.NoError(t, msg.err)
		require.NotNil(t, msg.iter)
		msg.iter.Stop()
		_, ok = <-c
		require.False(t, ok)
	})
	t.Run("should_return_error_from_building_iterator", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "admin",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(nil, errors.New("boom"))
		checker := NewLocalChecker()
		ctx := context.Background()

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

		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)

		_, err = checker.fastPathDirect(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "admin", "user:1"),
		})
		require.Error(t, err)
	})
}

func TestFastPathComputed(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("should_return_error_if_computed_relation_doesnt_exist", func(t *testing.T) {
		checker := NewLocalChecker()
		ctx := context.Background()

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

		ctx = typesystem.ContextWithTypesystem(ctx, ts)

		_, err = checker.fastPathComputed(ctx, &ResolveCheckRequest{
			StoreID:  ulid.Make().String(),
			TupleKey: tuple.NewTupleKey("document:1", "admin", "user:1"),
		}, &openfgav1.Userset{Userset: &openfgav1.Userset_ComputedUserset{ComputedUserset: &openfgav1.ObjectRelation{Relation: "fake"}}})

		require.Error(t, err)
	})
}

func TestFastPathUnion(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("should_return_on_context_cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iteratorMsg)
		producers := make([]*iteratorStream, 0)
		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Stop().Times(1)
		producer := make(chan *iteratorMsg, 1)
		producer <- &iteratorMsg{iter: iter1}
		close(producer)
		producers = append(producers, &iteratorStream{source: producer})

		pool := concurrency.NewPool(context.Background(), 1)
		pool.Go(func(ctx context.Context) error {
			cancellableCtx, cancel := context.WithCancel(ctx)
			cancel()
			fastPathUnion(cancellableCtx, &iteratorStreams{streams: producers}, res)
			return nil
		})
		_, ok := <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_drain_iterators_on_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iteratorMsg)
		producers := make([]*iteratorStream, 0)
		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return(nil, errors.New("boom"))
		iter1.EXPECT().Stop().Times(1)
		producer1 := make(chan *iteratorMsg, 1)
		producer1 <- &iteratorMsg{iter: iter1}
		close(producer1)
		producers = append(producers, &iteratorStream{source: producer1})

		iter2 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter2.EXPECT().Stop().Times(1)
		producer2 := make(chan *iteratorMsg, 1)
		producer2 <- &iteratorMsg{iter: iter2}
		close(producer2)
		producers = append(producers, &iteratorStream{source: producer2})

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathUnion(ctx, &iteratorStreams{streams: producers}, res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_calculate_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		res := make(chan *iteratorMsg)
		producers := make([]*iteratorStream, 0, 4)

		ctx := context.Background()

		producer1 := make(chan *iteratorMsg, 3)
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "1",
		}})}
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "5",
		}})}
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "6",
		}})}
		close(producer1)
		producers = append(producers, &iteratorStream{source: producer1})

		producer2 := make(chan *iteratorMsg, 1)
		producer2 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "2",
		}})}
		close(producer2)
		producers = append(producers, &iteratorStream{source: producer2})

		producer3 := make(chan *iteratorMsg, 4)
		producer3 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "0",
		}})}
		producer3 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "3",
		}})}
		producer3 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "8",
		}})}
		producer3 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "9",
		}})}
		close(producer3)
		producers = append(producers, &iteratorStream{source: producer3})

		producer4 := make(chan *iteratorMsg, 2)
		producer4 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "4",
		}})}
		producer4 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "8",
		}})}
		close(producer4)
		producers = append(producers, &iteratorStream{source: producer4})

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathUnion(ctx, &iteratorStreams{streams: producers}, res)
			return nil
		})

		ids := make([]string, 0)
		for msg := range res {
			require.NoError(t, msg.err)
			for {
				tk, err := msg.iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					require.NoError(t, err)
				}
				ids = append(ids, tk.GetObject())
			}
		}
		err := pool.Wait()
		require.NoError(t, err)
		require.Equal(t, []string{"0", "1", "2", "3", "4", "5", "6", "8", "9"}, ids)
	})
	t.Run("multiple_item_in_same_stream", func(t *testing.T) {
		tests := []struct {
			name     string
			objects  [][]string
			expected []string
		}{
			{
				name: "first_item_matches",
				objects: [][]string{
					{"1", "5", "6"},
					{"1", "2"},
					{"0", "1", "2", "3", "8", "9"},
					{"1", "4"},
				},
				expected: []string{"0", "1", "2", "3", "4", "5", "6", "8", "9"},
			},
			{
				name: "last_item_matches",
				objects: [][]string{
					{"1", "5"},
					{"5"},
				},
				expected: []string{"1", "5"},
			},
			{
				name: "multiple_items",
				objects: [][]string{
					{"1", "5", "7", "9"},
					{"3", "4", "5", "6", "7"},
					{"5", "7", "9", "11"},
					{"5", "7", "8", "9", "11"},
				},
				expected: []string{"1", "3", "4", "5", "6", "7", "8", "9", "11"},
			},
			{
				name: "all_item_matches",
				objects: [][]string{
					{"1", "5", "7", "9"},
					{"1", "5", "7", "9"},
					{"1", "5", "7", "9"},
				},
				expected: []string{"1", "5", "7", "9"},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				res := make(chan *iteratorMsg)
				producers := make([]*iteratorStream, 0, len(tt.objects))
				ctx := context.Background()

				for _, objs := range tt.objects {
					producer := make(chan *iteratorMsg, 1)
					var keys []*openfgav1.TupleKey
					for _, obj := range objs {
						keys = append(keys, &openfgav1.TupleKey{Object: obj})
					}
					producer <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(keys)}
					close(producer)
					producers = append(producers, &iteratorStream{source: producer})
				}
				pool := concurrency.NewPool(ctx, 1)
				pool.Go(func(ctx context.Context) error {
					fastPathUnion(ctx, &iteratorStreams{streams: producers}, res)
					return nil
				})

				ids := make([]string, 0)
				for msg := range res {
					require.NoError(t, msg.err)
					for {
						tk, err := msg.iter.Next(ctx)
						if err != nil {
							if storage.IterIsDoneOrCancelled(err) {
								break
							}
							require.NoError(t, err)
						}
						ids = append(ids, tk.GetObject())
					}
				}
				err := pool.Wait()
				require.NoError(t, err)
				require.Equal(t, tt.expected, ids)
			})
		}
	})
}

func TestFastPathIntersection(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("should_return_on_context_cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iteratorMsg)
		producers := make([]*iteratorStream, 0)
		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Stop().Times(1)
		producer := make(chan *iteratorMsg, 1)
		producer <- &iteratorMsg{iter: iter1}
		close(producer)
		producers = append(producers, &iteratorStream{source: producer})

		pool := concurrency.NewPool(context.Background(), 1)
		pool.Go(func(ctx context.Context) error {
			cancellableCtx, cancel := context.WithCancel(ctx)
			cancel()
			fastPathIntersection(cancellableCtx, &iteratorStreams{streams: producers}, res)
			return nil
		})
		_, ok := <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_drain_iterators_on_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iteratorMsg)
		producers := make([]*iteratorStream, 0)
		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return(nil, errors.New("boom"))
		iter1.EXPECT().Stop().Times(1)
		producer1 := make(chan *iteratorMsg, 1)
		producer1 <- &iteratorMsg{iter: iter1}
		close(producer1)
		producers = append(producers, &iteratorStream{source: producer1})
		iter2 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter2.EXPECT().Stop().Times(1)
		producer2 := make(chan *iteratorMsg, 1)
		producer2 <- &iteratorMsg{iter: iter2}
		close(producer2)
		producers = append(producers, &iteratorStream{source: producer2})
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, &iteratorStreams{streams: producers}, res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_calculate_intersection", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		res := make(chan *iteratorMsg)
		producers := make([]*iteratorStream, 0, 4)

		ctx := context.Background()

		producer1 := make(chan *iteratorMsg, 3)
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "1",
		}})}
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "5",
		}})}
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "6",
		}})}
		close(producer1)
		producers = append(producers, &iteratorStream{source: producer1})

		producer2 := make(chan *iteratorMsg, 2)
		producer2 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "1",
		}})}
		producer2 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "2",
		}})}
		close(producer2)
		producers = append(producers, &iteratorStream{source: producer2})

		producer3 := make(chan *iteratorMsg, 6)
		producer3 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "0",
		}})}
		producer3 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "1",
		}})}
		producer3 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "2",
		}})}
		producer3 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "3",
		}})}
		producer3 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "8",
		}})}
		producer3 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "9",
		}})}
		close(producer3)
		producers = append(producers, &iteratorStream{source: producer3})

		producer4 := make(chan *iteratorMsg, 2)
		producer4 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "1",
		}})}
		producer4 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "4",
		}})}
		close(producer4)
		producers = append(producers, &iteratorStream{source: producer4})

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, &iteratorStreams{streams: producers}, res)
			return nil
		})

		ids := make([]string, 0)
		for msg := range res {
			require.NoError(t, msg.err)
			for {
				tk, err := msg.iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					require.NoError(t, err)
				}
				ids = append(ids, tk.GetObject())
			}
		}
		err := pool.Wait()
		require.NoError(t, err)
		require.Equal(t, []string{"1"}, ids)
	})
	t.Run("multiple_item_in_same_stream", func(t *testing.T) {
		tests := []struct {
			name     string
			objects  [][]string
			expected []string
		}{
			{
				name: "first_item_matches",
				objects: [][]string{
					{"1", "5", "6"},
					{"1", "2"},
					{"0", "1", "2", "3", "8", "9"},
					{"1", "4"},
				},
				expected: []string{"1"},
			},
			{
				name: "last_item_matches",
				objects: [][]string{
					{"1", "5"},
					{"5"},
				},
				expected: []string{"5"},
			},
			{
				name: "multiple_items",
				objects: [][]string{
					{"1", "5", "7", "9"},
					{"3", "4", "5", "6", "7"},
					{"5", "7", "9", "11"},
					{"5", "7", "8", "9", "11"},
				},
				expected: []string{"5", "7"},
			},
			{
				name: "no_item_matches",
				objects: [][]string{
					{"1", "5", "7", "9"},
					{"3", "8", "10", "12"},
					{"5", "7", "9", "11"},
					{"5", "7", "8", "9", "11"},
				},
				expected: []string{},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				res := make(chan *iteratorMsg)
				producers := make([]*iteratorStream, 0, len(tt.objects))
				ctx := context.Background()

				for _, objs := range tt.objects {
					producer := make(chan *iteratorMsg, 1)
					var keys []*openfgav1.TupleKey
					for _, obj := range objs {
						keys = append(keys, &openfgav1.TupleKey{Object: obj})
					}
					producer <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(keys)}
					close(producer)
					producers = append(producers, &iteratorStream{source: producer})
				}
				pool := concurrency.NewPool(ctx, 1)
				pool.Go(func(ctx context.Context) error {
					fastPathIntersection(ctx, &iteratorStreams{streams: producers}, res)
					return nil
				})

				ids := make([]string, 0)
				for msg := range res {
					require.NoError(t, msg.err)
					for {
						tk, err := msg.iter.Next(ctx)
						if err != nil {
							if storage.IterIsDoneOrCancelled(err) {
								break
							}
							require.NoError(t, err)
						}
						ids = append(ids, tk.GetObject())
					}
				}
				err := pool.Wait()
				require.NoError(t, err)
				require.Equal(t, tt.expected, ids)
			})
		}
	})
}

func TestFastPathDifference(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("should_return_on_context_cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iteratorMsg)
		producers := make([]*iteratorStream, 0)

		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Stop().Times(1)
		producer1 := make(chan *iteratorMsg, 1)
		producer1 <- &iteratorMsg{iter: iter1}
		close(producer1)
		producers = append(producers, &iteratorStream{source: producer1})

		iter2 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter2.EXPECT().Stop().Times(1)
		producer2 := make(chan *iteratorMsg, 1)
		producer2 <- &iteratorMsg{iter: iter2}
		close(producer2)
		producers = append(producers, &iteratorStream{source: producer2})

		pool := concurrency.NewPool(context.Background(), 1)
		pool.Go(func(ctx context.Context) error {
			cancellableCtx, cancel := context.WithCancel(ctx)
			cancel()
			fastPathDifference(cancellableCtx, &iteratorStreams{streams: producers}, res)
			return nil
		})
		_, ok := <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_drain_iterators_on_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iteratorMsg)
		producers := make([]*iteratorStream, 0)
		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return(nil, errors.New("boom"))
		iter1.EXPECT().Stop().Times(1)
		producer1 := make(chan *iteratorMsg, 1)
		producer1 <- &iteratorMsg{iter: iter1}
		close(producer1)
		producers = append(producers, &iteratorStream{source: producer1})
		iter2 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter2.EXPECT().Stop().Times(1)
		producer2 := make(chan *iteratorMsg, 1)
		producer2 <- &iteratorMsg{iter: iter2}
		close(producer2)
		producers = append(producers, &iteratorStream{source: producer2})
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, &iteratorStreams{streams: producers}, res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_calculate_difference", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		res := make(chan *iteratorMsg)
		producers := make([]*iteratorStream, 0, 2)

		ctx := context.Background()

		producer1 := make(chan *iteratorMsg, 6)
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "1",
		}})}
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "2",
		}})}
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "3",
		}})}
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "6",
		}})}
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "8",
		}})}
		producer1 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "9",
		}})}
		close(producer1)
		producers = append(producers, &iteratorStream{idx: BaseIndex, source: producer1})

		producer2 := make(chan *iteratorMsg, 6)
		producer2 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "0",
		}})}
		producer2 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "1",
		}})}
		producer2 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "5",
		}})}
		producer2 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "6",
		}})}
		close(producer2)
		producers = append(producers, &iteratorStream{idx: DifferenceIndex, source: producer2})

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, &iteratorStreams{streams: producers}, res)
			return nil
		})

		ids := make([]string, 0)
		for msg := range res {
			require.NoError(t, msg.err)
			for {
				tk, err := msg.iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					require.NoError(t, err)
				}
				ids = append(ids, tk.GetObject())
			}
		}
		err := pool.Wait()
		require.NoError(t, err)
		require.Equal(t, []string{"2", "3", "8", "9"}, ids)
	})
	t.Run("multiple_item_in_same_stream", func(t *testing.T) {
		tests := []struct {
			name     string
			objects  [][]string
			expected []string
		}{
			{
				name: "subtract_first_item_last_item_smaller",
				objects: [][]string{
					{"1", "5", "6"},
					{"1", "2"},
				},
				expected: []string{"5", "6"},
			},
			{
				name: "subtract_first_item_last_item_bigger",
				objects: [][]string{
					{"1", "5", "6"},
					{"1", "2", "7"},
				},
				expected: []string{"5", "6"},
			},
			{
				name: "subtract_first_few_item",
				objects: [][]string{
					{"1", "5", "6"},
					{"1", "5"},
				},
				expected: []string{"6"},
			},
			{
				name: "subtract_last_item",
				objects: [][]string{
					{"1", "2", "5"},
					{"3", "5"},
				},
				expected: []string{"1", "2"},
			},
			{
				name: "subtract_few_item",
				objects: [][]string{
					{"1", "2", "5"},
					{"2", "5"},
				},
				expected: []string{"1"},
			},
			{
				name: "subtract_no_item",
				objects: [][]string{
					{"1", "2", "5"},
					{"3", "6"},
				},
				expected: []string{"1", "2", "5"},
			},
			{
				name: "subtract_all_item",
				objects: [][]string{
					{"1", "2", "5"},
					{"1", "2", "5"},
				},
				expected: []string{},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				res := make(chan *iteratorMsg)
				producers := make([]*iteratorStream, 0, len(tt.objects))
				ctx := context.Background()

				for idx, objs := range tt.objects {
					producer := make(chan *iteratorMsg, 1)
					var keys []*openfgav1.TupleKey
					for _, obj := range objs {
						keys = append(keys, &openfgav1.TupleKey{Object: obj})
					}
					producer <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(keys)}
					close(producer)
					producers = append(producers, &iteratorStream{idx: idx, source: producer})
				}
				pool := concurrency.NewPool(ctx, 1)
				pool.Go(func(ctx context.Context) error {
					fastPathDifference(ctx, &iteratorStreams{streams: producers}, res)
					return nil
				})

				ids := make([]string, 0)
				for msg := range res {
					require.NoError(t, msg.err)
					for {
						tk, err := msg.iter.Next(ctx)
						if err != nil {
							if storage.IterIsDoneOrCancelled(err) {
								break
							}
							require.NoError(t, err)
						}
						ids = append(ids, tk.GetObject())
					}
				}
				err := pool.Wait()
				require.NoError(t, err)
				require.Equal(t, tt.expected, ids)
			})
		}
	})
}
