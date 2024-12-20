package graph

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"

	"github.com/emirpasic/gods/sets/hashset"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

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

func TestBreadthFirstRecursiveMatch(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	tests := []struct {
		name                 string
		currentLevelUsersets *hashset.Set
		usersetFromUser      *hashset.Set
		readMocks            [][]*openfgav1.Tuple
		expectedOutcomes     []checkOutcome
	}{
		{
			name:                 "empty_userset",
			currentLevelUsersets: hashset.New(),
			usersetFromUser:      hashset.New(),
			expectedOutcomes:     []checkOutcome{},
		},
		{
			name:                 "duplicates_no_match_no_recursion",
			currentLevelUsersets: hashset.New("group:1", "group:2", "group:3", "group:1"),
			usersetFromUser:      hashset.New(),
			readMocks: [][]*openfgav1.Tuple{
				{{}},
				{{}},
				{{}},
			},
			expectedOutcomes: []checkOutcome{
				{resp: &ResolveCheckResponse{
					Allowed: false,
				}},
			},
		},
		{
			name:                 "duplicates_no_match_with_recursion",
			currentLevelUsersets: hashset.New("group:1", "group:2", "group:3"),
			usersetFromUser:      hashset.New(),
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:3")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:2")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:1")}},
			},
			expectedOutcomes: []checkOutcome{
				{resp: &ResolveCheckResponse{
					Allowed: false,
				}},
				{resp: &ResolveCheckResponse{
					Allowed: false,
				}},
			},
		},
		{
			name:                 "duplicates_match_with_recursion",
			currentLevelUsersets: hashset.New("group:1", "group:2", "group:3"),
			usersetFromUser:      hashset.New("group:4"),
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:3")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:1")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:4")}},
			},
			expectedOutcomes: []checkOutcome{
				{resp: &ResolveCheckResponse{
					Allowed: true,
				}},
			},
		},
		{
			name:                 "duplicates_match_with_recursion",
			currentLevelUsersets: hashset.New("group:1", "group:2", "group:3"),
			usersetFromUser:      hashset.New("group:4"),
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:3")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:1")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:4")}},
			},
			expectedOutcomes: []checkOutcome{
				{resp: &ResolveCheckResponse{
					Allowed: true,
				}},
			},
		},
		{
			name:                 "no_duplicates_no_match_counts",
			currentLevelUsersets: hashset.New("group:1", "group:2", "group:3"),
			usersetFromUser:      hashset.New(),
			readMocks: [][]*openfgav1.Tuple{
				{{Key: tuple.NewTupleKey("group:1", "parent", "group:4")}},
				{{Key: tuple.NewTupleKey("group:2", "parent", "group:5")}},
				{{Key: tuple.NewTupleKey("group:3", "parent", "group:6")}},
				{{Key: tuple.NewTupleKey("group:6", "parent", "group:9")}},
				{{Key: tuple.NewTupleKey("group:7", "parent", "group:10")}},
				{{Key: tuple.NewTupleKey("group:8", "parent", "group:11")}},
				{{}},
				{{}},
				{{}},
			},
			expectedOutcomes: []checkOutcome{
				{resp: &ResolveCheckResponse{
					Allowed: false,
				}},
				{resp: &ResolveCheckResponse{
					Allowed: false,
				}},
				{resp: &ResolveCheckResponse{
					Allowed: false,
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ds := mocks.NewMockRelationshipTupleReader(ctrl)

			storeID := ulid.Make().String()

			for _, mock := range tt.readMocks {
				ds.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).Times(1).Return(storage.NewStaticTupleIterator(mock), nil)
			}

			model := parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user] or member from parent
						define parent: [group]
				`)

			req := &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:3", "member", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(),
			}

			ts, err := typesystem.New(model)
			require.NoError(t, err)
			ctx := context.Background()
			ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)
			ctx = typesystem.ContextWithTypesystem(ctx, ts)

			checker := NewLocalChecker()
			mapping := &recursiveMapping{
				kind:             TTUKind,
				tuplesetRelation: "parent",
			}
			checkOutcomeChan := make(chan checkOutcome, 100) // large buffer since there is no need to concurrently evaluate partial results
			checker.breadthFirstRecursiveMatch(ctx, req, mapping, &sync.Map{}, tt.currentLevelUsersets, tt.usersetFromUser, checkOutcomeChan)

			collectedOutcomes := make([]checkOutcome, 0)
			for outcome := range checkOutcomeChan {
				collectedOutcomes = append(collectedOutcomes, outcome)
			}
			require.Equal(t, tt.expectedOutcomes, collectedOutcomes)
		})
	}
}

func TestRecursiveTTUFastPath(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	model := parser.MustTransformDSLToProto(`
				model
					schema 1.1
				type user
				type group
					relations
						define member: [user] or member from parent
						define parent: [group]
				`)
	tests := []struct {
		name                            string
		readStartingWithUserTuples      []*openfgav1.Tuple
		readStartingWithUserTuplesError error
		readTuples                      [][]*openfgav1.Tuple
		readTuplesError                 error
		expected                        *ResolveCheckResponse
		expectedError                   error
	}{
		{
			name: "happy_case",
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:1", "member", "user:maria"),
				},
			},
			readTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:2a", "parent", "group:1a"),
					},
					{
						Key: tuple.NewTupleKey("group:3", "parent", "group:2a"),
					},
					{
						Key: tuple.NewTupleKey("group:3", "parent", "group:2"),
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			ds := mocks.NewMockRelationshipTupleReader(ctrl)
			storeID := ulid.Make().String()
			ds.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
				ObjectType: "group",
				Relation:   "member",
				UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
				ObjectIDs:  nil,
			}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuples), tt.readStartingWithUserTuplesError)

			for _, tuples := range tt.readTuples {
				ds.EXPECT().Read(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), tt.readTuplesError)
			}
			ts, err := typesystem.New(model)
			require.NoError(t, err)
			rel, err := ts.GetRelation("group", "parent")
			require.NoError(t, err)

			req := &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:3", "member", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(),
			}
			ctx := context.Background()
			ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)
			ctx = typesystem.ContextWithTypesystem(ctx, ts)
			checker := NewLocalChecker()
			result, err := checker.recursiveTTUFastPath(ctx, req, rel.GetRewrite(), storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{Object: "group:2", Relation: "parent", User: "group:1"}}))
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
			require.Equal(t, tt.expected.GetResolutionMetadata(), result.GetResolutionMetadata())
		})
	}
}

func TestRecursiveUsersetFastPath(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	tests := []struct {
		name                            string
		readStartingWithUserTuples      []*openfgav1.Tuple
		readStartingWithUserTuplesError error
		readUsersetTuples               [][]*openfgav1.Tuple
		readUsersetTuplesError          error
		expected                        *ResolveCheckResponse
		expectedError                   error
	}{
		{
			name:                       "no_user_assigned_to_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{},
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
					},
				},
				{},
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
			},
		},
		{
			name: "user_assigned_to_first_level_sub_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
				},
			},
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:3#member"),
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
			},
		},
		{
			name: "user_assigned_to_second_level_sub_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
				},
			},
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:6", "member", "group:5#member"),
					},
				},
				{
					{
						Key: tuple.NewTupleKey("group:5", "member", "group:3#member"),
					},
				},
			},
			expected: &ResolveCheckResponse{
				Allowed: true,
			},
		},
		{
			name: "user_not_assigned_to_sub_group",
			readStartingWithUserTuples: []*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:3", "member", "user:maria"),
				},
				{
					Key: tuple.NewTupleKey("group:4", "member", "user:maria"),
				},
			},
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:2#member"),
					},
				},
				{},
			},
			expected: &ResolveCheckResponse{
				Allowed: false,
			},
		},
		{
			name:                            "error_getting_tuple",
			readStartingWithUserTuples:      []*openfgav1.Tuple{},
			readStartingWithUserTuplesError: fmt.Errorf("mock error"),
			readUsersetTuples: [][]*openfgav1.Tuple{
				{
					{
						Key: tuple.NewTupleKey("group:1", "member", "group:2#member"),
					},
				},
				{},
			},
			expected:      nil,
			expectedError: fmt.Errorf("mock error"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			storeID := ulid.Make().String()
			ds := mocks.NewMockRelationshipTupleReader(ctrl)
			ctx := context.Background()
			ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

			ds.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
				ObjectType: "group",
				Relation:   "member",
				UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
				ObjectIDs:  nil,
			}, gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tt.readStartingWithUserTuples), tt.readStartingWithUserTuplesError)

			for _, tuples := range tt.readUsersetTuples[1:] {
				ds.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(storage.NewStaticTupleIterator(tuples), tt.readUsersetTuplesError)
			}
			model := parser.MustTransformDSLToProto(`
						model
							schema 1.1

						type user
						type group
							relations
								define member: [user, group#member]
`)

			ts, err := typesystem.New(model)
			require.NoError(t, err)
			ctx = typesystem.ContextWithTypesystem(ctx, ts)

			req := &ResolveCheckRequest{
				StoreID:              storeID,
				AuthorizationModelID: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
				RequestMetadata:      NewCheckRequestMetadata(),
			}

			checker := NewLocalChecker()
			tupleKeys := make([]*openfgav1.TupleKey, 0, len(tt.readUsersetTuples[0]))
			for _, t := range tt.readUsersetTuples[0] {
				k := t.GetKey()
				tupleKeys = append(tupleKeys, &openfgav1.TupleKey{
					User:     k.GetUser(),
					Relation: k.GetRelation(),
					Object:   k.GetObject(),
				})
			}

			result, err := checker.recursiveUsersetFastPath(ctx, req, storage.NewStaticTupleKeyIterator(tupleKeys))
			require.Equal(t, tt.expectedError, err)
			require.Equal(t, tt.expected.GetAllowed(), result.GetAllowed())
			require.Equal(t, tt.expected.GetResolutionMetadata(), result.GetResolutionMetadata())
		})
	}

	t.Run("resolution_depth_exceeded", func(t *testing.T) {
		t.Parallel()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()
		ds := mocks.NewMockRelationshipTupleReader(ctrl)

		ctx := context.Background()
		ctx = storage.ContextWithRelationshipTupleReader(ctx, ds)

		ds.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(
			storage.NewStaticTupleIterator([]*openfgav1.Tuple{
				{
					Key: tuple.NewTupleKey("group:bad", "member", "user:maria"),
				},
			}), nil)

		for i := 1; i < 26; i++ {
			ds.EXPECT().ReadUsersetTuples(gomock.Any(), storeID, gomock.Any(), gomock.Any()).MaxTimes(1).Return(
				storage.NewStaticTupleIterator([]*openfgav1.Tuple{
					{
						Key: tuple.NewTupleKey("group:"+strconv.Itoa(i+1), "member", "group:"+strconv.Itoa(i)+"#member"),
					},
				}), nil)
		}
		model := parser.MustTransformDSLToProto(`
							model
								schema 1.1

							type user
							type group
								relations
									define member: [user, group#member]

			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)
		ctx = typesystem.ContextWithTypesystem(ctx, ts)

		req := &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ulid.Make().String(),
			TupleKey:             tuple.NewTupleKey("group:1", "member", "user:maria"),
			RequestMetadata:      NewCheckRequestMetadata(),
		}

		checker := NewLocalChecker()
		tupleKeys := []*openfgav1.TupleKey{{Object: "group:1", Relation: "member", User: "group:0#member"}}

		result, err := checker.recursiveUsersetFastPath(ctx, req, storage.NewStaticTupleKeyIterator(tupleKeys))
		require.Nil(t, result)
		require.Equal(t, ErrResolutionDepthExceeded, err)
	})
}

func TestBuildRecursiveMapper(t *testing.T) {
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)

	storeID := ulid.Make().String()
	ctx := context.Background()
	ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)

	model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define member: [user]`)
	ts, err := typesystem.New(model)
	require.NoError(t, err)

	ctx = typesystem.ContextWithTypesystem(ctx, ts)
	checker := NewLocalChecker()

	t.Run("recursive_userset", func(t *testing.T) {
		mockDatastore.EXPECT().ReadUsersetTuples(ctx, storeID, storage.ReadUsersetTuplesFilter{
			Object:   "document:1",
			Relation: "viewer",
			AllowedUserTypeRestrictions: []*openfgav1.RelationReference{
				typesystem.DirectRelationReference("group", "member"),
			},
		}, storage.ReadUsersetTuplesOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}).Times(1)

		mapping := &recursiveMapping{
			kind: UsersetKind,
			allowedUserTypeRestrictions: []*openfgav1.RelationReference{
				typesystem.DirectRelationReference("group", "member"),
			},
		}
		res, err := checker.buildRecursiveMapper(ctx, &ResolveCheckRequest{
			StoreID:     storeID,
			TupleKey:    tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context:     testutils.MustNewStruct(t, map[string]interface{}{"x": "2"}),
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		}, mapping)
		require.NoError(t, err)
		_, ok := res.(*UsersetMapper)
		require.True(t, ok)
	})

	t.Run("recursive_ttu", func(t *testing.T) {
		mockDatastore.EXPECT().Read(ctx, storeID, tuple.NewTupleKey("document:1", "parent", ""), storage.ReadOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
			},
		}).Times(1)

		mapping := &recursiveMapping{
			tuplesetRelation: "parent",
			kind:             TTUKind,
		}
		res, err := checker.buildRecursiveMapper(ctx, &ResolveCheckRequest{
			StoreID:     storeID,
			TupleKey:    tuple.NewTupleKey("document:1", "viewer", "user:maria"),
			Context:     testutils.MustNewStruct(t, map[string]interface{}{"x": "2"}),
			Consistency: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		}, mapping)
		require.NoError(t, err)
		_, ok := res.(*TTUMapper)
		require.True(t, ok)
	})
}

func TestCheckUsersetFastPathV2(t *testing.T) {
	t.Parallel()
	t.Run("non_public_wildcard_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "members", "user:1")},
		}), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:1"},
				{Object: tuple.TypedPublicWildcard("user")},
			},
			ObjectIDs: nil,
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
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define viewer: [group#all]
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)
		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:1#all",
			Relation: "viewer",
			Object:   "document:1",
		}})
		val, err := checker.checkUsersetFastPathV2(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, iter)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.True(t, val.GetAllowed())
	})
	t.Run("non_public_wildcard_union_not_match", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "members", "user:1")},
		}), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:1"},
				{Object: tuple.TypedPublicWildcard("user")},
			},
			ObjectIDs: nil,
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
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define viewer: [group#all]
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)
		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:2#all",
			Relation: "viewer",
			Object:   "document:1",
		}})
		val, err := checker.checkUsersetFastPathV2(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, iter)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.False(t, val.GetAllowed())
	})
	t.Run("public_wildcard_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:1"},
				{Object: tuple.TypedPublicWildcard("user")},
			},
			ObjectIDs: nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "public", "user:*")},
		}), nil)
		checker := NewLocalChecker()
		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define viewer: [group#all]
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)
		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:1#all",
			Relation: "viewer",
			Object:   "document:1",
		}})
		val, err := checker.checkUsersetFastPathV2(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, iter)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.True(t, val.GetAllowed())
	})
	t.Run("public_wildcard_union_not_match", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:1"},
				{Object: tuple.TypedPublicWildcard("user")},
			},
			ObjectIDs: nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "public", "user:*")},
		}), nil)
		checker := NewLocalChecker()
		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define viewer: [group#all]
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)
		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:2#all",
			Relation: "viewer",
			Object:   "document:1",
		}})
		val, err := checker.checkUsersetFastPathV2(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, iter)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.False(t, val.GetAllowed())
	})
}

func TestCheckTTUFastPathV2(t *testing.T) {
	t.Parallel()
	t.Run("non_public_wildcard_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "members", "user:1")},
		}), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:1"},
				{Object: tuple.TypedPublicWildcard("user")},
			},
			ObjectIDs: nil,
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
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define parent: [group]
					define viewer: all from parent
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)
		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:1",
			Relation: "parent",
			Object:   "document:1",
		}})
		val, err := checker.checkTTUFastPathV2(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, typesystem.TupleToUserset("parent", "all"), iter)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.True(t, val.GetAllowed())
	})

	t.Run("public_wildcard_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:1"},
				{Object: tuple.TypedPublicWildcard("user")},
			},
			ObjectIDs: nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "public", "user:*")},
		}), nil)

		checker := NewLocalChecker()
		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define parent: [group]
					define viewer: all from parent
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)
		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:1",
			Relation: "parent",
			Object:   "document:1",
		}})
		val, err := checker.checkTTUFastPathV2(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, typesystem.TupleToUserset("parent", "all"), iter)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.True(t, val.GetAllowed())
	})

	t.Run("non_public_wildcard_union_not_match", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "members", "user:1")},
		}), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:1"},
				{Object: tuple.TypedPublicWildcard("user")},
			},
			ObjectIDs: nil,
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
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define parent: [group]
					define viewer: all from parent
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)
		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:2",
			Relation: "parent",
			Object:   "document:1",
		}})
		val, err := checker.checkTTUFastPathV2(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, typesystem.TupleToUserset("parent", "all"), iter)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.False(t, val.GetAllowed())
	})

	t.Run("public_wildcard_union_not_match", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "members",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "public",
			UserFilter: []*openfgav1.ObjectRelation{
				{Object: "user:1"},
				{Object: tuple.TypedPublicWildcard("user")},
			},
			ObjectIDs: nil,
		}, storage.ReadStartingWithUserOptions{
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator([]*openfgav1.Tuple{
			{Key: tuple.NewTupleKey("group:1", "public", "user:*")},
		}), nil)

		checker := NewLocalChecker()
		ctx := context.Background()

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define members: [user]
					define public: [user, user:*]
					define all: members or public
			type document
				relations
					define parent: [group]
					define viewer: all from parent
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		ctx = typesystem.ContextWithTypesystem(ctx, ts)
		ctx = storage.ContextWithRelationshipTupleReader(ctx, mockDatastore)
		iter := storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			User:     "group:2",
			Relation: "parent",
			Object:   "document:1",
		}})
		val, err := checker.checkTTUFastPathV2(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, typesystem.TupleToUserset("parent", "all"), iter)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.False(t, val.GetAllowed())
	})
}
