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

	"github.com/openfga/openfga/internal/concurrency"
	"github.com/openfga/openfga/internal/iterator"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/internal/utils/apimethod"
	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// setRequestContext creates the correct storage wrappers in the request. NOTE: "ds" can be a mock.
func setRequestContext(ctx context.Context, ts *typesystem.TypeSystem, ds storage.RelationshipTupleReader, ctxTuples []*openfgav1.TupleKey) context.Context {
	rsw := storagewrappers.NewRequestStorageWrapperWithCache(
		ds,
		ctxTuples,
		&storagewrappers.Operation{
			Method:      apimethod.Check,
			Concurrency: config.DefaultMaxConcurrentReadsForCheck,
		},
		storagewrappers.DataResourceConfiguration{
			Resources:     nil,
			CacheSettings: config.CacheSettings{},
		},
	)
	ctx = storage.ContextWithRelationshipTupleReader(ctx, rsw)
	ctx = typesystem.ContextWithTypesystem(ctx, ts)
	return ctx
}

func TestFastPathDirect(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("should_return_iterator_through_channel", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "admin",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			WithResultsSortedAscending: true,
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(storage.NewStaticTupleIterator(nil), nil)

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

		ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)

		c, err := fastPathDirect(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "admin", "user:1"),
		})
		require.NoError(t, err)
		msg, ok := <-c
		require.True(t, ok)
		require.NoError(t, msg.Err)
		require.NotNil(t, msg.Iter)
		msg.Iter.Stop()
		_, ok = <-c
		require.False(t, ok)
	})
	t.Run("should_return_error_from_building_iterator", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockRelationshipTupleReader(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "document",
			Relation:   "admin",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:1"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			WithResultsSortedAscending: true,
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).MaxTimes(1).Return(nil, errors.New("boom"))

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

		ctx := setRequestContext(context.Background(), ts, mockDatastore, nil)

		_, err = fastPathDirect(ctx, &ResolveCheckRequest{
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

		ctx = setRequestContext(ctx, ts, nil, nil)

		_, err = fastPathComputed(ctx, &ResolveCheckRequest{
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

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Stop().MaxTimes(1)
		producer := make(chan *iterator.Msg, 1)
		producer <- &iterator.Msg{Iter: iter1}
		close(producer)
		producers = append(producers, iterator.NewStream(0, producer))

		pool := concurrency.NewPool(context.Background(), 1)
		pool.Go(func(ctx context.Context) error {
			cancellableCtx, cancel := context.WithCancel(ctx)
			cancel()
			fastPathUnion(cancellableCtx, iterator.NewStreams(producers), res)
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

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return("", errors.New("boom"))
		iter1.EXPECT().Stop().MaxTimes(1)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: iter1}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Stop().MaxTimes(1)
		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: iter2}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathUnion(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_calculate_union", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0, 4)

		ctx := context.Background()

		producer1 := make(chan *iterator.Msg, 3)
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:1"})}
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:5"})}
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:6"})}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		producer3 := make(chan *iterator.Msg, 4)
		producer3 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:0"})}
		producer3 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:3"})}
		producer3 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:8"})}
		producer3 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:9"})}
		close(producer3)
		producers = append(producers, iterator.NewStream(0, producer3))

		producer4 := make(chan *iterator.Msg, 2)
		producer4 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:4"})}
		producer4 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:8"})}
		close(producer4)
		producers = append(producers, iterator.NewStream(0, producer4))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathUnion(ctx, iterator.NewStreams(producers), res)
			return nil
		})

		ids := make([]string, 0)
		for msg := range res {
			require.NoError(t, msg.Err)
			for {
				tk, err := msg.Iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					require.NoError(t, err)
				}
				ids = append(ids, tk)
			}
		}
		err := pool.Wait()
		require.NoError(t, err)
		require.Equal(t, []string{"obj:0", "obj:1", "obj:2", "obj:3", "obj:4", "obj:5", "obj:6", "obj:8", "obj:9"}, ids)
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
					{"obj:1", "obj:5", "obj:6"},
					{"obj:1", "obj:2"},
					{"obj:0", "obj:1", "obj:2", "obj:3", "obj:8", "obj:9"},
					{"obj:1", "obj:4"},
				},
				expected: []string{"obj:0", "obj:1", "obj:2", "obj:3", "obj:4", "obj:5", "obj:6", "obj:8", "obj:9"},
			},
			{
				name: "last_item_matches",
				objects: [][]string{
					{"obj:1", "obj:5"},
					{"obj:5"},
				},
				expected: []string{"obj:1", "obj:5"},
			},
			{
				name: "multiple_items",
				objects: [][]string{
					{"obj:1", "obj:5", "obj:7", "obj:9"},
					{"obj:3", "obj:4", "obj:5", "obj:6", "obj:7"},
					{"obj:5", "obj:7", "obj:9", "obj:11"},
					{"obj:5", "obj:7", "obj:8", "obj:9", "obj:11"},
				},
				expected: []string{"obj:1", "obj:3", "obj:4", "obj:5", "obj:6", "obj:7", "obj:8", "obj:9", "obj:11"},
			},
			{
				name: "all_item_matches",
				objects: [][]string{
					{"obj:1", "obj:5", "obj:7", "obj:9"},
					{"obj:1", "obj:5", "obj:7", "obj:9"},
					{"obj:1", "obj:5", "obj:7", "obj:9"},
				},
				expected: []string{"obj:1", "obj:5", "obj:7", "obj:9"},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				res := make(chan *iterator.Msg)
				producers := make([]*iterator.Stream, 0, len(tt.objects))
				ctx := context.Background()

				for _, objs := range tt.objects {
					producer := make(chan *iterator.Msg, 1)
					producer <- &iterator.Msg{Iter: storage.NewStaticIterator[string](objs)}
					close(producer)
					producers = append(producers, iterator.NewStream(0, producer))
				}
				pool := concurrency.NewPool(ctx, 1)
				pool.Go(func(ctx context.Context) error {
					fastPathUnion(ctx, iterator.NewStreams(producers), res)
					return nil
				})

				ids := make([]string, 0)
				for msg := range res {
					require.NoError(t, msg.Err)
					for {
						tk, err := msg.Iter.Next(ctx)
						if err != nil {
							if storage.IterIsDoneOrCancelled(err) {
								break
							}
							require.NoError(t, err)
						}
						ids = append(ids, tk)
					}
				}
				err := pool.Wait()
				require.NoError(t, err)
				require.Equal(t, tt.expected, ids)
			})
		}
	})
	t.Run("large_than_single_batch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		res := make(chan *iterator.Msg)
		const numStream = 2
		producers := make([]*iterator.Stream, 0, numStream)
		ctx := context.Background()

		const numItems = 2000

		for i := 0; i < numStream; i++ {
			producer := make(chan *iterator.Msg, 1)
			var keys []string
			for j := 0; j < numItems; j++ {
				keys = append(keys, "obj:"+strconv.Itoa(j))
			}
			producer <- &iterator.Msg{Iter: storage.NewStaticIterator[string](keys)}
			close(producer)
			producers = append(producers, iterator.NewStream(0, producer))
		}
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathUnion(ctx, iterator.NewStreams(producers), res)
			return nil
		})

		ids := make([]string, 0)
		for msg := range res {
			require.NoError(t, msg.Err)
			for {
				tk, err := msg.Iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					require.NoError(t, err)
				}
				ids = append(ids, tk)
			}
		}
		err := pool.Wait()
		require.NoError(t, err)
		var expectedObjects []string
		for j := 0; j < numItems; j++ {
			expectedObjects = append(expectedObjects, "obj:"+strconv.Itoa(j))
		}
		require.Equal(t, expectedObjects, ids)
	})
	t.Run("should_return_error_get_active_stream_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Err: fmt.Errorf("mock error")}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathUnion(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_drain_next_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return("obj:0", nil)
		iter1.EXPECT().Next(gomock.Any()).MaxTimes(1).Return("", errors.New("boom"))

		iter1.EXPECT().Stop().MaxTimes(1)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: iter1}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:0"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathUnion(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
}

func TestFastPathIntersection(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("should_return_on_context_cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Stop().MaxTimes(1)
		producer := make(chan *iterator.Msg, 1)
		producer <- &iterator.Msg{Iter: iter1}
		close(producer)
		producers = append(producers, iterator.NewStream(0, producer))

		pool := concurrency.NewPool(context.Background(), 1)
		pool.Go(func(ctx context.Context) error {
			cancellableCtx, cancel := context.WithCancel(ctx)
			cancel()
			fastPathIntersection(cancellableCtx, iterator.NewStreams(producers), res)
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

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return("", errors.New("boom"))
		iter1.EXPECT().Stop().MaxTimes(1)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: iter1}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))
		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Stop().MaxTimes(1)
		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: iter2}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_calculate_intersection", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0, 4)

		ctx := context.Background()

		producer1 := make(chan *iterator.Msg, 3)
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:1"})}
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:5"})}
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:6"})}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		producer2 := make(chan *iterator.Msg, 2)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:1"})}
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		producer3 := make(chan *iterator.Msg, 6)
		producer3 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:0"})}
		producer3 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:1"})}
		producer3 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		producer3 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:3"})}
		producer3 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:8"})}
		producer3 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:9"})}
		close(producer3)
		producers = append(producers, iterator.NewStream(0, producer3))

		producer4 := make(chan *iterator.Msg, 2)
		producer4 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:1"})}
		producer4 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:4"})}
		close(producer4)
		producers = append(producers, iterator.NewStream(0, producer4))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, iterator.NewStreams(producers), res)
			return nil
		})

		ids := make([]string, 0)
		for msg := range res {
			require.NoError(t, msg.Err)
			for {
				tk, err := msg.Iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					require.NoError(t, err)
				}
				ids = append(ids, tk)
			}
		}
		err := pool.Wait()
		require.NoError(t, err)
		require.Equal(t, []string{"obj:1"}, ids)
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
					{"obj:1", "obj:5", "obj:6"},
					{"obj:1", "obj:2"},
					{"obj:0", "obj:1", "obj:2", "obj:3", "obj:8", "obj:9"},
					{"obj:1", "obj:4"},
				},
				expected: []string{"obj:1"},
			},
			{
				name: "last_item_matches",
				objects: [][]string{
					{"obj:1", "obj:5"},
					{"obj:5"},
				},
				expected: []string{"obj:5"},
			},
			{
				name: "multiple_items",
				objects: [][]string{
					{"obj:1", "obj:5", "obj:7", "obj:9"},
					{"obj:3", "obj:4", "obj:5", "obj:6", "obj:7"},
					{"obj:5", "obj:7", "obj:9", "obj:11"},
					{"obj:5", "obj:7", "obj:8", "obj:9", "obj:11"},
				},
				expected: []string{"obj:5", "obj:7"},
			},
			{
				name: "no_item_matches",
				objects: [][]string{
					{"obj:1", "obj:5", "obj:7", "obj:9"},
					{"obj:3", "obj:8", "obj:10", "obj:12"},
					{"obj:5", "obj:7", "obj:9", "obj:11"},
					{"obj:5", "obj:7", "obj:8", "obj:9", "obj:11"},
				},
				expected: []string{},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				res := make(chan *iterator.Msg)
				producers := make([]*iterator.Stream, 0, len(tt.objects))
				ctx := context.Background()

				for _, objs := range tt.objects {
					producer := make(chan *iterator.Msg, 1)
					producer <- &iterator.Msg{Iter: storage.NewStaticIterator[string](objs)}
					close(producer)
					producers = append(producers, iterator.NewStream(0, producer))
				}
				pool := concurrency.NewPool(ctx, 1)
				pool.Go(func(ctx context.Context) error {
					fastPathIntersection(ctx, iterator.NewStreams(producers), res)
					return nil
				})

				ids := make([]string, 0)
				for msg := range res {
					require.NoError(t, msg.Err)
					for {
						tk, err := msg.Iter.Next(ctx)
						if err != nil {
							if storage.IterIsDoneOrCancelled(err) {
								break
							}
							require.NoError(t, err)
						}
						ids = append(ids, tk)
					}
				}
				err := pool.Wait()
				require.NoError(t, err)
				require.Equal(t, tt.expected, ids)
			})
		}
	})
	t.Run("large_than_single_batch", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		res := make(chan *iterator.Msg)
		const numStream = 2
		producers := make([]*iterator.Stream, 0, numStream)
		ctx := context.Background()

		const numItems = 2000

		for i := 0; i < numStream; i++ {
			producer := make(chan *iterator.Msg, 1)
			var keys []string
			for j := 0; j < numItems; j++ {
				keys = append(keys, "obj:"+strconv.Itoa(j))
			}
			producer <- &iterator.Msg{Iter: storage.NewStaticIterator[string](keys)}
			close(producer)
			producers = append(producers, iterator.NewStream(0, producer))
		}
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, iterator.NewStreams(producers), res)
			return nil
		})

		ids := make([]string, 0)
		for msg := range res {
			require.NoError(t, msg.Err)
			for {
				tk, err := msg.Iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					require.NoError(t, err)
				}
				ids = append(ids, tk)
			}
		}
		err := pool.Wait()
		require.NoError(t, err)
		var expectedObjects []string
		for j := 0; j < numItems; j++ {
			expectedObjects = append(expectedObjects, "obj:"+strconv.Itoa(j))
		}
		require.Equal(t, expectedObjects, ids)
	})
	t.Run("should_return_error_get_active_stream_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Err: fmt.Errorf("mock error")}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_drain_next_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return("obj:0", nil)
		iter1.EXPECT().Next(gomock.Any()).MaxTimes(1).Return("", errors.New("boom"))

		iter1.EXPECT().Stop().MaxTimes(1)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: iter1}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:0"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_drain_head_error_when_removing_smaller_item", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return("obj:1", nil)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return("", errors.New("boom"))

		iter1.EXPECT().Stop().MaxTimes(1)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: iter1}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_drain_next_error_when_removing_smaller_item", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		// the first two times of Head() is to remove the first item (1)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(2).Return("obj:1", nil)
		// this is to simulate successful remove of item 1
		iter1.EXPECT().Next(gomock.Any()).MaxTimes(1).Return("obj:1", nil)
		// the next get Head() is bad
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return("", fmt.Errorf("bad_head"))

		iter1.EXPECT().Stop().MaxTimes(1)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: iter1}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
}

func TestFastPathDifference(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("should_return_on_context_cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)

		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Stop().MaxTimes(1)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: iter1}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Stop().MaxTimes(1)
		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: iter2}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		pool := concurrency.NewPool(context.Background(), 1)
		pool.Go(func(ctx context.Context) error {
			cancellableCtx, cancel := context.WithCancel(ctx)
			cancel()
			fastPathDifference(cancellableCtx, iterator.NewStreams(producers), res)
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

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return("", errors.New("boom"))
		iter1.EXPECT().Stop().MaxTimes(1)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: iter1}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))
		iter2 := mocks.NewMockIterator[string](ctrl)
		iter2.EXPECT().Stop().MaxTimes(1)
		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: iter2}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_calculate_difference", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0, 2)

		ctx := context.Background()

		producer1 := make(chan *iterator.Msg, 6)
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:1"})}
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:3"})}
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:6"})}
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:8"})}
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:9"})}
		close(producer1)
		producers = append(producers, iterator.NewStream(BaseIndex, producer1))

		producer2 := make(chan *iterator.Msg, 6)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:0"})}
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:1"})}
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:5"})}
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:6"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(DifferenceIndex, producer2))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, iterator.NewStreams(producers), res)
			return nil
		})

		ids := make([]string, 0)
		for msg := range res {
			require.NoError(t, msg.Err)
			for {
				tk, err := msg.Iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					require.NoError(t, err)
				}
				ids = append(ids, tk)
			}
		}
		err := pool.Wait()
		require.NoError(t, err)
		require.Equal(t, []string{"obj:2", "obj:3", "obj:8", "obj:9"}, ids)
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
					{"obj:1", "obj:5", "obj:6"},
					{"obj:1", "obj:2"},
				},
				expected: []string{"obj:5", "obj:6"},
			},
			{
				name: "subtract_first_item_last_item_bigger",
				objects: [][]string{
					{"obj:1", "obj:5", "obj:6"},
					{"obj:1", "obj:2", "obj:7"},
				},
				expected: []string{"obj:5", "obj:6"},
			},
			{
				name: "subtract_first_few_item",
				objects: [][]string{
					{"obj:1", "obj:5", "obj:6"},
					{"obj:1", "obj:5"},
				},
				expected: []string{"obj:6"},
			},
			{
				name: "subtract_last_item",
				objects: [][]string{
					{"obj:1", "obj:2", "obj:5"},
					{"obj:3", "obj:5"},
				},
				expected: []string{"obj:1", "obj:2"},
			},
			{
				name: "subtract_few_item",
				objects: [][]string{
					{"obj:1", "obj:2", "obj:5"},
					{"obj:2", "obj:5"},
				},
				expected: []string{"obj:1"},
			},
			{
				name: "subtract_no_item",
				objects: [][]string{
					{"obj:1", "obj:2", "obj:5"},
					{"obj:3", "obj:6"},
				},
				expected: []string{"obj:1", "obj:2", "obj:5"},
			},
			{
				name: "subtract_all_item",
				objects: [][]string{
					{"obj:1", "obj:2", "obj:5"},
					{"obj:1", "obj:2", "obj:5"},
				},
				expected: []string{},
			},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				defer ctrl.Finish()
				res := make(chan *iterator.Msg)
				producers := make([]*iterator.Stream, 0, len(tt.objects))
				ctx := context.Background()

				for idx, objs := range tt.objects {
					producer := make(chan *iterator.Msg, 1)
					producer <- &iterator.Msg{Iter: storage.NewStaticIterator[string](objs)}
					close(producer)
					producers = append(producers, iterator.NewStream(idx, producer))
				}
				pool := concurrency.NewPool(ctx, 1)
				pool.Go(func(ctx context.Context) error {
					fastPathDifference(ctx, iterator.NewStreams(producers), res)
					return nil
				})

				ids := make([]string, 0)
				for msg := range res {
					require.NoError(t, msg.Err)
					for {
						tk, err := msg.Iter.Next(ctx)
						if err != nil {
							if storage.IterIsDoneOrCancelled(err) {
								break
							}
							require.NoError(t, err)
						}
						ids = append(ids, tk)
					}
				}
				err := pool.Wait()
				require.NoError(t, err)
				require.Equal(t, tt.expected, ids)
			})
		}
	})
	t.Run("should_return_error_get_active_stream_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Err: fmt.Errorf("mock error")}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))

		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_able_to_handle_larger_than_batch_size", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		res := make(chan *iterator.Msg)

		numItems := 2002
		var object1 []string
		for i := 0; i < numItems; i++ {
			object1 = append(object1, "obj:"+strconv.Itoa(i))
		}
		object2 := []string{"obj:0"}
		objects := [][]string{object1, object2}

		producers := make([]*iterator.Stream, 0, len(objects))
		ctx := context.Background()

		for idx, objs := range objects {
			producer := make(chan *iterator.Msg, 1)
			producer <- &iterator.Msg{Iter: storage.NewStaticIterator[string](objs)}
			close(producer)
			producers = append(producers, iterator.NewStream(idx, producer))
		}
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, iterator.NewStreams(producers), res)
			return nil
		})

		ids := make([]string, 0)
		for msg := range res {
			require.NoError(t, msg.Err)
			for {
				tk, err := msg.Iter.Next(ctx)
				if err != nil {
					if storage.IterIsDoneOrCancelled(err) {
						break
					}
					require.NoError(t, err)
				}
				ids = append(ids, tk)
			}
		}
		err := pool.Wait()
		require.NoError(t, err)
		var expected []string
		for i := 1; i < numItems; i++ {
			expected = append(expected, "obj:"+strconv.Itoa(i))
		}
		require.Equal(t, expected, ids)
	})
	t.Run("should_return_error_when_same_item_next_has_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Head(gomock.Any()).AnyTimes().Return("obj:1", nil)
		iter1.EXPECT().Next(gomock.Any()).MaxTimes(1).Return("", errors.New("boom"))
		iter1.EXPECT().Stop().MaxTimes(1)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: iter1}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))
		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:1"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_return_error_when_smaller_base_has_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Head(gomock.Any()).AnyTimes().Return("obj:1", nil)
		iter1.EXPECT().Next(gomock.Any()).MaxTimes(1).Return("", errors.New("boom"))
		iter1.EXPECT().Stop().MaxTimes(1)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: iter1}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))
		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
	t.Run("should_return_error_when_smaller_diff_has_error", func(t *testing.T) {
		ctx := context.Background()
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iterator.Msg)
		producers := make([]*iterator.Stream, 0)
		producer1 := make(chan *iterator.Msg, 1)
		producer1 <- &iterator.Msg{Iter: storage.NewStaticIterator[string]([]string{"obj:2"})}
		close(producer1)
		producers = append(producers, iterator.NewStream(0, producer1))
		iter1 := mocks.NewMockIterator[string](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(2).Return("obj:1", nil)
		iter1.EXPECT().Next(gomock.Any()).MaxTimes(2).Return("obj:1", nil)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return("", errors.New("boom"))
		iter1.EXPECT().Stop().MaxTimes(1)
		producer2 := make(chan *iterator.Msg, 1)
		producer2 <- &iterator.Msg{Iter: iter1}
		close(producer2)
		producers = append(producers, iterator.NewStream(0, producer2))
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, iterator.NewStreams(producers), res)
			return nil
		})
		msg, ok := <-res
		require.True(t, ok)
		require.Error(t, msg.Err)
		_, ok = <-res
		require.False(t, ok)
		err := pool.Wait()
		require.NoError(t, err)
	})
}

func TestFastPathOperationSetup(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	t.Run("should_error_when_resolver_panics", func(t *testing.T) {
		ctx := context.Background()
		errMessage := "deliberate panic in fastPathUnion"
		resolver := func(ctx context.Context, streams *iterator.Streams, outChan chan<- *iterator.Msg) {
			panic(errMessage)
		}

		outChan, err := fastPathOperationSetup(ctx, &ResolveCheckRequest{}, resolver)

		require.NoError(t, err)

		outcome := <-outChan
		require.ErrorContains(t, outcome.Err, errMessage)
		require.ErrorIs(t, outcome.Err, ErrPanic)
	})
}

func TestCheckUsersetFastPathV2(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
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
			},
			WithResultsSortedAscending: true,
		},
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
			},
			WithResultsSortedAscending: true,
		},
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
		val, err := checker.weight2Userset(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, []*openfgav1.RelationReference{{
			Type:               "group",
			RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "all"},
		}}, iter, "weight2")(ctx)
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
			},
			WithResultsSortedAscending: true,
		},
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
			},
			WithResultsSortedAscending: true,
		},
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
		val, err := checker.weight2Userset(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, []*openfgav1.RelationReference{{
			Type:               "group",
			RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "all"},
		}}, iter, "weight2")(ctx)
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
			},
			WithResultsSortedAscending: true,
		},
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
			},
			WithResultsSortedAscending: true,
		},
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
		val, err := checker.weight2Userset(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, []*openfgav1.RelationReference{{
			Type:               "group",
			RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "all"},
		}}, iter, "weight2")(ctx)
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
			},
			WithResultsSortedAscending: true,
		},
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
			},
			WithResultsSortedAscending: true,
		},
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
		val, err := checker.weight2Userset(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, []*openfgav1.RelationReference{{
			Type:               "group",
			RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "all"},
		}}, iter, "weight2")(ctx)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.False(t, val.GetAllowed())
	})

	t.Run("with_contextual_tuples_unsorted_works", func(t *testing.T) {
		storeID := ulid.Make().String()

		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define member1: [user]
					define member2: [user]
					define intersect: member1 and member2
			type folder
				relations
					define target: [group#intersect]
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)

		// left-hand side tuples returned by contextual tuples (unsorted) and DB (sorted)
		contextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:3", "member1", "user:maria"),
			tuple.NewTupleKey("group:2", "member1", "user:maria"),
			tuple.NewTupleKey("group:1", "member1", "user:maria"),
		}
		dbTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:1", "member2", "user:maria"),
			tuple.NewTupleKey("group:2", "member2", "user:maria"),
			tuple.NewTupleKey("group:3", "member2", "user:maria"),
		}

		// right-hand side tuples returned by DB
		usersetTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("folder:target", "target", "group:1#intersect"),
		}

		usersetIterator := storage.NewStaticTupleKeyIterator(usersetTuples)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "member1",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			WithResultsSortedAscending: true,
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).Times(1).
			Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "member2",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			WithResultsSortedAscending: true,
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).Times(1).
			Return(storage.NewStaticTupleIterator(testutils.ConvertTuplesKeysToTuples(dbTuples)), nil)

		ctx := setRequestContext(context.Background(), ts, mockDatastore, contextualTuples)

		checker := NewLocalChecker()
		checkResult, err := checker.weight2Userset(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("folder:target", "target", "user:maria"),
		}, []*openfgav1.RelationReference{{Type: "group", RelationOrWildcard: &openfgav1.RelationReference_Relation{Relation: "intersect"}}}, usersetIterator, "weight2")(ctx)
		require.NoError(t, err)
		require.NotNil(t, checkResult)
		require.True(t, checkResult.GetAllowed())
	})
}

func TestCheckTTUFastPathV2(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

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
			},
			WithResultsSortedAscending: true,
		},
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
			},
			WithResultsSortedAscending: true,
		},
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
		val, err := checker.weight2TTU(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, typesystem.TupleToUserset("parent", "all"), iter, "weight2")(ctx)
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
			},
			WithResultsSortedAscending: true,
		},
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
			},
			WithResultsSortedAscending: true,
		},
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
		val, err := checker.weight2TTU(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, typesystem.TupleToUserset("parent", "all"), iter, "weight2")(ctx)
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
			},
			WithResultsSortedAscending: true,
		},
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
			},
			WithResultsSortedAscending: true,
		},
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
		val, err := checker.weight2TTU(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, typesystem.TupleToUserset("parent", "all"), iter, "weight2")(ctx)
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
			},
			WithResultsSortedAscending: true,
		},
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
			},
			WithResultsSortedAscending: true,
		},
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
		val, err := checker.weight2TTU(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("document:1", "viewer", "user:1"),
		}, typesystem.TupleToUserset("parent", "all"), iter, "weight2")(ctx)
		require.NoError(t, err)
		require.NotNil(t, val)
		require.False(t, val.GetAllowed())
	})
	t.Run("with_contextual_tuples_unsorted_works", func(t *testing.T) {
		storeID := ulid.Make().String()
		model := testutils.MustTransformDSLToProtoWithID(`
			model
				schema 1.1
			type user
			type group
				relations
					define member1: [user]
					define member2: [user]
					define intersect: member1 and member2
			type folder
				relations
					define parent: [group]
					define target: intersect from parent
			`)

		ts, err := typesystem.New(model)
		require.NoError(t, err)
		ttuRewrite := typesystem.TupleToUserset("parent", "intersect")

		// left-hand side tuples (computed relation of TTU) returned by contextual tuples (unsorted) and DB (sorted)
		contextualTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:3", "member1", "user:maria"),
			tuple.NewTupleKey("group:2", "member1", "user:maria"),
			tuple.NewTupleKey("group:1", "member1", "user:maria"),
		}
		dbTuples := []*openfgav1.TupleKey{
			tuple.NewTupleKey("group:1", "member2", "user:maria"),
			tuple.NewTupleKey("group:2", "member2", "user:maria"),
			tuple.NewTupleKey("group:3", "member2", "user:maria"),
		}

		// right-hand side tuples (tupleset of TTU) returned by DB
		tuplesets := []*openfgav1.TupleKey{
			tuple.NewTupleKey("folder:target", "parent", "group:1"),
		}
		rightHandSideIterator := storage.NewStaticTupleKeyIterator(tuplesets)

		ctrl := gomock.NewController(t)
		defer ctrl.Finish()
		mockDatastore := mocks.NewMockOpenFGADatastore(ctrl)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "member1",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			WithResultsSortedAscending: true,
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).Times(1).
			Return(storage.NewStaticTupleIterator(nil), nil)
		mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), storeID, storage.ReadStartingWithUserFilter{
			ObjectType: "group",
			Relation:   "member2",
			UserFilter: []*openfgav1.ObjectRelation{{Object: "user:maria"}},
			ObjectIDs:  nil,
		}, storage.ReadStartingWithUserOptions{
			WithResultsSortedAscending: true,
			Consistency: storage.ConsistencyOptions{
				Preference: openfgav1.ConsistencyPreference_UNSPECIFIED,
			}},
		).Times(1).
			Return(storage.NewStaticTupleIterator(testutils.ConvertTuplesKeysToTuples(dbTuples)), nil)

		ctx := setRequestContext(context.Background(), ts, mockDatastore, contextualTuples)

		checker := NewLocalChecker()

		checkResult, err := checker.weight2TTU(ctx, &ResolveCheckRequest{
			StoreID:              storeID,
			AuthorizationModelID: ts.GetAuthorizationModelID(),
			TupleKey:             tuple.NewTupleKey("folder:target", "target", "user:maria"),
		}, ttuRewrite, rightHandSideIterator, "weight2")(ctx)
		require.NoError(t, err)
		require.NotNil(t, checkResult)
		require.True(t, checkResult.GetAllowed())
	})
}
