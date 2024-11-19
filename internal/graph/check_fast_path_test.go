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

func TestPollIteratorProducers(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("should_exit_on_context_cancelled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := pollIteratorProducers(ctx, []*iteratorProducer{{done: false}})
		require.Equal(t, context.Canceled, err)
	})
	t.Run("should_error_out_with_producer_errors", func(t *testing.T) {
		ctx := context.Background()
		c := make(chan *iteratorMsg, 1)
		expectedErr := errors.New("boom")
		c <- &iteratorMsg{err: expectedErr}
		_, err := pollIteratorProducers(ctx, []*iteratorProducer{{done: true}, {done: false, producer: c}})
		require.Equal(t, expectedErr, err)
	})
	t.Run("should_filter_out_drained_producers", func(t *testing.T) {
		ctx := context.Background()
		producers := make([]*iteratorProducer, 0)
		expectedLen := 0
		for i := 0; i < 5; i++ {
			c := make(chan *iteratorMsg, 1)
			producer := &iteratorProducer{producer: c}
			if i%2 == 0 {
				close(c)
			} else {
				expectedLen++
				c <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator(nil)}
			}
			producers = append(producers, producer)
		}
		res, err := pollIteratorProducers(ctx, producers)
		require.NoError(t, err)
		require.Len(t, res, expectedLen)
	})
	t.Run("should_return_empty_when_fully_drained", func(t *testing.T) {
		ctx := context.Background()
		producers := make([]*iteratorProducer, 0)
		for i := 0; i < 5; i++ {
			producer := &iteratorProducer{iter: nil, done: true}
			producers = append(producers, producer)
		}
		res, err := pollIteratorProducers(ctx, producers)
		require.NoError(t, err)
		require.Empty(t, res)
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
		producers := make([]*iteratorProducer, 0)
		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Stop().Times(1)
		producer := make(chan *iteratorMsg, 1)
		producer <- &iteratorMsg{iter: iter1}
		close(producer)
		producers = append(producers, &iteratorProducer{producer: producer})

		pool := concurrency.NewPool(context.Background(), 1)
		pool.Go(func(ctx context.Context) error {
			cancellableCtx, cancel := context.WithCancel(ctx)
			cancel()
			fastPathUnion(cancellableCtx, producers, res)
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
		producers := make([]*iteratorProducer, 0)
		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return(nil, errors.New("boom"))
		iter1.EXPECT().Stop().Times(1)
		producer1 := make(chan *iteratorMsg, 1)
		producer1 <- &iteratorMsg{iter: iter1}
		close(producer1)
		producers = append(producers, &iteratorProducer{producer: producer1})

		iter2 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter2.EXPECT().Stop().Times(1)
		producer2 := make(chan *iteratorMsg, 1)
		producer2 <- &iteratorMsg{iter: iter2}
		close(producer2)
		producers = append(producers, &iteratorProducer{producer: producer2})

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathUnion(ctx, producers, res)
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
		producers := make([]*iteratorProducer, 0, 4)

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
		producers = append(producers, &iteratorProducer{producer: producer1})

		producer2 := make(chan *iteratorMsg, 1)
		producer2 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "2",
		}})}
		close(producer2)
		producers = append(producers, &iteratorProducer{producer: producer2})

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
		producers = append(producers, &iteratorProducer{producer: producer3})

		producer4 := make(chan *iteratorMsg, 2)
		producer4 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "4",
		}})}
		producer4 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "8",
		}})}
		close(producer4)
		producers = append(producers, &iteratorProducer{producer: producer4})

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathUnion(ctx, producers, res)
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
}

func TestFastPathIntersection(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("should_return_on_context_cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iteratorMsg)
		producers := make([]*iteratorProducer, 0)
		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Stop().Times(1)
		producer := make(chan *iteratorMsg, 1)
		producer <- &iteratorMsg{iter: iter1}
		close(producer)
		producers = append(producers, &iteratorProducer{producer: producer})

		pool := concurrency.NewPool(context.Background(), 1)
		pool.Go(func(ctx context.Context) error {
			cancellableCtx, cancel := context.WithCancel(ctx)
			cancel()
			fastPathIntersection(cancellableCtx, producers, res)
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
		producers := make([]*iteratorProducer, 0)
		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return(nil, errors.New("boom"))
		iter1.EXPECT().Stop().Times(1)
		producer1 := make(chan *iteratorMsg, 1)
		producer1 <- &iteratorMsg{iter: iter1}
		close(producer1)
		producers = append(producers, &iteratorProducer{producer: producer1})
		iter2 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter2.EXPECT().Stop().Times(1)
		producer2 := make(chan *iteratorMsg, 1)
		producer2 <- &iteratorMsg{iter: iter2}
		close(producer2)
		producers = append(producers, &iteratorProducer{producer: producer2})
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, producers, res)
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
		producers := make([]*iteratorProducer, 0, 4)

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
		producers = append(producers, &iteratorProducer{producer: producer1})

		producer2 := make(chan *iteratorMsg, 2)
		producer2 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "1",
		}})}
		producer2 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "2",
		}})}
		close(producer2)
		producers = append(producers, &iteratorProducer{producer: producer2})

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
		producers = append(producers, &iteratorProducer{producer: producer3})

		producer4 := make(chan *iteratorMsg, 2)
		producer4 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "1",
		}})}
		producer4 <- &iteratorMsg{iter: storage.NewStaticTupleKeyIterator([]*openfgav1.TupleKey{{
			Object: "4",
		}})}
		close(producer4)
		producers = append(producers, &iteratorProducer{producer: producer4})

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathIntersection(ctx, producers, res)
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
}

func TestFastPathDifference(t *testing.T) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})
	t.Run("should_return_on_context_cancelled", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		res := make(chan *iteratorMsg)
		producers := make([]*iteratorProducer, 0)

		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Stop().Times(1)
		producer1 := make(chan *iteratorMsg, 1)
		producer1 <- &iteratorMsg{iter: iter1}
		close(producer1)
		producers = append(producers, &iteratorProducer{producer: producer1})

		iter2 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter2.EXPECT().Stop().Times(1)
		producer2 := make(chan *iteratorMsg, 1)
		producer2 <- &iteratorMsg{iter: iter2}
		close(producer2)
		producers = append(producers, &iteratorProducer{producer: producer2})

		pool := concurrency.NewPool(context.Background(), 1)
		pool.Go(func(ctx context.Context) error {
			cancellableCtx, cancel := context.WithCancel(ctx)
			cancel()
			fastPathDifference(cancellableCtx, producers, res)
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
		producers := make([]*iteratorProducer, 0)
		iter1 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter1.EXPECT().Head(gomock.Any()).MaxTimes(1).Return(nil, errors.New("boom"))
		iter1.EXPECT().Stop().Times(1)
		producer1 := make(chan *iteratorMsg, 1)
		producer1 <- &iteratorMsg{iter: iter1}
		close(producer1)
		producers = append(producers, &iteratorProducer{producer: producer1})
		iter2 := mocks.NewMockIterator[*openfgav1.TupleKey](ctrl)
		iter2.EXPECT().Stop().Times(1)
		producer2 := make(chan *iteratorMsg, 1)
		producer2 <- &iteratorMsg{iter: iter2}
		close(producer2)
		producers = append(producers, &iteratorProducer{producer: producer2})
		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, producers, res)
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
		producers := make([]*iteratorProducer, 0, 2)

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
		producers = append(producers, &iteratorProducer{producer: producer1})

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
		producers = append(producers, &iteratorProducer{producer: producer2})

		pool := concurrency.NewPool(ctx, 1)
		pool.Go(func(ctx context.Context) error {
			fastPathDifference(ctx, producers, res)
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
}
