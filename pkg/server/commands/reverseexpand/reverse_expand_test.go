package reverseexpand

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReverseExpandRespectsContextCancellation(t *testing.T) {
	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`model
  schema 1.1
type user
type document
  relations
	define viewer: [user]`)

	typeSystem := typesystem.New(model)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	var tuples []*openfgav1.Tuple
	for i := 0; i < 100; i++ {
		obj := fmt.Sprintf("document:%s", strconv.Itoa(i))
		tuples = append(tuples, &openfgav1.Tuple{Key: tuple.NewTupleKey(obj, "viewer", "user:maria")})
	}

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).
		Times(1).
		DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
			// simulate many goroutines trying to write to the results channel
			iterator := storage.NewStaticTupleIterator(tuples)
			t.Logf("returning tuple iterator")
			return iterator, nil
		})
	ctx, cancelFunc := context.WithCancel(context.Background())

	resultChan := make(chan *ReverseExpandResult)

	done := make(chan struct{})

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		t.Logf("before execute reverse expand")
		reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
			StoreID:    store,
			ObjectType: "document",
			Relation:   "viewer",
			User: &UserRefObject{
				Object: &openfgav1.Object{
					Type: "user",
					Id:   "maria",
				},
			},
			ContextualTuples: []*openfgav1.TupleKey{},
		}, resultChan, NewResolutionMetadata())
		t.Logf("after execute reverse expand")
		done <- struct{}{}
	}()
	go func() {
		// simulate max_results=1
		t.Logf("before receive one result")
		res := <-resultChan
		t.Logf("after receive one result")
		cancelFunc()
		t.Logf("after send cancellation")
		require.NotNil(t, res.Object)
		require.NoError(t, res.Err)
	}()

	select {
	case <-done:
		t.Log("OK!")
		return
	case <-time.After(30 * time.Millisecond):
		require.FailNow(t, "timed out")
	}
}

func TestReverseExpandRespectsContextTimeout(t *testing.T) {
	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`model
  schema 1.1
type user
type document
  relations
	define allowed: [user]
	define viewer: [user] and allowed`)

	typeSystem := typesystem.New(model)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).
		MaxTimes(2) // we expect it to be 0 most of the time

	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	resultChan := make(chan *ReverseExpandResult)
	done := make(chan struct{})

	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		reverseExpandQuery.Execute(timeoutCtx, &ReverseExpandRequest{
			StoreID:    store,
			ObjectType: "document",
			Relation:   "viewer",
			User: &UserRefObject{
				Object: &openfgav1.Object{
					Type: "user",
					Id:   "maria",
				},
			},
			ContextualTuples: []*openfgav1.TupleKey{},
		}, resultChan, NewResolutionMetadata())
		done <- struct{}{}
	}()
	select {
	case res, open := <-resultChan:
		if open {
			require.Error(t, res.Err)
		} else {
			require.Nil(t, res)
		}
	case <-done:
		// OK!
	}
}

// errorIterator is a mock iterator that returns error when calling next on the second Next call
type errorIterator[T any] struct {
	items          []T
	originalLength int
}

func (s *errorIterator[T]) Next(ctx context.Context) (T, error) {
	var val T

	if ctx.Err() != nil {
		return val, ctx.Err()
	}

	if len(s.items) != s.originalLength {
		return val, fmt.Errorf("simulated errors")
	}

	if len(s.items) == 0 {
		return val, nil
	}

	next, rest := s.items[0], s.items[1:]
	s.items = rest

	return next, nil
}

func (s *errorIterator[T]) Stop() {}

func newErrorIterator(tuples []*openfgav1.Tuple) storage.TupleIterator {
	iter := &errorIterator[*openfgav1.Tuple]{
		items:          tuples,
		originalLength: len(tuples),
	}

	return iter
}

func TestReverseExpandErrorInTuples(t *testing.T) {
	defer goleak.VerifyNone(t)

	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`model
  schema 1.1
type user
type document
  relations
	define viewer: [user]`)

	typeSystem := typesystem.New(model)
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	var tuples []*openfgav1.Tuple
	for i := 0; i < 100; i++ {
		obj := fmt.Sprintf("document:%s", strconv.Itoa(i))
		tuples = append(tuples, &openfgav1.Tuple{Key: tuple.NewTupleKey(obj, "viewer", "user:maria")})
	}

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).
		DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
			iterator := newErrorIterator(tuples)
			t.Logf("returning tuple iterator")
			return iterator, nil
		})
	ctx, cancelFunc := context.WithCancel(context.Background())

	resultChan := make(chan *ReverseExpandResult)

	done := make(chan struct{})

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		t.Logf("before execute reverse expand")
		reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
			StoreID:    store,
			ObjectType: "document",
			Relation:   "viewer",
			User: &UserRefObject{
				Object: &openfgav1.Object{
					Type: "user",
					Id:   "maria",
				},
			},
			ContextualTuples: []*openfgav1.TupleKey{},
		}, resultChan, NewResolutionMetadata())
		t.Logf("after execute reverse expand")
		done <- struct{}{}
	}()

	go func() {
		<-resultChan
		<-resultChan
		cancelFunc()
	}()

	select {
	case <-done:
		t.Log("OK!")
		return
	case <-time.After(30 * time.Millisecond):
		require.FailNow(t, "timed out")
	}
}
