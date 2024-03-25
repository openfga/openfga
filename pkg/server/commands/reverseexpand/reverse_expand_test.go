package reverseexpand

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	gomock "go.uber.org/mock/gomock"

	"go.uber.org/goleak"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

func TestReverseExpandResultChannelClosed(t *testing.T) {
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

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).
		Times(1).
		DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
			iterator := storage.NewStaticTupleIterator(tuples)
			return iterator, nil
		})

	ctx := context.Background()

	resultChan := make(chan *ReverseExpandResult)
	errChan := make(chan error, 1)

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		t.Logf("before execute reverse expand")
		err := reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
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

		if err != nil {
			errChan <- err
		}
	}()

	select {
	case _, open := <-resultChan:
		if open {
			require.FailNow(t, "expected immediate closure of result channel")
		}
	case err := <-errChan:
		require.FailNow(t, "unexpected error received on error channel :%v", err)
	case <-time.After(30 * time.Millisecond):
		require.FailNow(t, "unexpected timeout on channel receive, expected receive on error channel")
	}
}

func TestReverseExpandRespectsContextCancellation(t *testing.T) {
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
		Times(1).
		DoAndReturn(func(_ context.Context, _ string, _ storage.ReadStartingWithUserFilter) (storage.TupleIterator, error) {
			// simulate many goroutines trying to write to the results channel
			iterator := storage.NewStaticTupleIterator(tuples)
			t.Logf("returning tuple iterator")
			return iterator, nil
		})
	ctx, cancelFunc := context.WithCancel(context.Background())

	resultChan := make(chan *ReverseExpandResult)
	errChan := make(chan error, 1)

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		t.Logf("before execute reverse expand")
		err := reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
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

		if err != nil {
			errChan <- err
		}
	}()
	go func() {
		// simulate max_results=1
		t.Logf("before receive one result")
		res := <-resultChan
		t.Logf("after receive one result")

		// send cancellation to the other goroutine
		cancelFunc()

		// this check it not the goal of this test, it's here just as sanity check
		if res.Object == "" {
			panic("expected object, got nil")
		}
		t.Logf("received object %s ", res.Object)
	}()

	select {
	case err := <-errChan:
		require.ErrorContains(t, err, "context canceled")
	case <-time.After(30 * time.Millisecond):
		require.FailNow(t, "unexpected timeout on channel receive, expected receive on error channel")
	}
}

func TestReverseExpandRespectsContextTimeout(t *testing.T) {
	defer goleak.VerifyNone(t)

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
	errChan := make(chan error, 1)

	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		err := reverseExpandQuery.Execute(timeoutCtx, &ReverseExpandRequest{
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

		if err != nil {
			errChan <- err
		}
	}()
	select {
	case _, open := <-resultChan:
		if !open {
			require.FailNow(t, "unexpected closure of result channel")
		}
	case err := <-errChan:
		require.Error(t, err)
	case <-time.After(1 * time.Second):
		require.FailNow(t, "unexpected timeout encountered, expected other receive")
	}
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
			iterator := mocks.NewErrorTupleIterator(tuples)
			return iterator, nil
		})

	ctx, cancelFunc := context.WithCancel(context.Background())
	defer cancelFunc()

	resultChan := make(chan *ReverseExpandResult)
	errChan := make(chan error, 1)

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
		err := reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
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
		if err != nil {
			errChan <- err
		}
	}()

ConsumerLoop:
	for {
		select {
		case _, open := <-resultChan:
			if !open {
				require.FailNow(t, "unexpected closure of result channel")
			}

			cancelFunc()
		case err := <-errChan:
			require.Error(t, err)
			break ConsumerLoop
		case <-time.After(30 * time.Millisecond):
			require.FailNow(t, "unexpected timeout waiting for channel receive, expected an error on the error channel")
		}
	}
}

func TestReverseExpandSendsAllErrorsThroughChannel(t *testing.T) {
	defer goleak.VerifyNone(t)

	store := ulid.Make().String()

	model := testutils.MustTransformDSLToProtoWithID(`model
  schema 1.1
type user
type document
  relations
    define viewer: [user]`)

	mockDatastore := mocks.NewMockSlowDataStorage(memory.New(), 1*time.Second)

	for i := 0; i < 50; i++ {
		t.Logf("iteration %d", i)
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(1*time.Nanosecond))
		t.Cleanup(cancel)

		resultChan := make(chan *ReverseExpandResult)
		errChan := make(chan error, 1)

		go func() {
			reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typesystem.New(model))
			t.Logf("before produce")
			err := reverseExpandQuery.Execute(ctx, &ReverseExpandRequest{
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
			t.Logf("after produce")

			if err != nil {
				errChan <- err
			}
		}()

		select {
		case _, channelOpen := <-resultChan:
			if !channelOpen {
				require.FailNow(t, "unexpected closure of result channel")
			}
		case err := <-errChan:
			require.Error(t, err)
		case <-time.After(3 * time.Second):
			require.FailNow(t, "unexpected timeout waiting for channel receive, expected an error on the error channel")
		}
	}
}
