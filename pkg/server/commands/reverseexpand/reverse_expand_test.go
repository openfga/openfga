package reverseexpand

import (
	"context"
	"sync"
	"testing"
	"time"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/golang/mock/gomock"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
)

func TestReverseExpandRespectsContextCancellation(t *testing.T) {
	store := ulid.Make().String()
	typeSystem := typesystem.New(&openfgav1.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define allowed: [user] as self
				    define viewer: [user] as self and allowed
				`),
	})
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).
		Times(0)
	ctx, cancelFunc := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(2)

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		resultChan := make(chan *ReverseExpandResult)

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
			require.ErrorIs(t, err, context.Canceled)
		} else {
			t.Errorf("expected an error but got none")
		}
		wg.Done()
	}()

	// cancel query in another goroutine
	go func() {
		cancelFunc()
		wg.Done()
	}()

	wg.Wait()
}

func TestReverseExpandRespectsContextTimeout(t *testing.T) {
	store := ulid.Make().String()
	typeSystem := typesystem.New(&openfgav1.AuthorizationModel{
		Id:            ulid.Make().String(),
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(`
				type user

				type document
				  relations
				    define allowed: [user] as self
				    define viewer: [user] as self and allowed
				`),
	})
	mockController := gomock.NewController(t)
	defer mockController.Finish()

	mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
	mockDatastore.EXPECT().ReadStartingWithUser(gomock.Any(), store, gomock.Any()).
		MaxTimes(2) // we expect it to be 0 most of the time

	timeoutCtx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	resultChan := make(chan *ReverseExpandResult)
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
		require.ErrorIs(t, err, context.DeadlineExceeded)
	} else {
		t.Errorf("expected an error but got none")
	}
}
