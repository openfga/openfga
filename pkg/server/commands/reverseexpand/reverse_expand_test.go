package reverseexpand

import (
	"context"
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
	cancelFunc()

	resultChan := make(chan *ReverseExpandResult)

	// process query in one goroutine, but it will be cancelled almost right away
	go func() {
		reverseExpandQuery := NewReverseExpandQuery(mockDatastore, typeSystem)
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
	}()

	res, open := <-resultChan
	if open && res.Err != nil {
		require.ErrorIs(t, res.Err, context.Canceled)
	} else {
		t.Errorf("expected an error but got none")
	}
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
	}()
	res, open := <-resultChan
	if open && res.Err != nil {
		require.ErrorIs(t, res.Err, context.DeadlineExceeded)
	} else {
		t.Errorf("expected an error but got none")
	}
}
