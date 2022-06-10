package commands

import (
	"context"
	"testing"

	"github.com/go-errors/errors"
	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/logger"
	serverErrors "github.com/openfga/openfga/server/errors"
	mockstorage "github.com/openfga/openfga/storage/mocks"
	openfgav1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const writeTestStore = "auth0"

func TestWriteAssertions(t *testing.T) {
	type writeAssertionsTestSettings struct {
		_name    string
		setup    func()
		request  *openfgav1.WriteAssertionsRequest
		err      error
		response *openfgav1.WriteAssertionsResponse
	}

	logger := logger.NewNoopLogger()
	ctx := context.Background()
	ignoreStateOpts := cmpopts.IgnoreUnexported(openfgav1.WriteAssertionsResponse{})

	mockController := gomock.NewController(t)
	defer mockController.Finish()
	mockDatastore := mockstorage.NewMockOpenFGADatastore(mockController)
	cmd := NewWriteAssertionsCommand(mockDatastore, logger)

	td := &openfgav1.TypeDefinition{
		Type: "repo",
		Relations: map[string]*openfgav1.Userset{
			"reader": {Userset: &openfgav1.Userset_This{}},
		},
	}

	var tests = []writeAssertionsTestSettings{
		{
			_name: "succeeds with one assertion",
			setup: func() {
				mockDatastore.EXPECT().ReadTypeDefinition(ctx, writeTestStore, gomock.Any(), "repo").Times(1).Return(td, nil)
				mockDatastore.EXPECT().WriteAssertions(ctx, writeTestStore, gomock.Any(), gomock.Any()).Times(1).Return(nil)
			},
			request: &openfgav1.WriteAssertionsRequest{
				StoreId: writeTestStore,
				Assertions: []*openfgav1.Assertion{{
					TupleKey: &openfgav1.TupleKey{
						Object:   "repo:test",
						Relation: "reader",
						User:     "elbuo",
					},
					Expectation: false,
				}},
			},
			err:      nil,
			response: &openfgav1.WriteAssertionsResponse{},
		},
		{
			_name: "succeeds with two conflicting assertions",
			setup: func() {
				mockDatastore.EXPECT().ReadTypeDefinition(ctx, writeTestStore, gomock.Any(), "repo").Times(2).Return(td, nil)
				mockDatastore.EXPECT().WriteAssertions(ctx, writeTestStore, gomock.Any(), gomock.Any()).Times(1).Return(nil)
			},
			request: &openfgav1.WriteAssertionsRequest{
				StoreId: writeTestStore,
				Assertions: []*openfgav1.Assertion{{
					TupleKey: &openfgav1.TupleKey{
						Object:   "repo:test",
						Relation: "reader",
						User:     "elbuo",
					},
					Expectation: false,
				}, {
					TupleKey: &openfgav1.TupleKey{
						Object:   "repo:test",
						Relation: "reader",
						User:     "elbuo",
					},
					Expectation: true,
				}},
			},
			err:      nil,
			response: &openfgav1.WriteAssertionsResponse{},
		},
		{
			_name: "fails with invalid relation in assertion",
			setup: func() {
				mockDatastore.EXPECT().ReadTypeDefinition(ctx, writeTestStore, gomock.Any(), "repo").Times(1).Return(td, nil)
			},
			request: &openfgav1.WriteAssertionsRequest{
				StoreId: writeTestStore,
				Assertions: []*openfgav1.Assertion{{
					TupleKey: &openfgav1.TupleKey{
						Object:   "repo:test",
						Relation: "invalidrelation",
						User:     "elbuo",
					},
					Expectation: false,
				}},
			},
			err: serverErrors.RelationNotFound("invalidrelation", "repo", &openfgav1.TupleKey{
				Object:   "repo:test",
				Relation: "invalidrelation",
				User:     "elbuo",
			}),
			response: nil,
		},
		{
			_name: "fails when storage fails to save assertions",
			setup: func() {
				mockDatastore.EXPECT().ReadTypeDefinition(ctx, writeTestStore, gomock.Any(), "repo").Times(1).Return(td, nil)
				mockDatastore.EXPECT().WriteAssertions(ctx, writeTestStore, gomock.Any(), gomock.Any()).Times(1).Return(errors.New("storage error"))
			},
			request: &openfgav1.WriteAssertionsRequest{
				StoreId: writeTestStore,
				Assertions: []*openfgav1.Assertion{{
					TupleKey: &openfgav1.TupleKey{
						Object:   "repo:test",
						Relation: "reader",
						User:     "elbuo",
					},
					Expectation: false,
				}},
			},
			err:      status.Error(codes.Code(openfgav1.InternalErrorCode_internal_error), "Internal Server Error"),
			response: nil,
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			test.setup()
			actualResponse, actualError := cmd.Execute(ctx, test.request)

			if test.err != nil {
				if actualError == nil {
					t.Errorf("Expected error '%s', but got none", test.err)
				}
				if test.err.Error() != actualError.Error() {
					t.Errorf("Expected error '%s', actual '%s'", test.err, actualError)
				}
			}
			if test.err == nil && actualError != nil {
				t.Errorf("Did not expect an error but got one: %v", actualError)
			}

			if actualResponse == nil && test.err == nil {
				t.Error("Expected non nil response, got nil")
			} else {
				if diff := cmp.Diff(actualResponse, test.response, ignoreStateOpts, cmpopts.EquateEmpty()); diff != "" {
					t.Errorf("Unexpected response (-got +want):\n%s", diff)
				}
			}
		})
	}

}
