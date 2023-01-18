package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/openfga/openfga/internal/server/commands"
	serverErrors "github.com/openfga/openfga/internal/server/errors"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestWriteAssertions(t *testing.T, datastore storage.OpenFGADatastore) {
	type writeAssertionsTestSettings struct {
		_name   string
		request *openfgapb.WriteAssertionsRequest
		err     error
	}

	store := testutils.CreateRandomString(10)

	githubModelReq := &openfgapb.WriteAuthorizationModelRequest{
		StoreId: store,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"reader": {Userset: &openfgapb.Userset_This{}},
					"can_read": {
						Userset: &openfgapb.Userset_ComputedUserset{
							ComputedUserset: &openfgapb.ObjectRelation{
								Relation: "reader",
							},
						}},
				},
			},
		},
	}

	var tests = []writeAssertionsTestSettings{
		{
			_name: "writing_assertions_succeeds",
			request: &openfgapb.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgapb.Assertion{{
					TupleKey: &openfgapb.TupleKey{
						Object:   "repo:test",
						Relation: "reader",
						User:     "elbuo",
					},
					Expectation: false,
				}},
			},
		},
		{
			_name: "writing_assertions_succeeds_when_it_is_not_directly_assignable",
			request: &openfgapb.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgapb.Assertion{{
					TupleKey: &openfgapb.TupleKey{
						Object:   "repo:test",
						Relation: "can_read",
						User:     "elbuo",
					},
					Expectation: false,
				}},
			},
		},
		{
			_name: "writing_empty_assertions_succeeds",
			request: &openfgapb.WriteAssertionsRequest{
				StoreId:    store,
				Assertions: []*openfgapb.Assertion{},
			},
		},
		{
			_name: "writing_assertion_with_invalid_relation_fails",
			request: &openfgapb.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgapb.Assertion{
					{
						TupleKey: &openfgapb.TupleKey{
							Object:   "repo:test",
							Relation: "invalidrelation",
							User:     "elbuo",
						},
						Expectation: false,
					},
				},
			},
			err: serverErrors.ValidationError(fmt.Errorf("relation 'repo#invalidrelation' not found")),
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	modelID, err := commands.NewWriteAuthorizationModelCommand(datastore, logger).Execute(ctx, githubModelReq)
	if err != nil {
		t.Fatal(err)
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			cmd := commands.NewWriteAssertionsCommand(datastore, logger)
			test.request.AuthorizationModelId = modelID.AuthorizationModelId

			_, err := cmd.Execute(ctx, test.request)
			require.ErrorIs(t, test.err, err)
		})
	}
}
