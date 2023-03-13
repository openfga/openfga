package test

import (
	"context"
	"fmt"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
)

func TestWriteAssertions(t *testing.T, datastore storage.OpenFGADatastore) {
	type writeAssertionsTestSettings struct {
		_name         string
		request       *openfgapb.WriteAssertionsRequest
		allowSchema10 bool
		assertModel10 bool
		err           error
	}

	store := testutils.CreateRandomString(10)

	githubModelReq := &openfgapb.WriteAuthorizationModelRequest{
		StoreId: store,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"reader":   {Userset: &openfgapb.Userset_This{}},
					"can_read": typesystem.ComputedUserset("reader"),
				},
				Metadata: &openfgapb.Metadata{
					Relations: map[string]*openfgapb.RelationMetadata{
						"reader": {
							DirectlyRelatedUserTypes: []*openfgapb.RelationReference{
								{
									Type: "user",
								},
							},
						},
					},
				},
			},
		},
		SchemaVersion: typesystem.SchemaVersion1_1,
	}

	githubModelReq10 := &openfgapb.WriteAuthorizationModelRequest{
		StoreId: store,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "repo",
				Relations: map[string]*openfgapb.Userset{
					"reader":   {Userset: &openfgapb.Userset_This{}},
					"can_read": typesystem.ComputedUserset("reader"),
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
						User:     "user:elbuo",
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
						User:     "user:elbuo",
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
							User:     "user:elbuo",
						},
						Expectation: false,
					},
				},
			},
			err: serverErrors.ValidationError(fmt.Errorf("relation 'repo#invalidrelation' not found")),
		},
		{
			_name: "writing_assertions_obsolete",
			request: &openfgapb.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgapb.Assertion{{
					TupleKey: &openfgapb.TupleKey{
						Object:   "repo:test",
						Relation: "reader",
						User:     "user:elbuo",
					},
					Expectation: false,
				}},
			},
			assertModel10: true,
			allowSchema10: false,
			err:           serverErrors.ObsoleteAuthorizationModel,
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {

		t.Run(test._name, func(t *testing.T) {
			model := githubModelReq
			if test.assertModel10 {
				model = githubModelReq10
			}

			modelID, err := commands.NewWriteAuthorizationModelCommand(datastore, logger, true).Execute(ctx, model)
			if err != nil {
				t.Fatal(err)
			}

			cmd := commands.NewWriteAssertionsCommand(datastore, logger, test.allowSchema10)
			test.request.AuthorizationModelId = modelID.AuthorizationModelId

			_, err = cmd.Execute(ctx, test.request)
			require.ErrorIs(t, test.err, err)
		})
	}
}
