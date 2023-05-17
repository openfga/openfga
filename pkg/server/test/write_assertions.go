package test

import (
	"context"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
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
		TypeDefinitions: parser.MustParse(`
		type user

		type repo
		  relations
		    define reader: [user] as self
		    define can_read as reader
		`),
		SchemaVersion: typesystem.SchemaVersion1_1,
	}

	var tests = []writeAssertionsTestSettings{
		{
			_name: "writing_assertions_succeeds",
			request: &openfgapb.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgapb.Assertion{{
					TupleKey:    tuple.NewTupleKey("repo:test", "reader", "user:elbuo"),
					Expectation: false,
				}},
			},
		},
		{
			_name: "writing_assertions_succeeds_when_it_is_not_directly_assignable",
			request: &openfgapb.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgapb.Assertion{{
					TupleKey:    tuple.NewTupleKey("repo:test", "can_read", "user:elbuo"),
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
						TupleKey:    tuple.NewTupleKey("repo:test", "invalidrelation", "user:elbuo"),
						Expectation: false,
					},
				},
			},
			err: serverErrors.ValidationError(fmt.Errorf("relation 'repo#invalidrelation' not found")),
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {

		t.Run(test._name, func(t *testing.T) {
			model := githubModelReq

			modelID, err := commands.NewWriteAuthorizationModelCommand(datastore, logger).Execute(ctx, model)
			require.NoError(t, err)

			cmd := commands.NewWriteAssertionsCommand(datastore, logger)
			test.request.AuthorizationModelId = modelID.AuthorizationModelId

			_, err = cmd.Execute(ctx, test.request)
			require.ErrorIs(t, test.err, err)
		})
	}
}
