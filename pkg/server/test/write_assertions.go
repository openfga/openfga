package test

import (
	"context"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	serverconfig "github.com/openfga/openfga/internal/server/config"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server/commands"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
)

func TestWriteAssertions(t *testing.T, datastore storage.OpenFGADatastore) {
	type writeAssertionsTestSettings struct {
		_name   string
		request *openfgav1.WriteAssertionsRequest
		err     error
	}

	store := testutils.CreateRandomString(10)

	githubModelReq := &openfgav1.WriteAuthorizationModelRequest{
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
			request: &openfgav1.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgav1.Assertion{{
					TupleKey:    tuple.NewCheckRequestTupleKey("repo:test", "reader", "user:elbuo"),
					Expectation: false,
				}},
			},
		},
		{
			_name: "writing_assertions_succeeds_when_it_is_not_directly_assignable",
			request: &openfgav1.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgav1.Assertion{{
					TupleKey:    tuple.NewCheckRequestTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
				}},
			},
		},
		{
			_name: "writing_empty_assertions_succeeds",
			request: &openfgav1.WriteAssertionsRequest{
				StoreId:    store,
				Assertions: []*openfgav1.Assertion{},
			},
		},
		{
			_name: "writing_assertion_with_invalid_relation_fails",
			request: &openfgav1.WriteAssertionsRequest{
				StoreId: store,
				Assertions: []*openfgav1.Assertion{
					{
						TupleKey: tuple.NewCheckRequestTupleKey(
							"repo:test",
							"invalidrelation",
							"user:elbuo",
						),
						Expectation: false,
					},
				},
			},
			err: serverErrors.ValidationError(
				fmt.Errorf("relation 'repo#invalidrelation' not found"),
			),
		},
	}

	ctx := context.Background()
	logger := logger.NewNoopLogger()

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			model := githubModelReq

			writeAuthzModelCmd := commands.NewWriteAuthorizationModelCommand(
				datastore, logger, serverconfig.DefaultMaxAuthorizationModelSizeInBytes,
			)

			modelID, err := writeAuthzModelCmd.Execute(ctx, model)
			require.NoError(t, err)

			test.request.AuthorizationModelId = modelID.AuthorizationModelId

			writeAssetionCmd := commands.NewWriteAssertionsCommand(datastore, logger)
			_, err = writeAssetionCmd.Execute(ctx, test.request)
			require.ErrorIs(t, test.err, err)
		})
	}
}
