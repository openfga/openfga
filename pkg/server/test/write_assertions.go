package test

import (
	"context"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/google/go-cmp/cmp"
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
	"google.golang.org/protobuf/testing/protocmp"
)

func TestWriteAssertions(t *testing.T, datastore storage.OpenFGADatastore) {
	type writeAssertionsTestSettings struct {
		_name      string
		assertions []*openfgav1.Assertion
		err        error
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
			assertions: []*openfgav1.Assertion{{
				TupleKey:    tuple.NewTupleKey("repo:test", "reader", "user:elbuo"),
				Expectation: false,
			}},
		},
		{
			_name: "writing_assertions_succeeds_when_it_is_not_directly_assignable",
			assertions: []*openfgav1.Assertion{{
				TupleKey:    tuple.NewTupleKey("repo:test", "can_read", "user:elbuo"),
				Expectation: false,
			}},
		},
		{
			_name: "writing_multiple_assertions_succeeds",
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewTupleKey("repo:test", "reader", "user:elbuo"),
					Expectation: false,
				},
				{
					TupleKey:    tuple.NewTupleKey("repo:test", "reader", "user:maria"),
					Expectation: true,
				},
				{
					TupleKey:    tuple.NewTupleKey("repo:test", "reader", "user:jon"),
					Expectation: false,
				},
				{
					TupleKey:    tuple.NewTupleKey("repo:test", "reader", "user:jose"),
					Expectation: true,
				},
			},
		},
		{
			_name: "writing_multiple_assertions_succeeds_when_it_is_not_directly_assignable",
			assertions: []*openfgav1.Assertion{
				{
					TupleKey:    tuple.NewTupleKey("repo:test", "can_read", "user:elbuo"),
					Expectation: false,
				},
				{
					TupleKey:    tuple.NewTupleKey("repo:test", "can_read", "user:maria"),
					Expectation: false,
				},
				{
					TupleKey:    tuple.NewTupleKey("repo:test", "can_read", "user:jon"),
					Expectation: true,
				},
			},
		},
		{
			_name:      "writing_empty_assertions_succeeds",
			assertions: []*openfgav1.Assertion{},
		},
		{
			_name: "writing_assertion_with_invalid_relation_fails",
			assertions: []*openfgav1.Assertion{
				{
					TupleKey: tuple.NewTupleKey(
						"repo:test",
						"invalidrelation",
						"user:elbuo",
					),
					Expectation: false,
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
			request := &openfgav1.WriteAssertionsRequest{
				StoreId:              store,
				Assertions:           test.assertions,
				AuthorizationModelId: modelID.AuthorizationModelId,
			}

			writeAssertionCmd := commands.NewWriteAssertionsCommand(datastore, logger)
			_, err = writeAssertionCmd.Execute(ctx, request)
			require.ErrorIs(t, test.err, err)
			if err == nil {
				query := commands.NewReadAssertionsQuery(datastore, logger)
				actualResponse, actualError := query.Execute(ctx, store, modelID.AuthorizationModelId)
				require.NoError(t, actualError)

				expectedResponse := &openfgav1.ReadAssertionsResponse{
					AuthorizationModelId: modelID.AuthorizationModelId,
					Assertions:           test.assertions,
				}
				if diff := cmp.Diff(expectedResponse, actualResponse, protocmp.Transform()); diff != "" {
					t.Errorf("store mismatch (-want +got):\n%s", diff)
				}
			}
		})
	}
}
