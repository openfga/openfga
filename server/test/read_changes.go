package test

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type testCase struct {
	_name                            string
	request                          *openfgapb.ReadChangesRequest
	expectedError                    error
	expectedChanges                  []*openfgapb.TupleChange
	expectEmptyContinuationToken     bool
	saveContinuationTokenForNextTest bool
}

var tkMaria = &openfgapb.TupleKey{
	Object:   "repo:auth0/openfgapb",
	Relation: "admin",
	User:     "maria",
}
var tkMariaOrg = &openfgapb.TupleKey{
	Object:   "org:auth0",
	Relation: "member",
	User:     "maria",
}
var tkCraig = &openfgapb.TupleKey{
	Object:   "repo:auth0/openfgapb",
	Relation: "admin",
	User:     "craig",
}
var tkYamil = &openfgapb.TupleKey{
	Object:   "repo:auth0/openfgapb",
	Relation: "admin",
	User:     "yamil",
}

func newReadChangesRequest(store, objectType, contToken string, pageSize int32) *openfgapb.ReadChangesRequest {
	return &openfgapb.ReadChangesRequest{
		StoreId:           store,
		Type:              objectType,
		ContinuationToken: contToken,
		PageSize:          wrapperspb.Int32(pageSize),
	}
}

func TestReadChanges(t *testing.T, datastore storage.OpenFGADatastore) {
	store := testutils.CreateRandomString(10)
	ctx, backend, tracer, err := setup(store, datastore)
	require.NoError(t, err)

	encrypter, err := encrypter.NewGCMEncrypter("key")
	require.NoError(t, err)

	encoder := encoder.NewTokenEncoder(encrypter, encoder.NewBase64Encoder())

	t.Run("read changes without type", func(t *testing.T) {
		testCases := []testCase{
			{
				_name:   "request with pageSize=2 returns 2 tuple and a token",
				request: newReadChangesRequest(store, "", "", 2),
				expectedChanges: []*openfgapb.TupleChange{
					{
						TupleKey:  tkMaria,
						Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
					},
					{
						TupleKey:  tkCraig,
						Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
					},
				},
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: true,
			},
			{
				_name:   "request with previous token returns all remaining changes",
				request: newReadChangesRequest(store, "", "", storage.DefaultPageSize),
				expectedChanges: []*openfgapb.TupleChange{
					{
						TupleKey:  tkYamil,
						Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
					},
					{
						TupleKey:  tkMariaOrg,
						Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
					},
				},
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: true,
			},
			{
				_name:                            "request with previous token returns no more changes",
				request:                          newReadChangesRequest(store, "", "", storage.DefaultPageSize),
				expectedChanges:                  nil,
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: false,
			},
			{
				_name:                            "request with invalid token returns invalid token error",
				request:                          newReadChangesRequest(store, "", "foo", storage.DefaultPageSize),
				expectedChanges:                  nil,
				expectEmptyContinuationToken:     false,
				expectedError:                    serverErrors.InvalidContinuationToken,
				saveContinuationTokenForNextTest: false,
			},
		}

		readChangesQuery := commands.NewReadChangesQuery(backend, tracer, logger.NewNoopLogger(), encoder, 0)
		runTests(t, ctx, testCases, readChangesQuery)
	})

	t.Run("read changes with type", func(t *testing.T) {
		testCases := []testCase{
			{
				_name:                        "if no tuples with type, return empty changes and no token",
				request:                      newReadChangesRequest(store, "type-not-found", "", 1),
				expectedChanges:              nil,
				expectEmptyContinuationToken: true,
				expectedError:                nil,
			},
			{
				_name:   "if 1 tuple with 'org type', read changes with 'org' filter returns 1 change and a token",
				request: newReadChangesRequest(store, "org", "", storage.DefaultPageSize),
				expectedChanges: []*openfgapb.TupleChange{{
					TupleKey:  tkMariaOrg,
					Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
				}},
				expectEmptyContinuationToken: false,
				expectedError:                nil,
			},
			{
				_name:   "if 2 tuples with 'repo' type, read changes with 'repo' filter and page size of 1 returns 1 change and a token",
				request: newReadChangesRequest(store, "repo", "", 1),
				expectedChanges: []*openfgapb.TupleChange{{
					TupleKey:  tkMaria,
					Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
				}},
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: true,
			}, {
				_name:   "using the token from the previous test yields 1 change and a token",
				request: newReadChangesRequest(store, "repo", "", storage.DefaultPageSize),
				expectedChanges: []*openfgapb.TupleChange{{
					TupleKey:  tkCraig,
					Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
				}, {
					TupleKey:  tkYamil,
					Operation: openfgapb.TupleOperation_TUPLE_OPERATION_WRITE,
				}},
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: true,
			}, {
				_name:                            "using the token from the previous test yields 0 changes and a token",
				request:                          newReadChangesRequest(store, "repo", "", storage.DefaultPageSize),
				expectedChanges:                  nil,
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: true,
			}, {
				_name:         "using the token from the previous test yields an error because the types in the token and the request don't match",
				request:       newReadChangesRequest(store, "does-not-match", "", storage.DefaultPageSize),
				expectedError: serverErrors.MismatchObjectType,
			},
		}

		readChangesQuery := commands.NewReadChangesQuery(backend, tracer, logger.NewNoopLogger(), encoder, 0)
		runTests(t, ctx, testCases, readChangesQuery)
	})

	t.Run("read changes with horizon offset", func(t *testing.T) {
		testCases := []testCase{
			{
				_name: "when the horizon offset is non-zero no tuples should be returned",
				request: &openfgapb.ReadChangesRequest{
					StoreId: store,
				},
				expectedChanges:              nil,
				expectEmptyContinuationToken: true,
				expectedError:                nil,
			},
		}

		readChangesQuery := commands.NewReadChangesQuery(backend, tracer, logger.NewNoopLogger(), encoder, 2)
		runTests(t, ctx, testCases, readChangesQuery)
	})
}

func runTests(t *testing.T, ctx context.Context, testCasesInOrder []testCase, readChangesQuery *commands.ReadChangesQuery) {
	ignoreStateOpts := cmpopts.IgnoreUnexported(openfgapb.Tuple{}, openfgapb.TupleKey{}, openfgapb.TupleChange{})
	ignoreTimestampOpts := cmpopts.IgnoreFields(openfgapb.TupleChange{}, "Timestamp")

	var res *openfgapb.ReadChangesResponse
	var err error
	for i, test := range testCasesInOrder {
		if i >= 1 {
			previousTest := testCasesInOrder[i-1]
			if previousTest.saveContinuationTokenForNextTest {
				previousToken := res.ContinuationToken
				test.request.ContinuationToken = previousToken
			}
		}
		res, err = readChangesQuery.Execute(ctx, test.request)

		if test.expectedError == nil && err != nil {
			t.Errorf("[%s] Expected no error but got '%s'", test._name, err)
		}

		if test.expectedError != nil && err == nil {
			t.Errorf("[%s] Expected an error '%s' but got nothing", test._name, test.expectedError)
		}

		if test.expectedError != nil && err != nil && !strings.Contains(test.expectedError.Error(), err.Error()) {
			t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.expectedError, err)
		}

		if res != nil {
			if diff := cmp.Diff(res.Changes, test.expectedChanges, ignoreStateOpts, ignoreTimestampOpts, cmpopts.EquateEmpty()); diff != "" {
				t.Errorf("[%s] tuple change mismatch (-got +want):\n%s", test._name, diff)
			}
			if test.expectEmptyContinuationToken && res.ContinuationToken != "" {
				t.Errorf("[%s] continuation token mismatch. Expected empty, got %v", test._name, res.ContinuationToken)
			}
			if !test.expectEmptyContinuationToken && res.ContinuationToken == "" {
				t.Errorf("[%s] continuation token mismatch. Expected not empty, got empty", test._name)
			}
		}
	}
}

func TestReadChangesReturnsSameContTokenWhenNoChanges(t *testing.T, ds storage.OpenFGADatastore) {
	store := testutils.CreateRandomString(10)
	ctx, backend, tracer, err := setup(store, ds)
	require.NoError(t, err)

	readChangesQuery := commands.NewReadChangesQuery(backend, tracer, logger.NewNoopLogger(), encoder.NewBase64Encoder(), 0)

	res1, err := readChangesQuery.Execute(ctx, newReadChangesRequest(store, "", "", storage.DefaultPageSize))
	require.NoError(t, err)

	res2, err := readChangesQuery.Execute(ctx, newReadChangesRequest(store, "", res1.GetContinuationToken(), storage.DefaultPageSize))
	require.NoError(t, err)

	require.Equal(t, res1.ContinuationToken, res2.ContinuationToken)
}

func setup(store string, datastore storage.OpenFGADatastore) (context.Context, storage.ChangelogBackend, trace.Tracer, error) {
	ctx := context.Background()
	tracer := telemetry.NewNoopTracer()

	writes := []*openfgapb.TupleKey{tkMaria, tkCraig, tkYamil, tkMariaOrg}
	err := datastore.Write(ctx, store, []*openfgapb.TupleKey{}, writes)
	if err != nil {
		return nil, nil, nil, err
	}

	return ctx, datastore, tracer, nil
}
