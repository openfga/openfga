package queries

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"go.buf.build/openfga/go/openfga/api/openfga"
	openfgav1pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type testCase struct {
	_name                            string
	request                          *openfgav1pb.ReadChangesRequest
	expectedError                    error
	expectedChanges                  []*openfga.TupleChange
	expectEmptyContinuationToken     bool
	saveContinuationTokenForNextTest bool
}

var tkMaria = &openfga.TupleKey{
	Object:   "repo:auth0/openfga",
	Relation: "admin",
	User:     "maria",
}
var tkMariaOrg = &openfga.TupleKey{
	Object:   "org:auth0",
	Relation: "member",
	User:     "maria",
}
var tkCraig = &openfga.TupleKey{
	Object:   "repo:auth0/openfga",
	Relation: "admin",
	User:     "craig",
}
var tkYamil = &openfga.TupleKey{
	Object:   "repo:auth0/openfga",
	Relation: "admin",
	User:     "yamil",
}

func newReadChangesRequest(objectType, contToken string, pageSize int32) *openfgav1pb.ReadChangesRequest {
	return &openfgav1pb.ReadChangesRequest{
		StoreId:           testStore,
		Type:              objectType,
		ContinuationToken: contToken,
		PageSize:          wrapperspb.Int32(pageSize),
	}
}

func TestReadChanges(t *testing.T) {
	ctx, backend, tracer, err := setup()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("read changes without type", func(t *testing.T) {
		testCases := []testCase{
			{
				_name:   "request with pageSize=1 returns 1 tuple and a token",
				request: newReadChangesRequest("", "", 1),
				expectedChanges: []*openfga.TupleChange{
					{
						TupleKey:  tkMaria,
						Operation: openfga.TupleOperation_WRITE,
					},
				},
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: true,
			},
			{
				_name:   "request with previous token returns all remaining changes",
				request: newReadChangesRequest("", "", storage.DefaultPageSize),
				expectedChanges: []*openfga.TupleChange{
					{
						TupleKey:  tkCraig,
						Operation: openfga.TupleOperation_WRITE,
					},
					{
						TupleKey:  tkYamil,
						Operation: openfga.TupleOperation_WRITE,
					},
					{
						TupleKey:  tkMariaOrg,
						Operation: openfga.TupleOperation_WRITE,
					},
				},
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: true,
			},
			{
				_name:                            "request with previous token returns no more changes",
				request:                          newReadChangesRequest("", "", storage.DefaultPageSize),
				expectedChanges:                  nil,
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: false,
			},
			{
				_name:                            "request with invalid token returns invalid token error",
				request:                          newReadChangesRequest("", "foo", storage.DefaultPageSize),
				expectedChanges:                  nil,
				expectEmptyContinuationToken:     false,
				expectedError:                    serverErrors.InvalidContinuationToken,
				saveContinuationTokenForNextTest: false,
			},
		}

		encoder, err := encoder.NewTokenEncrypter("key")
		if err != nil {
			t.Fatal(err)
		}
		readChangesQuery := NewReadChangesQuery(backend, tracer, logger.NewNoopLogger(), encoder, 0)
		runTests(t, ctx, testCases, readChangesQuery)
	})

	t.Run("read changes with type", func(t *testing.T) {
		testCases := []testCase{
			{
				_name:                        "if no tuples with type, return empty changes and no token",
				request:                      newReadChangesRequest("type-not-found", "", 1),
				expectedChanges:              nil,
				expectEmptyContinuationToken: true,
				expectedError:                nil,
			},
			{
				_name:   "if 1 tuple with 'org type', read changes with 'org' filter returns 1 change and a token",
				request: newReadChangesRequest("org", "", storage.DefaultPageSize),
				expectedChanges: []*openfga.TupleChange{{
					TupleKey:  tkMariaOrg,
					Operation: openfga.TupleOperation_WRITE,
				}},
				expectEmptyContinuationToken: false,
				expectedError:                nil,
			},
			{
				_name:   "if 2 tuples with 'repo' type, read changes with 'repo' filter and page size of 1 returns 1 change and a token",
				request: newReadChangesRequest("repo", "", 1),
				expectedChanges: []*openfga.TupleChange{{
					TupleKey:  tkMaria,
					Operation: openfga.TupleOperation_WRITE,
				}},
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: true,
			}, {
				_name:   "using the token from the previous test yields 1 change and a token",
				request: newReadChangesRequest("repo", "", storage.DefaultPageSize),
				expectedChanges: []*openfga.TupleChange{{
					TupleKey:  tkCraig,
					Operation: openfga.TupleOperation_WRITE,
				}, {
					TupleKey:  tkYamil,
					Operation: openfga.TupleOperation_WRITE,
				}},
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: true,
			}, {
				_name:                            "using the token from the previous test yields 0 changes and a token",
				request:                          newReadChangesRequest("repo", "", storage.DefaultPageSize),
				expectedChanges:                  nil,
				expectEmptyContinuationToken:     false,
				expectedError:                    nil,
				saveContinuationTokenForNextTest: true,
			}, {
				_name:         "using the token from the previous test yields an error because the types in the token and the request don't match",
				request:       newReadChangesRequest("does-not-match", "", storage.DefaultPageSize),
				expectedError: serverErrors.MismatchObjectType,
			},
		}

		readChangesQuery := NewReadChangesQuery(backend, tracer, logger.NewNoopLogger(), encoder.Noop{}, 0)
		runTests(t, ctx, testCases, readChangesQuery)
	})

	t.Run("read changes with horizon offset", func(t *testing.T) {
		testCases := []testCase{
			{
				_name: "when the horizon offset is non-zero no tuples should be returned",
				request: &openfgav1pb.ReadChangesRequest{
					StoreId: testStore,
				},
				expectedChanges:              nil,
				expectEmptyContinuationToken: true,
				expectedError:                nil,
			},
		}

		readChangesQuery := NewReadChangesQuery(backend, tracer, logger.NewNoopLogger(), encoder.Noop{}, 2)
		runTests(t, ctx, testCases, readChangesQuery)
	})
}

func runTests(t *testing.T, ctx context.Context, testCasesInOrder []testCase, readChangesQuery *ReadChangesQuery) {
	var res *openfgav1pb.ReadChangesResponse
	var actualError error
	ignoreStateOpts := cmpopts.IgnoreUnexported(openfga.Tuple{}, openfga.TupleKey{}, openfga.TupleChange{})
	ignoreTimestampOpts := cmpopts.IgnoreFields(openfga.TupleChange{}, "Timestamp")
	for i, test := range testCasesInOrder {
		if i >= 1 {
			previousTest := testCasesInOrder[i-1]
			if previousTest.saveContinuationTokenForNextTest {
				previousToken := res.ContinuationToken
				test.request.ContinuationToken = previousToken
			}
		}
		res, actualError = readChangesQuery.Execute(ctx, test.request)

		if test.expectedError == nil && actualError != nil {
			t.Errorf("[%s] Expected no error but got '%s'", test._name, actualError)
		}

		if test.expectedError != nil && actualError == nil {
			t.Errorf("[%s] Expected an error '%s' but got nothing", test._name, test.expectedError)
		}

		if test.expectedError != nil && actualError != nil && !strings.Contains(test.expectedError.Error(), actualError.Error()) {
			t.Errorf("[%s] Expected error '%s', actual '%s'", test._name, test.expectedError, actualError)
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

func TestReadChangesReturnsSameContTokenWhenNoChanges(t *testing.T) {
	ctx, backend, tracer, err := setup()
	if err != nil {
		t.Fatal(err)
	}
	readChangesQuery := NewReadChangesQuery(backend, tracer, logger.NewNoopLogger(), encoder.Noop{}, 0)

	res1, err := readChangesQuery.Execute(ctx, newReadChangesRequest("", "", storage.DefaultPageSize))
	if err != nil {
		t.Fatal(err)
	}

	res2, err := readChangesQuery.Execute(ctx, newReadChangesRequest("", res1.GetContinuationToken(), storage.DefaultPageSize))
	if err != nil {
		t.Fatal(err)
	}

	if res1.ContinuationToken != res2.ContinuationToken {
		t.Errorf("expected ==, but got %s != %s", res1.ContinuationToken, res2.ContinuationToken)
	}
}

func setup() (context.Context, storage.ChangelogBackend, trace.Tracer, error) {
	ctx := context.Background()
	tracer := otel.Tracer("noop")
	backend, err := testutils.BuildAllBackends(tracer)
	if err != nil {
		return nil, nil, nil, err
	}

	writes := []*openfga.TupleKey{tkMaria, tkCraig, tkYamil, tkMariaOrg}
	if err := backend.TupleBackend.Write(ctx, testStore, []*openfga.TupleKey{}, writes); err != nil {
		return nil, nil, nil, err
	}

	return ctx, backend.ChangelogBackend, tracer, nil
}
