package check

import (
	"context"
	"testing"

	v1parser "github.com/craigpastro/openfga-dsl-parser"
	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/assets"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	"gopkg.in/yaml.v3"
)

type checkTests struct {
	Tests []struct {
		Name   string
		Stages []*stage
	}
}

// stage is a stage of a test. All stages will be run in a single store.
type stage struct {
	Model           string
	Tuples          []*openfgapb.TupleKey
	CheckAssertions []*assertion `yaml:"checkAssertions"`
}

type assertion struct {
	Tuple            *openfgapb.TupleKey
	ContextualTuples []*openfgapb.TupleKey `yaml:"contextualTuples"`
	Expectation      bool
	ErrorCode        int `yaml:"errorCode"` // If ErrorCode is non-zero then we expect that the check call failed.
	Trace            string
}

// ClientInterface defines client interface for running check tests
type ClientInterface interface {
	tests.TestClientBootstrapper
	Check(ctx context.Context, in *openfgapb.CheckRequest, opts ...grpc.CallOption) (*openfgapb.CheckResponse, error)
}

// RunAllTests will run all check tests
func RunAllTests(t *testing.T, client ClientInterface) {
	t.Run("RunAllTests", func(t *testing.T) {
		t.Run("Check", func(t *testing.T) {
			t.Parallel()
			testCheck(t, client)
		})
		t.Run("BadAuthModelID", func(t *testing.T) {
			t.Parallel()
			testBadAuthModelID(t, client)
		})
	})
}

func testCheck(t *testing.T, client ClientInterface) {
	t.Run("Schema1_1", func(t *testing.T) {
		t.Parallel()
		runSchema1_1CheckTests(t, client)
	})
	t.Run("Schema1_0", func(t *testing.T) {
		t.Parallel()
		runSchema1_0CheckTests(t, client)
	})
}

func testBadAuthModelID(t *testing.T, client ClientInterface) {

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &openfgapb.CreateStoreRequest{Name: "bad auth id"})
	require.NoError(t, err)

	storeID := resp.GetId()
	model := `
	type user

	type doc
	  relations
	    define viewer: [user] as self
	    define can_view as viewer
	`
	_, err = client.WriteAuthorizationModel(ctx, &openfgapb.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(model),
	})
	require.NoError(t, err)
	const badModelID = "01GS89AJC3R3PFQ9BNY5ZF6Q97"
	_, err = client.Check(ctx, &openfgapb.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tuple.NewTupleKey("doc:x", "viewer", "user:y"),
		AuthorizationModelId: badModelID,
	})

	require.ErrorIs(t, err, serverErrors.AuthorizationModelNotFound(badModelID))
}

func runSchema1_1CheckTests(t *testing.T, client ClientInterface) {
	runTests(t, typesystem.SchemaVersion1_1, client)
}

func runSchema1_0CheckTests(t *testing.T, client ClientInterface) {
	runTests(t, typesystem.SchemaVersion1_0, client)
}

func runTests(t *testing.T, schemaVersion string, client ClientInterface) {
	var b []byte
	var err error
	if schemaVersion == typesystem.SchemaVersion1_1 {
		b, err = assets.EmbedTests.ReadFile("tests/consolidated_1_1_tests.yaml")
	} else {
		b, err = assets.EmbedTests.ReadFile("tests/consolidated_1_0_tests.yaml")
	}
	require.NoError(t, err)

	var testCases checkTests
	err = yaml.Unmarshal(b, &testCases)
	require.NoError(t, err)

	ctx := context.Background()

	for _, test := range testCases.Tests {
		test := test

		t.Run(test.Name, func(t *testing.T) {
			t.Parallel()
			resp, err := client.CreateStore(ctx, &openfgapb.CreateStoreRequest{Name: test.Name})
			require.NoError(t, err)

			storeID := resp.GetId()

			for _, stage := range test.Stages {

				var typedefs []*openfgapb.TypeDefinition
				if schemaVersion == typesystem.SchemaVersion1_1 {
					typedefs = parser.MustParse(stage.Model)
				} else {
					typedefs = v1parser.MustParse(stage.Model)
				}

				_, err = client.WriteAuthorizationModel(ctx, &openfgapb.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   schemaVersion,
					TypeDefinitions: typedefs,
				})
				require.NoError(t, err)

				if len(stage.Tuples) > 0 {
					_, err = client.Write(ctx, &openfgapb.WriteRequest{
						StoreId: storeID,
						Writes:  &openfgapb.TupleKeys{TupleKeys: stage.Tuples},
					})
					require.NoError(t, err)
				}

				for _, assertion := range stage.CheckAssertions {
					resp, err := client.Check(ctx, &openfgapb.CheckRequest{
						StoreId:  storeID,
						TupleKey: assertion.Tuple,
						ContextualTuples: &openfgapb.ContextualTupleKeys{
							TupleKeys: assertion.ContextualTuples,
						},
						Trace: true,
					})

					if assertion.ErrorCode == 0 {
						require.NoError(t, err)
						require.Equal(t, assertion.Expectation, resp.Allowed, assertion)
					} else {
						require.Error(t, err)
						e, ok := status.FromError(err)
						require.True(t, ok)
						require.Equal(t, assertion.ErrorCode, int(e.Code()))
					}
				}
			}
		})
	}
}
