package check

import (
	"context"
	"testing"

	v1parser "github.com/craigpastro/openfga-dsl-parser"
	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/assets"
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

type CheckTestClientInterface interface {
	tests.TestClientBootstrapper
	Check(ctx context.Context, in *openfgapb.CheckRequest, opts ...grpc.CallOption) (*openfgapb.CheckResponse, error)
}

// RunSchema1_1CheckTests is public so can be run when OpenFGA is used as a
// library. An OpenFGA server needs to be running and the client parameter is
// a client for the server.
func RunSchema1_1CheckTests(t *testing.T, client CheckTestClientInterface) {
	runTests(t, typesystem.SchemaVersion1_1, client)
}

// RunSchema1_0CheckTests is the 1.0 version of RunSchema1_1CheckTests.
func RunSchema1_0CheckTests(t *testing.T, client CheckTestClientInterface) {
	runTests(t, typesystem.SchemaVersion1_0, client)
}

func runTests(t *testing.T, schemaVersion string, client CheckTestClientInterface) {
	var b []byte
	var err error
	if schemaVersion == typesystem.SchemaVersion1_1 {
		b, err = assets.EmbedTests.ReadFile("tests/consolidate_1_1_tests.yaml")
	} else {
		b, err = assets.EmbedTests.ReadFile("tests/consolidate_1_0_tests.yaml")
	}
	require.NoError(t, err)

	var testCases checkTests
	err = yaml.Unmarshal(b, &testCases)
	require.NoError(t, err)

	ctx := context.Background()

	for _, test := range testCases.Tests {
		resp, err := client.CreateStore(ctx, &openfgapb.CreateStoreRequest{Name: test.Name})
		require.NoError(t, err)

		storeID := resp.GetId()

		t.Run(test.Name, func(t *testing.T) {
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
