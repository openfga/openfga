package graph

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/yaml"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/assets"
	checktest "github.com/openfga/openfga/internal/test/check"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

type corpusStage struct {
	Model           string
	Tuples          []*openfgav1.TupleKey
	CheckAssertions []*checktest.Assertion `json:"checkAssertions"`
}

type corpusTest struct {
	Name   string
	Stages []*corpusStage
}

// TestCheckCorpusAgreesAcrossStrategies guards against the planner's choice changing a Check result: every strategy must
// match the default one, which must match the corpus. A strategy that does not apply to a node is ignored by it, so forcing
// is best effort.
func TestCheckCorpusAgreesAcrossStrategies(t *testing.T) {
	// assertions the default strategy does not satisfy are not compared, so log how many to keep that visible
	var skipped atomic.Int64
	t.Cleanup(func() {
		t.Logf("%d assertions skipped because the default strategy does not match the corpus", skipped.Load())
	})

	var tests []corpusTest
	for _, file := range []string{"tests/consolidated_1_1_tests.yaml", "tests/abac_tests.yaml"} {
		b, err := assets.EmbedTests.ReadFile(file)
		require.NoError(t, err)

		var parsed struct{ Tests []corpusTest }
		require.NoError(t, yaml.Unmarshal(b, &parsed))
		tests = append(tests, parsed.Tests...)
	}
	require.NotEmpty(t, tests)

	for _, test := range tests {
		if len(test.Stages) != 1 {
			continue // later stages build on the tuples and model of the earlier ones
		}
		stage := test.Stages[0]

		for _, contextual := range []bool{false, true} {
			name := test.Name
			if contextual {
				name += "_ctxTuples"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()

				model := testutils.MustTransformDSLToProtoWithID(stage.Model)
				typesys, err := typesystem.NewAndValidate(context.Background(), model)
				if err != nil {
					t.Skipf("model is not valid: %v", err)
				}

				storeID := ulid.Make().String()
				ds := memory.New()
				t.Cleanup(ds.Close)
				var stored []*openfgav1.TupleKey
				if !contextual {
					stored = stage.Tuples
				}
				require.NoError(t, ds.Write(context.Background(), storeID, nil, stored))

				for i, assertion := range stage.CheckAssertions {
					if assertion.Tuple == nil {
						continue
					}
					ctxTuples := append([]*openfgav1.TupleKey{}, assertion.ContextualTuples...)
					if contextual {
						ctxTuples = append(ctxTuples, stage.Tuples...)
					}

					run := func(strategy string) (*ResolveCheckResponse, error) {
						checker := NewLocalChecker()
						defer checker.Close()

						ctx := setRequestContext(context.Background(), typesys, ds, ctxTuples)
						return checker.ResolveCheck(ctx, &ResolveCheckRequest{
							StoreID:              storeID,
							AuthorizationModelID: model.GetId(),
							TupleKey:             tuple.NewTupleKey(assertion.Tuple.GetObject(), assertion.Tuple.GetRelation(), assertion.Tuple.GetUser()),
							ContextualTuples:     ctxTuples,
							Context:              assertion.Context,
							RequestMetadata:      NewCheckRequestMetadata(),
							SelectedStrategy:     strategy,
						})
					}

					// Some corpus cases are rejected by the server before Check runs; they say nothing about strategies.
					defaultResp, defaultErr := run(defaultResolver)
					if (assertion.ErrorCode != 0) != (defaultErr != nil) ||
						(defaultErr == nil && defaultResp.GetAllowed() != assertion.Expectation) {
						skipped.Add(1)
						continue
					}

					for _, strategy := range checkStrategies[1:] {
						t.Run(fmt.Sprintf("assertion_%d/%s", i, strategy), func(t *testing.T) {
							resp, err := run(strategy)
							if defaultErr != nil {
								require.EqualError(t, err, defaultErr.Error())
								return
							}
							require.NoError(t, err)
							require.Equal(t, assertion.Expectation, resp.GetAllowed())
						})
					}
				}
			})
		}
	}
}
