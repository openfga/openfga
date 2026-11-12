package listusers

import (
	"testing"

	"go.uber.org/goleak"

	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/tests"
)

func TestMatrixMemory(t *testing.T) {
	runMatrixWithEngine(t, "memory")
}

func TestMatrixPostgres(t *testing.T) {
	runMatrixWithEngine(t, "postgres")
}

func runMatrixWithEngine(t *testing.T, engine string) {
	t.Cleanup(func() {
		goleak.VerifyNone(t)
	})

	// Check runs with the weighted graph resolver: it is the specification the matrix verifies against.
	client := tests.BuildClientInterface(t, engine, []string{
		config.ExperimentalWeightedGraphCheck,
		config.ExperimentalInlineExpressions,
	})
	RunMatrixTests(t, engine, "weighted graph check", client)
}
