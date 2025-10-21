package check

import (
	"context"
	"time"

	authzGraph "github.com/openfga/language/pkg/go/graph"

	"github.com/openfga/openfga/internal/planner"
	"github.com/openfga/openfga/pkg/storage"
)

type Strategy interface {
	Userset(context.Context, *Request, *authzGraph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator) (*Response, error)
	TTU(context.Context, *Request, *authzGraph.WeightedAuthorizationModelEdge, storage.TupleKeyIterator) (*Response, error)
}

const DefaultStrategyName = "default"

var DefaultPlan = &planner.KeyPlanStrategy{
	Type:         DefaultStrategyName,
	InitialGuess: 50 * time.Millisecond,
	// Low Lambda: Represents zero confidence. It's a pure guess.
	Lambda: 1,
	// With α = 0.5 ≤ 1, it means maximum uncertainty about variance; with λ = 1, we also have weak confidence in the mean.
	// These values will encourage strong exploration of other strategies. Having these values for the default execute helps to enforce the usage of the "faster" strategies,
	// helping out with the cold start when we don't have enough data.
	Alpha: 0.5,
	Beta:  0.5,
}

var DefaultRecursivePlan = &planner.KeyPlanStrategy{
	Type:         DefaultStrategyName,
	InitialGuess: 300 * time.Millisecond, // Higher initial guess for recursive checks
	// Low Lambda: Represents zero confidence. It's a pure guess.
	Lambda: 1,
	// With α = 0.5 ≤ 1, it means maximum uncertainty about variance; with λ = 1, we also have weak confidence in the mean.
	// These values will encourage strong exploration of other strategies. Having these values for the default execute helps to enforce the usage of the "faster" strategies,
	// helping out with the cold start when we don't have enough data.
	Alpha: 0.5,
	Beta:  0.5,
}

const WeightTwoStrategyName = "weight2"

// This strategy is configured to show that it has proven fast and consistent.
var weight2Plan = &planner.KeyPlanStrategy{
	Type:         WeightTwoStrategyName,
	InitialGuess: 20 * time.Millisecond,
	// High Lambda: Represents strong confidence in the initial guess. It's like
	// starting with the belief of having already seen 10 good runs.
	Lambda: 10.0,
	// High Alpha, Low Beta: Creates a very NARROW belief about variance.
	// This tells the planner: "I am very confident that the performance is
	// consistently close to 10ms". A single slow run will be a huge surprise
	// and will dramatically shift this belief.

	// High expected precision: 𝐸[𝜏]= 𝛼/𝛽 = 20/2 = 10
	// Low expected variance: E[σ2]= β/(α−1) =2/9 = 0.105, narrow jitter
	// A slow sample will look like an outlier and move the posterior noticeably but overall this prior exploits.
	Alpha: 20,
	Beta:  2,
}

const RecursiveStrategyName = "recursive"

// In general these values tell the query planner that the recursive strategy usually performs around 150 ms but occasionally spikes.
// However, even when it spikes we want to keep it using it or exploring it despite variance, rather than over-penalizing single slow runs.
var RecursivePlan = &planner.KeyPlanStrategy{
	Type:         RecursiveStrategyName,
	InitialGuess: 150 * time.Millisecond,
	// Medium Lambda: Represents medium confidence in the initial guess. It's like
	// starting with the belief of having already seen 5 good runs.
	Lambda: 5.0,
	// UNCERTAINTY ABOUT CONSISTENCY: The gap between p50 and p99 is large.
	// Low Alpha/Beta values create a wider belief curve, telling the planner
	// to expect and not be overly surprised by performance variations.
	// Low expected precision: 𝐸[𝜏]= 𝛼/𝛽 = 2.0/2.5 = 0.8.
	// High expected variance: E[σ2]= β/(α−1) =2.5/1 = 2.5, this will allow for relative bursty / jiterry results.
	// Wide tolerance for spread: 𝛼 = 2, this will allow for considerable uncertainty in how spike the latency can be.
	// When β > α, we expect lower precision and higher variance
	Alpha: 2.0,
	Beta:  2.5,
}