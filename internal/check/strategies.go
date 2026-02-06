package check

import (
	"strings"
	"time"

	"github.com/openfga/openfga/internal/planner"
)

const DefaultStrategyName = "default"

var DefaultPlan = &planner.PlanConfig{
	Name:         DefaultStrategyName,
	InitialGuess: 50 * time.Millisecond,
	// Low Lambda: Represents zero confidence. It's a pure guess.
	Lambda: 1,
	// With α = 0.5 ≤ 1, it means maximum uncertainty about variance; with λ = 1, we also have weak confidence in the mean.
	// These values will encourage strong exploration of other strategies. Having these values for the default execute helps to enforce the usage of the "faster" strategies,
	// helping out with the cold start when we don't have enough data.
	Alpha: 0.5,
	Beta:  0.5,
}

var DefaultRecursivePlan = &planner.PlanConfig{
	Name:         DefaultStrategyName,
	InitialGuess: 500 * time.Millisecond, // Higher initial guess for recursive checks
	// Low Lambda: Represents zero confidence. It's a pure guess.
	Lambda: 1,
	// Moderate Alpha/Beta values create a balanced belief curve, telling the planner
	// to expect variations but with higher confidence than before.
	// Low expected precision: 𝐸[𝜏]= 𝛼/𝛽 = 3.0/2.0 = 1.5.
	// High expected variance: E[σ2]= β/(α−1) =2.0/(3.0−1) = 1.0, This allows for variance but is less jittery than previous settings
	// Tighter tolerance for spread: α = 3 indicates a narrower uncertainty than α = 2, meaning we are more certain about the variance range.
	// When α > β, we expect higher precision and more controlled variance.
	Alpha: 3.0,
	Beta:  2.0,
}

const WeightTwoStrategyName = "weight2"

// This strategy is configured to show that it has proven fast and consistent.
var weight2Plan = &planner.PlanConfig{
	Name:         WeightTwoStrategyName,
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
var RecursivePlan = &planner.PlanConfig{
	Name:         RecursiveStrategyName,
	InitialGuess: 150 * time.Millisecond,
	// Medium Lambda: Represents medium confidence in the initial guess. It's like
	// starting with the belief of having already seen 5 good runs.
	Lambda: 3.0,
	// UNCERTAINTY ABOUT CONSISTENCY: The gap between p50 and p99 is large.
	// Moderate Alpha/Beta values create a balanced belief curve, telling the planner
	// to expect variations but with higher confidence than before.
	// Low expected precision: 𝐸[𝜏]= 𝛼/𝛽 = 3.0/2.0 = 1.5.
	// High expected variance: E[σ2]= β/(α−1) =2.0/(3.0−1) = 1.0, This allows for variance but is less jittery than previous settings
	// Tighter tolerance for spread: α = 3 indicates a narrower uncertainty than α = 2, meaning we are more certain about the variance range.
	// When α > β, we expect higher precision and more controlled variance.
	Alpha: 3.0,
	Beta:  2.0,
}

func createUsersetPlanKey(req *Request, userset string) string {
	var b strings.Builder
	b.WriteString("v2|")
	b.WriteString("userset|")
	b.WriteString(req.GetAuthorizationModelID())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(req.GetObjectType())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(req.GetTupleKey().GetRelation())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(req.GetUserType())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(userset)
	return b.String()
}

func createRecursiveUsersetPlanKey(req *Request, userset string) string {
	var b strings.Builder
	b.WriteString("v2|")
	b.WriteString("userset|")
	b.WriteString(req.GetAuthorizationModelID())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(userset)
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(req.GetUserType())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString("infinite")
	return b.String()
}

func createRecursiveTTUPlanKey(req *Request, tuplesetRelation string) string {
	var b strings.Builder
	b.WriteString("v2|")
	b.WriteString("ttu|")
	b.WriteString(req.GetAuthorizationModelID())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(tuplesetRelation)
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(req.GetUserType())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString("infinite")
	return b.String()
}

func createTTUPlanKey(req *Request, tuplesetRelation, computedRelation string) string {
	var b strings.Builder
	b.WriteString("v2|")
	b.WriteString("ttu|")
	b.WriteString(req.GetAuthorizationModelID())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(req.GetObjectType())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(req.GetTupleKey().GetRelation())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(req.GetUserType())
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(tuplesetRelation)
	b.WriteString(cacheKeyDelimiter)
	b.WriteString(computedRelation)
	return b.String()
}
