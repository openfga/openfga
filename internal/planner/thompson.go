package planner

import (
	"math"
	"math/rand"
	"sync/atomic"
	"time"
	"unsafe"

	"gonum.org/v1/gonum/stat/distuv"
)

// ThompsonStats holds the parameters for the Normal-gamma distribution,
// which models our belief about the performance (execution time) of a strategy.
type ThompsonStats struct {
	// All internal atomic values for lock-free concurrent access
	mu     atomic.Value // float64
	lambda atomic.Value // float64
	alpha  atomic.Value // float64
	beta   atomic.Value // float64
	runs   int64        // atomic access via atomic.AddInt64/LoadInt64

	// Cache frequently accessed values to avoid repeated atomic loads
	cachedParams unsafe.Pointer // *samplingParams - atomic access
}

type samplingParams struct {
	mu     float64
	lambda float64
	alpha  float64
	beta   float64
}

// Sample draws a random execution time from the learned distribution.
// This is the core of Thompson Sampling: we sample from our belief and act greedily on that sample.
func (ts *ThompsonStats) Sample(r *rand.Rand) float64 {
	// Load cached parameters atomically for best performance
	params := (*samplingParams)(atomic.LoadPointer(&ts.cachedParams))
	if params == nil {
		// Fallback to individual atomic loads if cache not ready (rare case)
		mu := ts.mu.Load().(float64)
		lambda := ts.lambda.Load().(float64)
		alpha := ts.alpha.Load().(float64)
		beta := ts.beta.Load().(float64)
		params = &samplingParams{mu, lambda, alpha, beta}
	}

	// Fast path gamma sampling using acceptance-rejection
	tau := ts.fastGammaSample(r, params.alpha, params.beta)

	// Fast normal sampling
	variance := 1.0 / (params.lambda * tau)
	if variance <= 0 {
		return params.mu
	}

	// Use standard normal * sqrt(variance) + mean for better performance
	stdNormal := r.NormFloat64()
	mean := params.mu + stdNormal*math.Sqrt(variance)

	return mean
}

// fastGammaSample implements a fast gamma sampler for common cases.
func (ts *ThompsonStats) fastGammaSample(r *rand.Rand, alpha, beta float64) float64 {
	// For alpha >= 1, use acceptance-rejection method (faster than gonum)
	if alpha >= 1.0 {
		d := alpha - 1.0/3.0
		c := 1.0 / math.Sqrt(9.0*d)

		for {
			x := r.NormFloat64()
			v := 1.0 + c*x
			if v <= 0 {
				continue
			}
			v = v * v * v
			u := r.Float64()
			if u < 1.0-0.0331*(x*x)*(x*x) {
				return d * v / beta
			}
			if math.Log(u) < 0.5*x*x+d*(1.0-v+math.Log(v)) {
				return d * v / beta
			}
		}
	}

	// Fallback to gonum for alpha < 1
	return distuv.Gamma{Alpha: alpha, Beta: beta, Src: r}.Rand()
}

// Update performs a Bayesian update on the distribution's parameters
// using the new data point (the observed execution duration).
func (ts *ThompsonStats) Update(duration time.Duration) {
	x := float64(duration.Nanoseconds()) / 1e6 // Convert to milliseconds with higher precision

	// Load current values atomically
	currentMu := ts.mu.Load().(float64)
	currentLambda := ts.lambda.Load().(float64)
	currentAlpha := ts.alpha.Load().(float64)
	currentBeta := ts.beta.Load().(float64)

	// Standard Bayesian update rules for a Normal-gamma distribution.
	newLambda := currentLambda + 1
	newMu := (currentLambda*currentMu + x) / newLambda
	newAlpha := currentAlpha + 0.5
	diff := x - currentMu
	newBeta := currentBeta + (currentLambda*diff*diff)/(2*newLambda)

	// Store new values atomically
	ts.mu.Store(newMu)
	ts.lambda.Store(newLambda)
	ts.alpha.Store(newAlpha)
	ts.beta.Store(newBeta)

	// Update cached parameters atomically for concurrent sampling
	newParams := &samplingParams{
		mu:     newMu,
		lambda: newLambda,
		alpha:  newAlpha,
		beta:   newBeta,
	}
	atomic.StorePointer(&ts.cachedParams, unsafe.Pointer(newParams))

	atomic.AddInt64(&ts.runs, 1)
}

// NewThompsonStats creates a new stats object with a diffuse prior,
// representing our initial uncertainty about a strategy's performance.
func NewThompsonStats(initialGuess time.Duration) *ThompsonStats {
	initialMs := float64(initialGuess.Nanoseconds()) / 1e6

	ts := &ThompsonStats{}

	// Initialize atomic values
	ts.mu.Store(initialMs)
	ts.lambda.Store(1.0) // Low confidence in the initial mean
	ts.alpha.Store(1.0)  // Diffuse prior for the shape
	ts.beta.Store(10.0)  // Diffuse prior for the rate (encourages higher variance initially)

	// Initialize cached parameters for concurrent access
	params := &samplingParams{
		mu:     initialMs,
		lambda: 1.0,
		alpha:  1.0,
		beta:   10.0,
	}
	atomic.StorePointer(&ts.cachedParams, unsafe.Pointer(params))

	return ts
}
