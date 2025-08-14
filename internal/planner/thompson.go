package planner

import (
	"math/rand"
	"time"

	"gonum.org/v1/gonum/stat/distuv"
)

// ThompsonStats holds the parameters for the Normal-gamma distribution,
// which models our belief about the performance (execution time) of a strategy.
type ThompsonStats struct {
	// We model the distribution of execution times. Lower is better.
	// Mu is the estimated mean execution time.
	Mu float64
	// Lambda is a scaling factor for the precision.
	Lambda float64
	// Alpha is the shape parameter of the Gamma distribution for the precision.
	Alpha float64
	// Beta is the rate parameter of the Gamma distribution for the precision.
	Beta float64
	// Runs is the total number of data points observed.
	Runs int64
}

// Sample draws a random execution time from the learned distribution.
// This is the core of Thompson Sampling: we sample from our belief and act greedily on that sample.
func (ts *ThompsonStats) Sample(r *rand.Rand) float64 {
	// 1. Sample the precision (tau) from the Gamma distribution.
	tau := distuv.Gamma{Alpha: ts.Alpha, Beta: ts.Beta, Src: r}.Rand()

	// 2. Sample the mean (mu) from the Normal distribution, using the sampled precision.
	// The variance of the normal distribution is 1 / (lambda * tau).
	variance := 1 / (ts.Lambda * tau)
	mean := distuv.Normal{Mu: ts.Mu, Sigma: variance, Src: r}.Rand()

	return mean
}

// Update performs a Bayesian update on the distribution's parameters
// using the new data point (the observed execution duration).
func (ts *ThompsonStats) Update(duration time.Duration) {
	x := float64(duration.Milliseconds())

	// Standard Bayesian update rules for a Normal-gamma distribution.
	newMu := (ts.Lambda*ts.Mu + x) / (ts.Lambda + 1)
	newLambda := ts.Lambda + 1
	newAlpha := ts.Alpha + 0.5
	newBeta := ts.Beta + (ts.Lambda*(x-ts.Mu)*(x-ts.Mu))/(2*(ts.Lambda+1))

	ts.Mu = newMu
	ts.Lambda = newLambda
	ts.Alpha = newAlpha
	ts.Beta = newBeta
	ts.Runs++
}

// NewThompsonStats creates a new stats object with a diffuse prior,
// representing our initial uncertainty about a strategy's performance.
func NewThompsonStats() *ThompsonStats {
	return &ThompsonStats{
		Mu: 7.0, // Guess for mean execution time (in ms) for each call to "Update".
		// From the context of Check, each subproblem of userset or TTU will get an individual call to "Update".
		Lambda: 1.0,  // Low confidence in the initial mean
		Alpha:  1.0,  // Diffuse prior for the shape
		Beta:   10.0, // Diffuse prior for the rate (encourages higher variance initially)
		Runs:   0,
	}
}
