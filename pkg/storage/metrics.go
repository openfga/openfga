package storage

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/openfga/openfga/internal/build"
)

var iterQueryDurationHistogram = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace:                       build.ProjectName,
	Name:                            "iter_query_duration_ms",
	Help:                            "The duration (in ms) of a TupleIterator fetchBuffer query labeled by success.",
	Buckets:                         []float64{1, 5, 10, 25, 50, 100, 200, 300, 1000},
	NativeHistogramBucketFactor:     1.1,
	NativeHistogramMaxBucketNumber:  100,
	NativeHistogramMinResetDuration: time.Hour,
}, []string{"success"})

// SuccessLabel returns the prometheus success label for a storage error.
// A nil error or expected storage outcomes (not found, collision, invalid write) are labelled
// "true" since the DB processed the query correctly. Only genuine infrastructure errors are
// labelled "false".
func SuccessLabel(err error) string {
	if err == nil ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrCollision) ||
		errors.Is(err, ErrInvalidWriteInput) {
		return "true"
	}
	return "false"
}

// ObserveIterQueryDuration records a duration observation on the shared iterator query histogram.
// Err must be a storage-layer error (nil or a sentinel returned by the backend's error handler,
// e.g. ErrNotFound, ErrCollision); the success label is derived via SuccessLabel to guarantee
// only the fixed values "true"/"false" are ever emitted.
//
// Since iterQueryDurationHistogram buckets are defined in whole milliseconds, sub-millisecond
// accuracy is unnecessary. We use d.Milliseconds() (integer truncation) instead of float division
// (e.g. d.Seconds()*1000) as a deliberate performance tradeoff.
func ObserveIterQueryDuration(err error, d time.Duration) {
	iterQueryDurationHistogram.WithLabelValues(SuccessLabel(err)).Observe(float64(d.Milliseconds()))
}
