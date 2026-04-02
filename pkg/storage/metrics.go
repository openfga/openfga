package storage

import (
	"errors"
	"strconv"
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

// SuccessLabel returns true if err represents a successful storage outcome.
// A nil error or expected storage outcomes (not found, collision, invalid write) are considered
// successful since the DB processed the query correctly. Only genuine infrastructure errors
// return false.
func SuccessLabel(err error) bool {
	return err == nil ||
		errors.Is(err, ErrNotFound) ||
		errors.Is(err, ErrCollision) ||
		errors.Is(err, ErrInvalidWriteInput)
}

// ObserveIterQueryDuration records a duration observation on the shared iterator query histogram.
// Callers determine the success value — typically via SuccessLabel(err) for error-based classification,
// but any boolean is accepted to support unforeseen use cases.
//
// Since iterQueryDurationHistogram buckets are defined in whole milliseconds, sub-millisecond
// accuracy is unnecessary. We use d.Milliseconds() (integer truncation) instead of float division
// (e.g. d.Seconds()*1000) as a deliberate performance tradeoff.
func ObserveIterQueryDuration(success bool, d time.Duration) {
	iterQueryDurationHistogram.WithLabelValues(strconv.FormatBool(success)).Observe(float64(d.Milliseconds()))
}
