package utils

import (
	"sort"
	"strconv"
)

// Bucketize will put the value of a metric into the correct bucket, and return the label for it.
// It is expected that the buckets are already sorted in increasing order and non-empty.
func Bucketize(value uint, buckets []uint) string {
	idx := sort.Search(len(buckets), func(i int) bool {
		return value <= buckets[i]
	})

	if idx == len(buckets) {
		return "+Inf"
	}

	return strconv.Itoa(int(buckets[idx]))
}

// LinearBuckets returns an evenly distributed range of buckets in the closed interval
// [min...max]. The min and max count toward the bucket count since they are included
// in the range. It returns nil if count is not positive.
func LinearBuckets(minValue, maxValue float64, count int) []float64 {
	if count <= 0 {
		return nil
	}

	if count == 1 {
		return []float64{minValue}
	}

	width := (maxValue - minValue) / float64(count-1)

	buckets := make([]float64, count)
	for i := range count {
		buckets[i] = minValue + width*float64(i)
	}

	// Assign the endpoints directly so that rounding in the multiplication above cannot
	// push them off the requested interval.
	buckets[0] = minValue
	buckets[count-1] = maxValue

	return buckets
}
