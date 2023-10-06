package utils

import "strconv"

// Bucketize will put the value into the correct bucket.
// It is expected that the buckets are already sorted in increasing order and non-empty
func Bucketize(value uint, buckets []uint) string {
	for _, bucketValue := range buckets {
		if value <= bucketValue {
			return strconv.Itoa(int(bucketValue))
		}
	}
	return ">" + strconv.Itoa(int(buckets[len(buckets)-1]))
}
