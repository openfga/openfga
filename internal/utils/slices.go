package utils

// Uniq returns a slice of the input collection with all duplicates removed.
func Uniq[T comparable](collection []T) []T {
	result := make([]T, 0, len(collection))
	seen := make(map[T]struct{}, len(collection))

	for _, item := range collection {
		if _, ok := seen[item]; ok {
			continue
		}

		seen[item] = struct{}{}
		result = append(result, item)
	}

	return result
}

// Returns a slice of elements split into chunks the length of chunkSize. If the provided collection can't be split evenly,
// then the final chunk will be the residual elements.
func Chunk[T any](collection []T, chunkSize int) [][]T {
	if chunkSize <= 0 {
		panic("chunkSize parameter must be greater than 0")
	}

	numChunks := len(collection) / chunkSize
	if len(collection)%chunkSize != 0 {
		numChunks++
	}

	result := make([][]T, 0, numChunks)

	for i := 0; i < numChunks; i++ {
		last := (i + 1) * chunkSize
		if last > len(collection) {
			last = len(collection)
		}

		result = append(result, collection[i*chunkSize:last])
	}

	return result
}
