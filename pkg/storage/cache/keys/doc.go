// Package keys provides deterministic, collision-resistant cache key
// construction for OpenFGA's storage layer. It encodes heterogeneous
// fields into a compact TLV (type-length-value) byte sequence that is
// then hex-encoded for use as a cache key string.
package keys
