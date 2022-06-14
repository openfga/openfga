package cache

// Cache defines an interface for a generic cache.
type Cache interface {

	// Get returns the value for the given key in the cache, if it exists.
	Get(key interface{}) (interface{}, bool)

	// Set sets a value for the key in the cache, with the given cost.
	Set(key interface{}, entry interface{}, cost int64) bool

	// Close closes the cache, cleaning up ay residual resources before returning.
	Close()
}
