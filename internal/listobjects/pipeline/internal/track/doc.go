// Package track provides coordination primitives for detecting when a
// group of concurrent sources have all reached a ready state and the
// number of in-flight operations has dropped to zero (quiescence).
//
// A [StatusPool] tracks multiple sources registered via [StatusPool.Register].
// Each source receives a [Reporter] that can signal readiness and
// adjust the shared in-flight counter. [StatusPool.Wait] blocks until
// all sources have reported ready and the in-flight count reaches zero.
package track
