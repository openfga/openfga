// Package worker implements the concurrent processing nodes that form
// a reverse expansion pipeline.
//
// Each worker receives batches of object identifiers from upstream
// senders, transforms them through an [Interpreter] (typically a
// storage query), and broadcasts the results to downstream listeners.
// Workers communicate through [Medium] instances — bounded,
// unidirectional pipes that provide natural backpressure.
//
// Three worker types implement different set operations:
//
//   - [Basic] — union/passthrough with cross-sender deduplication.
//   - [Intersection] — outputs only items common to all senders.
//   - [Difference] — outputs base items not present in the subtract set.
//
// Cycles in the authorization model graph are handled by [CycleGroup],
// which coordinates quiescence detection and ordered teardown across the
// workers that form the cycle.
package worker
