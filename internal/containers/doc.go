// Package containers provides generic, concurrency-safe data structures.
//
// [Bag] is a lock-free, append-only collection backed by an atomic linked
// list. [AtomicMap] is a mutex-protected map that avoids the per-element
// allocation overhead of [sync.Map]. The [mpsc] subpackage provides a
// lock-free multiple-producer, single-consumer queue.
package containers
