// Package mpsc provides a lock-free MPSC (multiple-producer, single-consumer)
// queue built on atomic linked-list operations.
//
// Producers call [Accumulator.Add] concurrently to enqueue items.
// A single consumer iterates over them in insertion order via [Accumulator.Seq].
// [Accumulator.Close] inserts a sentinel node that causes Seq to return;
// the Accumulator is then reusable for another Add/Close/Seq cycle.
//
// Delivery is exactly-once: when the consumer breaks out of Seq early,
// the last yielded item is consumed and a subsequent Seq resumes from
// the next unconsumed item.
package mpsc
