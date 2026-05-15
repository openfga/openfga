// Package mpsc provides a lock-free MPSC (multiple-producer, single-consumer)
// queue built on atomic linked-list operations.
//
// Producers call [Accumulator.Send] concurrently to enqueue items.
// A single consumer iterates over them in insertion order via [Accumulator.Seq].
// [Accumulator.Close] must be called after all producers have completed
// to signal the end of the stream and wake the consumer.
package mpsc
