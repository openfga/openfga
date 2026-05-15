// Package mpmc provides a bounded, multiple-producer, multiple-consumer
// queue with optional automatic buffer growth.
//
// [Queue] is safe for concurrent use by any number of senders and
// receivers. It uses a mutex-protected ring buffer with channel-based
// signaling to block senders when full and receivers when empty.
package mpmc
