// Package pipeline implements reverse expansion for authorization queries.
//
// Reverse expansion answers the question: "What objects can a user access?"
// Given a target type/relation and a user, the pipeline traverses the
// authorization model graph to find all objects where the user has the
// specified relation.
//
// # Architecture
//
// The pipeline constructs a network of workers, one per node in the
// authorization graph. Workers communicate through concurrent message passing,
// processing tuples and propagating results upstream toward the query origin.
//
//	Query.Execute()
//	     │
//	     ▼
//	resolve() ─── builds worker graph from authorization model
//	     │
//	     ▼
//	┌─────────┐     ┌─────────┐     ┌─────────┐
//	│ Worker  │◄────│ Worker  │◄────│ Worker  │
//	│ (root)  │     │         │     │ (leaf)  │
//	└─────────┘     └─────────┘     └─────────┘
//	     │
//	     ▼
//	iter.Seq[Item] ─── results streamed to caller
//
// Each worker corresponds to a relation in the authorization model. Workers
// query tuples from storage, resolve relationships through connected workers,
// and propagate results back toward the query root. The root worker streams
// final results to the caller via Go 1.23's iter.Seq.
//
// # Trade-offs
//
// Message-Passing Concurrency:
//   - Workers communicate via bounded concurrent buffers, avoiding shared memory
//   - Natural backpressure prevents memory exhaustion when consumers fall behind
//   - Message passing overhead adds latency compared to direct function calls
//   - Graph structure determines parallelism; complex models exploit more concurrency
//
// Streaming Results:
//   - Results stream incrementally via iter.Seq rather than collecting in memory
//   - Memory usage remains constant regardless of result set size
//   - Enables early termination when caller stops iteration
//   - Cannot optimize based on full result set (e.g., batch database writes)
//
// Worker Network Overhead:
//   - Each worker spawns goroutines (NumProcs per worker) and allocates buffers
//   - Simple authorization models with few relations have low overhead
//   - Deep or wide graphs create many workers, increasing memory and scheduling cost
//   - Worker reuse across queries not currently implemented
//
// Cycle Handling:
//   - Authorization models may contain recursive relations (e.g., parent/ancestor)
//   - Cycles require input deduplication to prevent infinite loops
//   - Deduplication uses in-memory maps proportional to unique values seen
//   - Coordinated shutdown ensures all in-flight messages are processed
//   - More complex than acyclic graph traversal but handles general models
//
// Tuple Batching:
//   - Workers batch tuple results (ChunkSize) before sending to reduce message overhead
//   - Larger batches improve throughput but increase latency and memory per message
//   - Buffer pools reuse batch allocations to reduce GC pressure
//   - Batching trades off latency for efficiency
//
// # Configuration
//
// Use [Option] functions to configure pipeline behavior:
//
//	query := NewQuery(backend,
//	    WithNumProcs(4),      // goroutines per worker
//	    WithChunkSize(100),   // batch size for tuple processing
//	    WithBufferSize(128),  // concurrent buffer capacity
//	)
//
// # Usage
//
//	seq, err := NewQuery(backend).
//	    From("document", "viewer").
//	    To("user:alice").
//	    Execute(ctx)
//
//	for item := range seq {
//	    if item.Err != nil {
//	        // handle error
//	    }
//	    // item.Value is an object ID like "document:readme"
//	}
package pipeline
