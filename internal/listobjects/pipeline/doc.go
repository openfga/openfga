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
// authorization graph. Workers communicate through message passing,
// processing tuples and propagating results toward the query origin.
//
//	Pipeline.Expand()
//	       │
//	       ▼
//	  resolve() ─── builds worker graph from authorization model
//	       │
//	       ▼
//	  ┌─────────┐     ┌─────────┐     ┌─────────┐
//	  │ Worker  │◄────│ Worker  │◄────│ Worker  │
//	  │ (root)  │     │         │     │ (leaf)  │
//	  └─────────┘     └─────────┘     └─────────┘
//	       │
//	       ▼
//	  iter.Seq[Object] ─── results streamed to caller
//
// # Trade-offs
//
// Message-Passing Concurrency:
//   - Workers communicate via bounded buffers, avoiding shared memory
//   - Natural backpressure prevents memory exhaustion
//   - Graph structure determines parallelism
//
// Streaming Results:
//   - Memory usage remains constant regardless of result set size
//   - Enables early termination when caller stops iteration
//
// Cycle Handling:
//   - Cycles require input deduplication to prevent infinite loops
//   - Coordinated shutdown ensures all in-flight messages are processed
//
// # Usage
//
//	p := pipeline.New(graph, reader,
//	    pipeline.WithNumProcs(4),
//	    pipeline.WithChunkSize(100),
//	)
//
//	seq, err := p.Expand(ctx, pipeline.Spec{
//	    ObjectType: "document",
//	    Relation:   "viewer",
//	    User:       "user:alice",
//	})
//
//	for obj := range seq {
//	    id, err := obj.Object()
//	    if err != nil {
//	        // handle error
//	    }
//	    // id is an object ID like "document:readme"
//	}
package pipeline
