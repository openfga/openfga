package graph

import (
	"context"
	"fmt"

	dispatchpb "github.com/openfga/openfga/pkg/proto/dispatch/v1"
)

var emptyMetadata = &dispatchpb.ResponseMeta{}

type contextKey string

var checkResolutionTracerCtxKey contextKey = "check-resolution-tracer"

// WithCheckResolutionTraceContext returns a new context based on the parent one with the provided
// check resolution tracer included in it.
func WithCheckResolutionTraceContext(parent context.Context, t CheckResolutionTracer) context.Context {
	return context.WithValue(parent, checkResolutionTracerCtxKey, t)
}

// CheckResolutionTracerFromContext returns the check resolution tracer included in the provided context
// if it is present, otherwise it returns a noop implementation of the
func CheckResolutionTracerFromContext(ctx context.Context) CheckResolutionTracer {
	tracer, ok := ctx.Value(checkResolutionTracerCtxKey).(CheckResolutionTracer)
	if !ok {
		// todo: return a noop implementation of the CheckResolutionTracer interface
	}

	return tracer

}

type CheckResolutionTracer interface {
	AppendComputed() CheckResolutionTracer
	AppendDirect() CheckResolutionTracer
	AppendIndex(i int) CheckResolutionTracer
	AppendIntersection(t intersectionTracer) CheckResolutionTracer
	AppendString(s string) CheckResolutionTracer
	AppendTupleToUserset() CheckResolutionTracer
	AppendUnion() CheckResolutionTracer
	CreateIntersectionTracer() intersectionTracer
	GetResolution() string
}

type intersectionTracer interface {
	AppendTrace(rt CheckResolutionTracer)
	GetResolution() string
}

// noopResolutionTracer is thread safe as current implementation is immutable
type noopResolutionTracer struct{}

func NewNoopCheckResolutionTracer() CheckResolutionTracer {
	return &noopResolutionTracer{}
}

func (t *noopResolutionTracer) AppendComputed() CheckResolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendDirect() CheckResolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendIndex(_ int) CheckResolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendIntersection(_ intersectionTracer) CheckResolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendString(_ string) CheckResolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendTupleToUserset() CheckResolutionTracer {
	return t
}

func (t *noopResolutionTracer) AppendUnion() CheckResolutionTracer {
	return t
}

var nit = &noopIntersectionTracer{}

func (t *noopResolutionTracer) CreateIntersectionTracer() intersectionTracer {
	return nit
}

func (t *noopResolutionTracer) GetResolution() string {
	return ""
}

type noopIntersectionTracer struct{}

func (t *noopIntersectionTracer) AppendTrace(_ CheckResolutionTracer) {}

func (t *noopIntersectionTracer) GetResolution() string {
	return ""
}

// stringResolutionTracer is thread safe as current implementation is immutable
type stringResolutionTracer struct {
	trace string
}

func NewStringCheckResolutionTracer(s string) CheckResolutionTracer {

	t := &stringResolutionTracer{
		trace: ".",
	}

	if s != "" {
		t.trace = s
	}

	return t
}

func (t *stringResolutionTracer) AppendComputed() CheckResolutionTracer {
	return t.AppendString("(computed-userset)")
}

func (t *stringResolutionTracer) AppendDirect() CheckResolutionTracer {
	return t.AppendString("(direct)")
}

// AppendIndex We create separate append functions so no casting happens externally
// This aim to minimize overhead added by the no-op implementation
func (t *stringResolutionTracer) AppendIndex(n int) CheckResolutionTracer {
	return &stringResolutionTracer{
		trace: fmt.Sprintf("%s%d", t.trace, n),
	}
}

func (t *stringResolutionTracer) AppendIntersection(it intersectionTracer) CheckResolutionTracer {
	return &stringResolutionTracer{
		trace: fmt.Sprintf("%s[%s]", t.trace, it.GetResolution()),
	}
}

func (t *stringResolutionTracer) AppendString(subTrace string) CheckResolutionTracer {
	return &stringResolutionTracer{
		trace: fmt.Sprintf("%s%s.", t.trace, subTrace),
	}
}

func (t *stringResolutionTracer) AppendTupleToUserset() CheckResolutionTracer {
	return t.AppendString("(tuple-to-userset)")
}

func (t *stringResolutionTracer) AppendUnion() CheckResolutionTracer {
	return t.AppendString("union")
}

func (t *stringResolutionTracer) CreateIntersectionTracer() intersectionTracer {
	return &stringIntersectionTracer{}
}

func (t *stringResolutionTracer) GetResolution() string {
	return t.trace
}

type stringIntersectionTracer struct {
	trace string
}

func (t *stringIntersectionTracer) AppendTrace(rt CheckResolutionTracer) {
	if len(t.trace) != 0 {
		t.trace = fmt.Sprintf("%s,%s", t.trace, rt.GetResolution())
		return
	}

	t.trace = rt.GetResolution()
}

func (t *stringIntersectionTracer) GetResolution() string {
	return t.trace
}

// ReduceableCheckFunc is a function that can be bound to a concurrent check execution context and
// yields a check result on the provided channel.
type ReduceableCheckFunc func(ctx context.Context, resultChan chan<- *dispatchpb.DispatchCheckResponse) error

// CheckReducer is a type for the functions 'any', `all`, and `difference` which combine check results.
type CheckReducer func(ctx context.Context, requests []ReduceableCheckFunc) (*dispatchpb.DispatchCheckResponse, error)

func addCallToResponseMetadata(metadata *dispatchpb.ResponseMeta) *dispatchpb.ResponseMeta {
	// + 1 for the current call.
	return &dispatchpb.ResponseMeta{
		DispatchCount:       metadata.GetDispatchCount() + 1,
		DepthRequired:       metadata.GetDepthRequired() + 1,
		CachedDispatchCount: metadata.GetCachedDispatchCount(),
		ResolutionPath:      metadata.GetResolutionPath(),
	}
}

func decrementDepth(metadata *dispatchpb.ResolverMeta) *dispatchpb.ResolverMeta {
	return &dispatchpb.ResolverMeta{
		DepthRemaining: metadata.GetDepthRemaining() - 1,
	}
}

func combineResponseMetadata(existing *dispatchpb.ResponseMeta, responseMetadata *dispatchpb.ResponseMeta) *dispatchpb.ResponseMeta {

	// take the maximum depth of the two
	depthRequired := existing.GetDepthRequired()
	if responseMetadata.GetDepthRequired() > existing.GetDepthRequired() {
		depthRequired = responseMetadata.GetDepthRequired()
	}

	return &dispatchpb.ResponseMeta{
		DispatchCount:       existing.GetDispatchCount() + responseMetadata.GetDispatchCount(),
		DepthRequired:       depthRequired,
		CachedDispatchCount: existing.GetCachedDispatchCount() + responseMetadata.GetCachedDispatchCount(),
		ResolutionPath:      responseMetadata.GetResolutionPath(),
	}
}

func ensureMetadata(responseMetadata *dispatchpb.ResponseMeta) *dispatchpb.ResponseMeta {
	if responseMetadata == nil {
		responseMetadata = emptyMetadata
	}

	return &dispatchpb.ResponseMeta{
		DispatchCount:       responseMetadata.DispatchCount,
		DepthRequired:       responseMetadata.DepthRequired,
		CachedDispatchCount: responseMetadata.CachedDispatchCount,
		ResolutionPath:      responseMetadata.ResolutionPath,
	}
}
