package v2breaking

import (
	"slices"
	"strconv"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// BatchReasoner accumulates potential v1→v2 (weighted-graph) resolution
// divergences across a batch of Check tuple keys evaluated against a single
// type system. It encapsulates the per-check shape detection, the memoization
// that keeps repeated shapes cheap, and the aggregation the caller needs to
// emit a single summary log.
//
// Detection is purely schema-shape (see CheckExclusionReason / CheckReason) and
// does NOT gate on the check's allowed result: the outcome reflects whichever
// engine answered (v2 normally, v1 on fallback), so gating on it would silently
// drop real divergences on the fallback path — the path this telemetry most
// needs to flag. Being shape-only, the predicates may over-report.
//
// The predicates walk the model's rewrite tree, which is the dominant cost. For
// a fixed type system both reasons are functions of the request's shape (object
// type, relation, user type[#relation]), not its identifiers, so shapes are
// memoized and each is walked at most once. Two caches are kept:
//
//   - exclusionCache: pure-shape key. CheckExclusionReason never compares object
//     identity, so e.g. document:1@group:eng#member and document:2@group:sales#member
//     share one entry.
//   - checkReasonCache: shape key plus a sameObject bool, since CheckReason's
//     self_referential / computed_userset_self_object shapes depend on the
//     user's object equalling the target object — collapsing the concrete IDs to
//     one bit that still keys them together.
//
// A BatchReasoner is not safe for concurrent use.
type BatchReasoner struct {
	typesys *typesystem.TypeSystem

	// Both caches memoize the (possibly empty) reason for a shape key so repeated
	// shapes in the batch skip the rewrite-tree walk. Empty reasons are cached
	// too, so repeated no-match checks are equally cheap.
	exclusionCache   map[string]string
	checkReasonCache map[string]string

	reasonSet              map[string]struct{}
	reasonsByCorrelationID map[string]string
}

// NewBatchReasoner returns a BatchReasoner that detects divergences against the
// given type system.
func NewBatchReasoner(typesys *typesystem.TypeSystem) *BatchReasoner {
	return &BatchReasoner{
		typesys:                typesys,
		exclusionCache:         map[string]string{},
		checkReasonCache:       map[string]string{},
		reasonSet:              map[string]struct{}{},
		reasonsByCorrelationID: map[string]string{},
	}
}

// Add detects the divergence shape for a single check and, when one matches,
// records it against correlationID. Checks that don't match any shape are
// ignored.
func (b *BatchReasoner) Add(correlationID string, tk *openfgav1.CheckRequestTupleKey) {
	reason := b.reason(tk)
	if reason == "" {
		return
	}
	b.reasonSet[reason] = struct{}{}
	b.reasonsByCorrelationID[correlationID] = reason
}

// reason returns the memoized divergence reason for a single check's shape, or
// "" if none matches.
func (b *BatchReasoner) reason(tk *openfgav1.CheckRequestTupleKey) string {
	userObject, userRelation := tuple.SplitObjectRelation(tk.GetUser())
	// Pure-shape key: object type, relation, and user type[#relation]. No
	// identifiers and no allowed bit, so requests differing only in their
	// concrete IDs share an entry.
	shapeKey := tuple.GetType(tk.GetObject()) + "#" + tk.GetRelation() + "@" + tuple.GetType(userObject) + "#" + userRelation

	reason, cached := b.exclusionCache[shapeKey]
	if !cached {
		reason = CheckExclusionReason(b.typesys, tk)
		b.exclusionCache[shapeKey] = reason
	}
	if reason != "" {
		return reason
	}

	if !tuple.IsObjectRelation(tk.GetUser()) {
		return ""
	}
	// CheckReason's self_referential / computed_userset_self_object shapes
	// depend on the user's object equalling the target object, so add that as a
	// bool rather than keying on the identifiers themselves.
	sameObject := userObject == tk.GetObject()
	checkKey := shapeKey + "|" + strconv.FormatBool(sameObject)
	r, ok := b.checkReasonCache[checkKey]
	if !ok {
		r = CheckReason(b.typesys, tk)
		b.checkReasonCache[checkKey] = r
	}
	return r
}

// Empty reports whether no check added so far matched a divergence shape.
func (b *BatchReasoner) Empty() bool {
	return len(b.reasonSet) == 0
}

// Reasons returns the distinct divergence reasons found across the batch, sorted
// for stable log output.
func (b *BatchReasoner) Reasons() []string {
	reasons := make([]string, 0, len(b.reasonSet))
	for reason := range b.reasonSet {
		reasons = append(reasons, reason)
	}
	slices.Sort(reasons)
	return reasons
}

// ReasonsByCorrelationID returns the per-check attribution of each flagged check
// to its divergence reason.
func (b *BatchReasoner) ReasonsByCorrelationID() map[string]string {
	return b.reasonsByCorrelationID
}

// MatchedChecks returns the number of checks that matched a divergence shape.
func (b *BatchReasoner) MatchedChecks() int {
	return len(b.reasonsByCorrelationID)
}
