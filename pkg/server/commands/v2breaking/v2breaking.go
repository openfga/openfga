// Package v2breaking detects request shapes whose v1 vs v2 (weighted-graph)
// resolution behavior is known to differ. It exists so the server can emit
// telemetry about potential breaking-change exposure before the weighted-graph
// resolver becomes the default — see the openfga.dev breaking-change writeup.
//
// All predicates here are schema-shape filters only: they read the model, not
// stored tuples. They may over-report (the request shape matches but no tuple
// triggers the divergence) but never miss a real divergence.
package v2breaking

import (
	"slices"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
)

// Reason constants are stable strings emitted in logs/metrics. Do not rename
// without coordinating with downstream dashboards.
const (
	ReasonSelfReferentialUserset = "self_referential_userset"
	ReasonAliasUserset           = "alias_userset"
	ReasonComputedUsersetSelfObj = "computed_userset_self_object"
	ReasonTTUUserset             = "ttu_userset"
	ReasonUsersetWithExclusion   = "userset_with_exclusion"
	ReasonWildcardWithExclusion  = "wildcard_with_exclusion"
)

// CheckReason returns a non-empty reason string when the Check request shape
// matches a known v1→v2 divergence for userset users. The caller is expected
// to have already verified that the user is a userset (object#relation) and
// that v2Check returned FALSE — this function does not re-check those.
//
// Shape catalogue:
//
//   - "self_referential_userset": v1 unconditionally returned TRUE for
//     check(o#r, r, o); v2 evaluates against the schema and returns FALSE.
//
//   - "alias_userset": the target relation directly accepts T#R' where R'
//     resolves via computed_userset to the user's relation R, and R is not
//     itself directly assignable on the target. v1 follows the alias from a
//     stored tuple, v2 does not.
//
//   - "computed_userset_self_object": user's object equals the target object,
//     and the user's relation appears as a ComputedUserset leaf in the target
//     relation's rewrite tree.
//
//   - "ttu_userset": target relation's rewrite contains a TupleToUserset whose
//     computed relation equals the user's relation, AND the user's object type
//     is directly-related to the tupleset relation.
func CheckReason(typesys *typesystem.TypeSystem, tk *openfgav1.CheckRequestTupleKey) string {
	if tk.GetUser() == tk.GetObject()+"#"+tk.GetRelation() {
		return ReasonSelfReferentialUserset
	}
	userObject, userRelation := tuple.SplitObjectRelation(tk.GetUser())
	userObjectType := tuple.GetType(userObject)
	targetObjectType := tuple.GetType(tk.GetObject())
	targetRelation := tk.GetRelation()

	if usersetAliasesTargetRelation(typesys, targetObjectType, targetRelation, userObjectType, userRelation) {
		return ReasonAliasUserset
	}
	rel, err := typesys.GetRelation(targetObjectType, targetRelation)
	if err != nil {
		return ""
	}
	rewrite := rel.GetRewrite()
	if userObject == tk.GetObject() && rewriteContainsComputedUserset(rewrite, userRelation) {
		return ReasonComputedUsersetSelfObj
	}
	if rewriteContainsTTUForUser(typesys, targetObjectType, rewrite, userObjectType, userRelation) {
		return ReasonTTUUserset
	}
	return ""
}

// ExpandReason returns a non-empty reason string when the Expand request shape
// matches a known v1→v2 resolution divergence. Compared to Check/ListUsers,
// Expand has no user input — only (object, relation) — so detection is purely
// schema-shape against the target relation's rewrite.
//
// ExpandReason covers the shapes whose v1 Expand tree exposes a divergence
// from v2:
//
//   - The two exclusion shapes (userset_with_exclusion, wildcard_with_exclusion):
//     v2 rejects the request at request time, so any v1 Expand response on a
//     Difference-containing rewrite is itself the divergence signal.
//
//   - alias_userset: v1 follows `T#R' → R` aliases when materializing leaves,
//     surfacing aliased usersets that v2's strict storage-validation path
//     would not.
//
//   - computed_userset_self_object: the rewrite contains a ComputedUserset
//     leaf referring to a sibling relation. v1 Expand emits this leaf as a
//     direct member of the target relation (e.g. `viewer: editor or writer`
//     produces a Union with `document:d1#writer` as a leaf), which is
//     exactly the v1 inference v2's strict resolution would not surface.
//
//   - ttu_userset: the rewrite contains a TupleToUserset. v1 Expand emits a
//     TTU leaf naming the tupleset relation (e.g. `viewer: viewer from
//     parent` produces a leaf referencing `document:d1#parent`) even when no
//     parent tuples exist in storage — surfacing structural detail that v2's
//     strict resolution would not.
//
// `self_referential_userset` remains a Check-only boolean shortcut — it
// doesn't change Expand's emitted tree.
//
// Exclusion shapes are checked before the others since they are the more
// severe divergence (v2 returns a request-time error).
func ExpandReason(typesys *typesystem.TypeSystem, targetObjectType, targetRelation string) string {
	rel, err := typesys.GetRelation(targetObjectType, targetRelation)
	if err != nil {
		return ""
	}
	rewrite := rel.GetRewrite()

	if rewriteContainsDifference(rewrite) {
		if anyTypeWildcardReachableUnderDifferenceBase(typesys, targetObjectType, targetRelation, rewrite) {
			return ReasonWildcardWithExclusion
		}
		return ReasonUsersetWithExclusion
	}

	if isAliasedDirectlyRelatedUserset(typesys, targetObjectType, targetRelation, "", "") {
		return ReasonAliasUserset
	}

	if reason := computedUsersetOrTTUReason(rewrite); reason != "" {
		return reason
	}

	return ""
}

// computedUsersetOrTTUReason walks the rewrite tree once and returns
// ReasonComputedUsersetSelfObj if any ComputedUserset leaf is present, or
// ReasonTTUUserset if any TupleToUserset node is present, or "".
func computedUsersetOrTTUReason(rewrite *openfgav1.Userset) string {
	result, _ := typesystem.WalkUsersetRewrite(rewrite, func(r *openfgav1.Userset) interface{} {
		switch r.GetUserset().(type) {
		case *openfgav1.Userset_ComputedUserset:
			return ReasonComputedUsersetSelfObj
		case *openfgav1.Userset_TupleToUserset:
			return ReasonTTUUserset
		}
		return nil
	})
	if reason, ok := result.(string); ok {
		return reason
	}

	return ""
}

// anyTypeWildcardReachableUnderDifferenceBase walks every type defined in the
// model and reports whether any of them has a typed wildcard reachable under
// the rewrite's Difference base. Used by ExpandReason where there is no filter
// to anchor the wildcard search.
func anyTypeWildcardReachableUnderDifferenceBase(ts *typesystem.TypeSystem, targetObjectType, relation string, rewrite *openfgav1.Userset) bool {
	for userObjectType := range ts.GetAllRelations() {
		if wildcardReachableUnderDifferenceBase(ts, targetObjectType, relation, rewrite, userObjectType) {
			return true
		}
	}
	return false
}

// isAliasedDirectlyRelatedUserset reports whether (userObjectType, userRelation)
// is a directly-related userset on (targetObjectType, targetRelation) whose
// rewrite is a ComputedUserset alias (i.e. `define R' = R` on T). When both
// userObjectType and userRelation are empty the function returns true if ANY
// directly-related userset on the target is so aliased — the schema-shape
// check used by ExpandReason.
func isAliasedDirectlyRelatedUserset(ts *typesystem.TypeSystem, targetObjectType, targetRelation, userObjectType, userRelation string) bool {
	usersets, err := ts.DirectlyRelatedUsersets(targetObjectType, targetRelation)
	if err != nil {
		return false
	}
	matchAny := userObjectType == "" && userRelation == ""
	for _, ref := range usersets {
		if !matchAny && (ref.GetType() != userObjectType || ref.GetRelation() != userRelation) {
			continue
		}
		rel, err := ts.GetRelation(ref.GetType(), ref.GetRelation())
		if err != nil {
			continue
		}
		if _, ok := rel.GetRewrite().GetUserset().(*openfgav1.Userset_ComputedUserset); ok {
			return true
		}
	}
	return false
}

// ListUsersReason returns a non-empty reason string when the ListUsers request
// shape matches a known v1→v2 divergence. Compared to CheckReason, it also
// detects the two exclusion-shape cases that v2Check rejects at request time
// (rather than silently returning FALSE):
//
//   - "userset_with_exclusion": the filter is a userset and the target
//     relation's rewrite contains a Difference node.
//
//   - "wildcard_with_exclusion": the filter is a non-userset of type T and the
//     target relation's rewrite contains a Difference whose base branch can
//     accept a typed wildcard T:*.
func ListUsersReason(typesys *typesystem.TypeSystem, object *openfgav1.Object, relation string, filter *openfgav1.UserTypeFilter) string {
	if object == nil || filter == nil {
		return ""
	}
	targetObjectType := object.GetType()
	filterType := filter.GetType()
	filterRelation := filter.GetRelation()

	if filterRelation != "" {
		if targetObjectType == filterType && relation == filterRelation {
			return ReasonSelfReferentialUserset
		}
		if usersetAliasesTargetRelation(typesys, targetObjectType, relation, filterType, filterRelation) {
			return ReasonAliasUserset
		}
	}

	rel, err := typesys.GetRelation(targetObjectType, relation)
	if err != nil {
		return ""
	}
	rewrite := rel.GetRewrite()

	if filterRelation != "" {
		if targetObjectType == filterType && rewriteContainsComputedUserset(rewrite, filterRelation) {
			return ReasonComputedUsersetSelfObj
		}
		if rewriteContainsTTUForUser(typesys, targetObjectType, rewrite, filterType, filterRelation) {
			return ReasonTTUUserset
		}
		if rewriteContainsDifference(rewrite) {
			return ReasonUsersetWithExclusion
		}
		return ""
	}

	// Non-userset filter: only the wildcard-with-exclusion shape is in play.
	if wildcardReachableUnderDifferenceBase(typesys, targetObjectType, relation, rewrite, filterType) {
		return ReasonWildcardWithExclusion
	}
	return ""
}

// ListUsersResponseConfirmsReason reports whether the ListUsers response is
// consistent with v1 having actually traversed the divergent path for the
// given reason. It is used to suppress false-positive logs on shape-matched
// requests whose responses didn't observably exercise the v1 behavior.
//
// For the four per-user-shape reasons, this looks for the specific user that
// v1 surfaces via the divergent path. For the exclusion-shape reasons, v2
// would reject the request at request time — so any non-empty v1 response on
// a Difference-containing relation means v1 actually walked the exclusion
// path to produce it.
//
// Returns true for unknown reasons, so the caller's logic stays simple.
func ListUsersResponseConfirmsReason(reason string, typesys *typesystem.TypeSystem, object *openfgav1.Object, relation string, filter *openfgav1.UserTypeFilter, users []*openfgav1.User) bool {
	switch reason {
	case ReasonSelfReferentialUserset:
		// v1 synthesizes <object>#<relation> as a user of itself.
		return responseContainsUserset(users, object.GetType(), object.GetId(), relation)
	case ReasonComputedUsersetSelfObj:
		// v1 synthesizes <object>#<filter.relation> when filter.relation
		// appears as a ComputedUserset leaf on the target.
		return responseContainsUserset(users, object.GetType(), object.GetId(), filter.GetRelation())
	case ReasonAliasUserset:
		// v1 surfaces a userset of the filter shape because it walked
		// (T#R' alias) → filter.relation. ListUsers normalizes results to
		// the filter's (type, relation), so any matching userset confirms
		// the alias path was traversed.
		return responseContainsUsersetOfType(users, filter.GetType(), filter.GetRelation())
	case ReasonTTUUserset:
		// v1 surfaces a userset of (filter.type, filter.relation) via the TTU
		// edge. We can only confirm that *some* userset of that shape came
		// back, not that it came specifically via the TTU.
		return responseContainsUsersetOfType(users, filter.GetType(), filter.GetRelation())
	case ReasonUsersetWithExclusion, ReasonWildcardWithExclusion:
		// v2 would reject the request at request time. Any non-empty v1
		// response means v1 actually walked the Difference-containing rewrite
		// to produce a result that v2 would never have returned.
		return len(users) > 0
	}
	return true
}

func responseContainsUserset(users []*openfgav1.User, typ, id, relation string) bool {
	for _, u := range users {
		us := u.GetUserset()
		if us == nil {
			continue
		}
		if us.GetType() == typ && us.GetId() == id && us.GetRelation() == relation {
			return true
		}
	}
	return false
}

func responseContainsUsersetOfType(users []*openfgav1.User, typ, relation string) bool {
	for _, u := range users {
		us := u.GetUserset()
		if us == nil {
			continue
		}
		if us.GetType() == typ && us.GetRelation() == relation {
			return true
		}
	}
	return false
}

// ExpandResponseConfirmsReason reports whether the Expand response tree is
// consistent with v1 having actually traversed the divergent path for the
// given reason. It mirrors ListUsersResponseConfirmsReason: shape predicates
// fire on schema alone (which may over-report), and this function suppresses
// the log when the response shows the v1 path wasn't actually exercised.
//
// For the exclusion shapes, v2 would reject the request at request time, so
// any successful response means v1 actually walked the divergent path.
// For alias_userset, we confirm that v1 surfaced an aliased directly-related
// userset (T#R' where R' resolves via ComputedUserset to the queried
// relation) — that exact userset will not appear in a v2 expansion of the
// same call.
//
// Returns true for unknown reasons, so the caller's logic stays simple.
func ExpandResponseConfirmsReason(reason string, typesys *typesystem.TypeSystem, targetObjectType, targetRelation string, tree *openfgav1.UsersetTree) bool {
	switch reason {
	case ReasonUsersetWithExclusion, ReasonWildcardWithExclusion, ReasonComputedUsersetSelfObj, ReasonTTUUserset:
		// v2 has not yet been ported to Expand, so any response on a shape
		// known to diverge means the client is observing v1 behavior.
		return tree.GetRoot() != nil
	case ReasonAliasUserset:
		return treeHasAliasedUserset(typesys, targetObjectType, targetRelation, tree.GetRoot())
	}
	return true
}

func treeHasAliasedUserset(typesys *typesystem.TypeSystem, targetObjectType, targetRelation string, node *openfgav1.UsersetTree_Node) bool {
	if node == nil {
		return false
	}
	if leaf := node.GetLeaf(); leaf != nil {
		if users := leaf.GetUsers(); users != nil {
			for _, u := range users.GetUsers() {
				userObject, userRelation := tuple.SplitObjectRelation(u)
				if userRelation == "" {
					continue
				}
				userObjectType := tuple.GetType(userObject)
				if isAliasedDirectlyRelatedUserset(typesys, targetObjectType, targetRelation, userObjectType, userRelation) {
					return true
				}
			}
		}
		return false
	}
	if union := node.GetUnion(); union != nil {
		for _, child := range union.GetNodes() {
			if treeHasAliasedUserset(typesys, targetObjectType, targetRelation, child) {
				return true
			}
		}
	}
	if inter := node.GetIntersection(); inter != nil {
		for _, child := range inter.GetNodes() {
			if treeHasAliasedUserset(typesys, targetObjectType, targetRelation, child) {
				return true
			}
		}
	}
	if diff := node.GetDifference(); diff != nil {
		if treeHasAliasedUserset(typesys, targetObjectType, targetRelation, diff.GetBase()) {
			return true
		}
		if treeHasAliasedUserset(typesys, targetObjectType, targetRelation, diff.GetSubtract()) {
			return true
		}
	}
	return false
}

// rewriteContainsComputedUserset reports whether any ComputedUserset leaf in
// the rewrite tree references the given relation name.
func rewriteContainsComputedUserset(rewrite *openfgav1.Userset, relation string) bool {
	result, _ := typesystem.WalkUsersetRewrite(rewrite, func(r *openfgav1.Userset) interface{} {
		if cu, ok := r.GetUserset().(*openfgav1.Userset_ComputedUserset); ok && cu.ComputedUserset.GetRelation() == relation {
			return true
		}
		return nil
	})
	return result != nil
}

// rewriteContainsTTUForUser reports whether the target's rewrite contains a
// TupleToUserset whose computed relation equals userRelation, where the
// tupleset relation on the target object type is directly related to
// userObjectType.
func rewriteContainsTTUForUser(ts *typesystem.TypeSystem, targetObjectType string, rewrite *openfgav1.Userset, userObjectType, userRelation string) bool {
	result, _ := typesystem.WalkUsersetRewrite(rewrite, func(r *openfgav1.Userset) interface{} {
		ttu, ok := r.GetUserset().(*openfgav1.Userset_TupleToUserset)
		if !ok || ttu.TupleToUserset.GetComputedUserset().GetRelation() != userRelation {
			return nil
		}
		tuplesetRel := ttu.TupleToUserset.GetTupleset().GetRelation()
		// Loose type-only match (ignores relation/wildcard) is intentional:
		// this is an over-reporting necessary-condition filter. Do not swap
		// in IsDirectlyRelated.
		directlyRelated, err := ts.GetDirectlyRelatedUserTypes(targetObjectType, tuplesetRel)
		if err == nil && slices.ContainsFunc(directlyRelated, func(dr *openfgav1.RelationReference) bool {
			return dr.GetType() == userObjectType
		}) {
			return true
		}
		return nil
	})
	return result != nil
}

// usersetAliasesTargetRelation reports whether the target's directly-related
// usersets include some T#R' (where T = userObjectType) that is a
// computed_userset alias for userRelation. Excludes the trivial case where
// T#R is itself directly assignable (since v1 and v2 agree on direct matches).
func usersetAliasesTargetRelation(ts *typesystem.TypeSystem, targetObjectType, targetRelation, userObjectType, userRelation string) bool {
	usersets, err := ts.DirectlyRelatedUsersets(targetObjectType, targetRelation)
	if err != nil {
		return false
	}
	foundAlias := false
	for _, ref := range usersets {
		if ref.GetType() != userObjectType {
			continue
		}
		if ref.GetRelation() == userRelation {
			return false
		}
		if resolved, err := ts.ResolveComputedRelation(ref.GetType(), ref.GetRelation()); err == nil && resolved == userRelation {
			foundAlias = true
		}
	}
	return foundAlias
}

// rewriteContainsDifference reports whether the rewrite tree contains any
// Userset_Difference node.
func rewriteContainsDifference(rewrite *openfgav1.Userset) bool {
	result, _ := typesystem.WalkUsersetRewrite(rewrite, func(r *openfgav1.Userset) interface{} {
		if _, ok := r.GetUserset().(*openfgav1.Userset_Difference); ok {
			return true
		}
		return nil
	})
	return result != nil
}

// wildcardReachableUnderDifferenceBase walks rewrite (which belongs to
// `targetObjectType#relation`) and reports whether any Difference node it
// contains has a base branch that can accept a typed wildcard of
// userObjectType. "Can accept" means: somewhere in the base branch there is a
// relation reachable through This / ComputedUserset / TTU whose directly-
// related user types include userObjectType:*. This is a necessary condition
// only — it may over-report.
func wildcardReachableUnderDifferenceBase(ts *typesystem.TypeSystem, targetObjectType, relation string, rewrite *openfgav1.Userset, userObjectType string) bool {
	return walkForWildcardUnderDifference(ts, targetObjectType, relation, rewrite, userObjectType, map[string]struct{}{})
}

// walkForWildcardUnderDifference searches for a Difference node whose base
// branch can accept userObjectType:*. Visited prevents infinite recursion
// through computed/TTU cycles (e.g. recursive `viewer from parent` where
// parent points back to the same type) and mirrors the guard in
// branchAcceptsWildcard: it is only updated when crossing a relation edge
// (ComputedUserset / TTU), not on structural nodes — otherwise
// Union/Intersection/Difference children that share the parent's
// (objectType, relation) would short-circuit before being evaluated.
func walkForWildcardUnderDifference(ts *typesystem.TypeSystem, objectType, relation string, rewrite *openfgav1.Userset, userObjectType string, visited map[string]struct{}) bool {
	if rewrite == nil {
		return false
	}
	switch v := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_Difference:
		if branchAcceptsWildcard(ts, objectType, relation, v.Difference.GetBase(), userObjectType, map[string]struct{}{}) {
			return true
		}
		// A Difference may itself be nested inside another Difference's base;
		// keep walking for outer occurrences.
		return walkForWildcardUnderDifference(ts, objectType, relation, v.Difference.GetBase(), userObjectType, visited) ||
			walkForWildcardUnderDifference(ts, objectType, relation, v.Difference.GetSubtract(), userObjectType, visited)
	case *openfgav1.Userset_Union:
		for _, c := range v.Union.GetChild() {
			if walkForWildcardUnderDifference(ts, objectType, relation, c, userObjectType, visited) {
				return true
			}
		}
	case *openfgav1.Userset_Intersection:
		for _, c := range v.Intersection.GetChild() {
			if walkForWildcardUnderDifference(ts, objectType, relation, c, userObjectType, visited) {
				return true
			}
		}
	case *openfgav1.Userset_ComputedUserset:
		nextRel := v.ComputedUserset.GetRelation()
		key := objectType + "#" + nextRel
		if _, ok := visited[key]; ok {
			return false
		}
		r, err := ts.GetRelation(objectType, nextRel)
		if err != nil {
			return false
		}
		visited[key] = struct{}{}
		return walkForWildcardUnderDifference(ts, objectType, nextRel, r.GetRewrite(), userObjectType, visited)
	case *openfgav1.Userset_TupleToUserset:
		tuplesetRel := v.TupleToUserset.GetTupleset().GetRelation()
		computedRel := v.TupleToUserset.GetComputedUserset().GetRelation()
		directlyRelated, err := ts.GetDirectlyRelatedUserTypes(objectType, tuplesetRel)
		if err != nil {
			return false
		}
		for _, dr := range directlyRelated {
			key := dr.GetType() + "#" + computedRel
			if _, ok := visited[key]; ok {
				continue
			}
			r, err := ts.GetRelation(dr.GetType(), computedRel)
			if err != nil {
				continue
			}
			visited[key] = struct{}{}
			if walkForWildcardUnderDifference(ts, dr.GetType(), computedRel, r.GetRewrite(), userObjectType, visited) {
				return true
			}
		}
	}
	return false
}

// branchAcceptsWildcard walks a Difference's base branch and reports whether
// userObjectType:* is reachable as a directly-related type of some leaf
// relation. Visited prevents infinite recursion through computed/TTU cycles
// and is only updated when crossing a relation edge (ComputedUserset / TTU),
// not on structural nodes — otherwise Union/Intersection/Difference children
// that share the parent's (objectType, relation) would short-circuit before
// being evaluated.
func branchAcceptsWildcard(ts *typesystem.TypeSystem, objectType, relation string, rewrite *openfgav1.Userset, userObjectType string, visited map[string]struct{}) bool {
	if rewrite == nil {
		return false
	}

	switch v := rewrite.GetUserset().(type) {
	case *openfgav1.Userset_This:
		return relationAcceptsWildcardForType(ts, objectType, relation, userObjectType)
	case *openfgav1.Userset_ComputedUserset:
		nextRel := v.ComputedUserset.GetRelation()
		key := objectType + "#" + nextRel
		if _, ok := visited[key]; ok {
			return false
		}
		r, err := ts.GetRelation(objectType, nextRel)
		if err != nil {
			return false
		}
		visited[key] = struct{}{}
		return branchAcceptsWildcard(ts, objectType, nextRel, r.GetRewrite(), userObjectType, visited)
	case *openfgav1.Userset_TupleToUserset:
		tuplesetRel := v.TupleToUserset.GetTupleset().GetRelation()
		computedRel := v.TupleToUserset.GetComputedUserset().GetRelation()
		directlyRelated, err := ts.GetDirectlyRelatedUserTypes(objectType, tuplesetRel)
		if err != nil {
			return false
		}
		for _, dr := range directlyRelated {
			key := dr.GetType() + "#" + computedRel
			if _, ok := visited[key]; ok {
				continue
			}
			r, err := ts.GetRelation(dr.GetType(), computedRel)
			if err != nil {
				continue
			}
			visited[key] = struct{}{}
			if branchAcceptsWildcard(ts, dr.GetType(), computedRel, r.GetRewrite(), userObjectType, visited) {
				return true
			}
		}
		return false
	case *openfgav1.Userset_Union:
		for _, c := range v.Union.GetChild() {
			if branchAcceptsWildcard(ts, objectType, relation, c, userObjectType, visited) {
				return true
			}
		}
		return false
	case *openfgav1.Userset_Intersection:
		for _, c := range v.Intersection.GetChild() {
			if branchAcceptsWildcard(ts, objectType, relation, c, userObjectType, visited) {
				return true
			}
		}
		return false
	case *openfgav1.Userset_Difference:
		return branchAcceptsWildcard(ts, objectType, relation, v.Difference.GetBase(), userObjectType, visited)
	}
	return false
}

// relationAcceptsWildcardForType reports whether the given (objectType,
// relation) is directly related to userObjectType:* (typed wildcard).
func relationAcceptsWildcardForType(ts *typesystem.TypeSystem, objectType, relation, userObjectType string) bool {
	directlyRelated, err := ts.GetDirectlyRelatedUserTypes(objectType, relation)
	if err != nil {
		return false
	}
	for _, dr := range directlyRelated {
		if dr.GetType() == userObjectType && dr.GetWildcard() != nil {
			return true
		}
	}
	return false
}
