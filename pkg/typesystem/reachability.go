package typesystem

import openfgav1 "github.com/openfga/api/proto/openfga/v1"

// SubjectReachabilityInfo contains information about the potential reachability of different
// kinds of relationships between some `subjectType#subjectRelation` and one or more related
// types and their relation(s).
type SubjectReachabilityInfo struct {
	HasPossibleDirectRelationship   bool
	HasPossibleWildcardRelationship bool
	NonTerminalRelationRefs         []*openfgav1.RelationReference
}

// HasNonTerminalRelationships returns true if there is at least 1 relationship
// to a non-terminal type restriction (e.g. group#member). Non-terminal relationships
// are relatoinships that require an extra lookup followed by a dispatch for further
// evaluation.
func (s SubjectReachabilityInfo) HasNonTerminalRelationships() bool {
	return len(s.NonTerminalRelationRefs) > 0
}

// SubjectReachability returns information about the potential reachability of relationships for a given
// `subjectType#subjectRelation` w.r.t. the provided type restrictions provided in the `assignableTypes“.
func SubjectReachability(
	subjectType, subjectRelation string,
	assignableTypes []*openfgav1.RelationReference,
) SubjectReachabilityInfo {
	var possibleDirectRelationship, possibleWildcardRelationship bool
	nonTerminalRelationRefs := []*openfgav1.RelationReference{}

	for _, assignableTypeRef := range assignableTypes {
		if assignableTypeRef.GetType() == subjectType {
			if assignableTypeRef.GetRelationOrWildcard() == nil && subjectRelation == "" {
				possibleDirectRelationship = true
				continue
			}

			if assignableTypeRef.GetWildcard() != nil && subjectRelation == "" {
				possibleWildcardRelationship = true
				continue
			}

			if assignableTypeRef.GetRelation() != "" && subjectRelation == assignableTypeRef.GetRelation() {
				possibleDirectRelationship = true
				continue
			}
		}

		if assignableTypeRef.GetRelation() != "" {
			// todo: prune/optimize non-terminal type restrictions by omitting those who are not connected
			// whatsoever to the target `subjectType#subjectRelation`. These don't need to be included in
			// the search since they wouldn't lead to the subject in the first place.
			//
			// if !typesys.IsConnectedSubgraph(subjectType, subjectRelation, directlyAssignableTypeRef) { continue }

			nonTerminalRelationRefs = append(nonTerminalRelationRefs, assignableTypeRef)
		}
	}

	return SubjectReachabilityInfo{
		possibleDirectRelationship,
		possibleWildcardRelationship,
		nonTerminalRelationRefs,
	}
}
