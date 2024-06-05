package typesystem

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func TestSubjectReachability(t *testing.T) {
	testCases := []struct {
		name                         string
		subjectType, subjectRelation string
		assignableTypes              []*openfgav1.RelationReference
		expectedReachabilityInfo     SubjectReachabilityInfo
	}{
		{
			name:            "possible_terminal_relationship_with_subject",
			subjectType:     "user",
			subjectRelation: "",
			assignableTypes: []*openfgav1.RelationReference{
				DirectRelationReference("user", ""),
			},
			expectedReachabilityInfo: SubjectReachabilityInfo{
				HasPossibleDirectRelationship:   true,
				HasPossibleWildcardRelationship: false,
				NonTerminalRelationRefs:         []*openfgav1.RelationReference{},
			},
		},
		{
			name:            "no_terminal_relationship_with_subject",
			subjectType:     "user",
			subjectRelation: "",
			assignableTypes: []*openfgav1.RelationReference{
				DirectRelationReference("employee", ""),
			},
			expectedReachabilityInfo: SubjectReachabilityInfo{
				HasPossibleDirectRelationship:   false,
				HasPossibleWildcardRelationship: false,
				NonTerminalRelationRefs:         []*openfgav1.RelationReference{},
			},
		},
		{
			name:            "possible_terminal_relationship_with_wildcard",
			subjectType:     "user",
			subjectRelation: "",
			assignableTypes: []*openfgav1.RelationReference{
				WildcardRelationReference("user"),
			},
			expectedReachabilityInfo: SubjectReachabilityInfo{
				HasPossibleDirectRelationship:   false,
				HasPossibleWildcardRelationship: true,
				NonTerminalRelationRefs:         []*openfgav1.RelationReference{},
			},
		},
		{
			name:            "no_terminal_relationship_with_wildcard",
			subjectType:     "user",
			subjectRelation: "",
			assignableTypes: []*openfgav1.RelationReference{
				WildcardRelationReference("employee"),
			},
			expectedReachabilityInfo: SubjectReachabilityInfo{
				HasPossibleDirectRelationship:   false,
				HasPossibleWildcardRelationship: false,
				NonTerminalRelationRefs:         []*openfgav1.RelationReference{},
			},
		},
		{
			name:            "non_terminal_relationships",
			subjectType:     "user",
			subjectRelation: "",
			assignableTypes: []*openfgav1.RelationReference{
				DirectRelationReference("group", "member"),
				DirectRelationReference("org", "member"),
			},
			expectedReachabilityInfo: SubjectReachabilityInfo{
				HasPossibleDirectRelationship:   false,
				HasPossibleWildcardRelationship: false,
				NonTerminalRelationRefs: []*openfgav1.RelationReference{
					DirectRelationReference("group", "member"),
					DirectRelationReference("org", "member"),
				},
			},
		},
		{
			name:            "possible_terminal_subject_and_wildcard_with_non_terminal_relationships",
			subjectType:     "user",
			subjectRelation: "",
			assignableTypes: []*openfgav1.RelationReference{
				DirectRelationReference("user", ""),
				WildcardRelationReference("user"),
				DirectRelationReference("group", "member"),
				DirectRelationReference("org", "member"),
			},
			expectedReachabilityInfo: SubjectReachabilityInfo{
				HasPossibleDirectRelationship:   true,
				HasPossibleWildcardRelationship: true,
				NonTerminalRelationRefs: []*openfgav1.RelationReference{
					DirectRelationReference("group", "member"),
					DirectRelationReference("org", "member"),
				},
			},
		},
		{
			name:            "no_terminal_subject_and_wildcard_with_non_terminal_relationships",
			subjectType:     "user",
			subjectRelation: "",
			assignableTypes: []*openfgav1.RelationReference{
				DirectRelationReference("employee", ""),
				WildcardRelationReference("employee"),
				DirectRelationReference("group", "member"),
				DirectRelationReference("org", "member"),
			},
			expectedReachabilityInfo: SubjectReachabilityInfo{
				HasPossibleDirectRelationship:   false,
				HasPossibleWildcardRelationship: false,
				NonTerminalRelationRefs: []*openfgav1.RelationReference{
					DirectRelationReference("group", "member"),
					DirectRelationReference("org", "member"),
				},
			},
		},
		{
			name:            "no_terminal_subject_and_possible_wildcard_with_non_terminal_relationships",
			subjectType:     "user",
			subjectRelation: "",
			assignableTypes: []*openfgav1.RelationReference{
				DirectRelationReference("employee", ""),
				WildcardRelationReference("user"),
				DirectRelationReference("group", "member"),
			},
			expectedReachabilityInfo: SubjectReachabilityInfo{
				HasPossibleDirectRelationship:   false,
				HasPossibleWildcardRelationship: true,
				NonTerminalRelationRefs: []*openfgav1.RelationReference{
					DirectRelationReference("group", "member"),
				},
			},
		},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			reachabilityInfo := SubjectReachability(testCase.subjectType, testCase.subjectRelation, testCase.assignableTypes)

			require.Equal(t, testCase.expectedReachabilityInfo, reachabilityInfo)

			require.Equal(t, len(testCase.expectedReachabilityInfo.NonTerminalRelationRefs) == 0, reachabilityInfo.HasNonTerminalRelationships() == false)
			require.Equal(t, len(testCase.expectedReachabilityInfo.NonTerminalRelationRefs) > 0, reachabilityInfo.HasNonTerminalRelationships() == true)
		})
	}
}
