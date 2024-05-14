package tuple

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
)

func TestSplitObjectId(t *testing.T) {
	for _, tc := range []struct {
		name         string
		objectID     string
		expectedType string
		expectedOID  string
	}{
		{
			name: "empty",
		},
		{
			name:         "type_only",
			objectID:     "foo:",
			expectedType: "foo",
		},
		{
			name:        "no_separator",
			objectID:    "foo",
			expectedOID: "foo",
		},
		{
			name:         "missing_type",
			objectID:     ":foo",
			expectedType: "",
			expectedOID:  "foo",
		},
		{
			name:         "valid_input",
			objectID:     "foo:bar",
			expectedType: "foo",
			expectedOID:  "bar",
		},
		{
			name:         "separator_in_OID",
			objectID:     "url:https://bar/baz",
			expectedType: "url",
			expectedOID:  "https://bar/baz",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			td, oid := SplitObject(tc.objectID)

			require.Equal(t, tc.expectedType, td)
			require.Equal(t, tc.expectedOID, oid)
		})
	}
}

func TestObjectKey(t *testing.T) {
	key := ObjectKey(&openfgav1.Object{
		Type: "document",
		Id:   "1",
	})
	require.Equal(t, "document:1", key)
}

func TestSplitObjectRelation(t *testing.T) {
	for _, tc := range []struct {
		name             string
		objectRelation   string
		expectedObject   string
		expectedRelation string
	}{
		{
			name: "empty",
		},
		{
			name:           "userset_with_no_separator",
			objectRelation: "github|foo@bar.com",
			expectedObject: "github|foo@bar.com",
		},
		{
			name:             "valid_input",
			objectRelation:   "foo:bar#baz",
			expectedObject:   "foo:bar",
			expectedRelation: "baz",
		},
		{
			name:           "trailing_separator",
			objectRelation: "foo:bar#",
			expectedObject: "foo:bar",
		},
		{
			name:             "#_in_objectid",
			objectRelation:   "foo:bar#baz#reader",
			expectedObject:   "foo:bar#baz",
			expectedRelation: "reader",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			obj, rel := SplitObjectRelation(tc.objectRelation)

			require.Equal(t, tc.expectedObject, obj)
			require.Equal(t, tc.expectedRelation, rel)
		})
	}
}

func TestIsObjectRelation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		objectRelation string
		expected       bool
	}{
		{
			name:     "empty",
			expected: false,
		},
		{
			name:           "invalid_object_(missing_type)",
			objectRelation: "foo#bar",
			expected:       false,
		},
		{
			name:           "user_literal",
			objectRelation: "github|foo@bar.com",
			expected:       false,
		},
		{
			name:           "valid",
			objectRelation: "foo:bar#baz",
			expected:       true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := IsObjectRelation(tc.objectRelation)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestIsValidObject(t *testing.T) {
	for _, tc := range []struct {
		name  string
		valid bool
	}{
		{
			name:  "repo:sandcastle",
			valid: true,
		},
		{
			name:  "group:group:group",
			valid: false,
		},
		{
			name:  "github:org-iam#member",
			valid: false,
		},
		{
			name:  "repo:sand castle", // empty space
			valid: false,
		},
		{
			name:  "fga",
			valid: false,
		},
		{
			name:  "group#group1:member",
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := IsValidObject(tc.name)
			require.Equal(t, tc.valid, got)
		})
	}
}

func TestIsValidRelation(t *testing.T) {
	for _, tc := range []struct {
		name  string
		valid bool
	}{
		{
			name:  "repo:sandcastle",
			valid: false,
		},
		{
			name:  "group#group",
			valid: false,
		},
		{
			name:  "git hub",
			valid: false,
		},
		{
			name:  "imavalidrelation",
			valid: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := IsValidRelation(tc.name)
			require.Equal(t, tc.valid, got)
		})
	}
}

func TestBuildObject(t *testing.T) {
	require.Equal(t, "document:1", BuildObject("document", "1"))
	require.Equal(t, ":", BuildObject("", ""))
}

func TestGetType(t *testing.T) {
	require.Equal(t, "document", GetType("document:1"))
	require.Equal(t, "", GetType("doc"))
	require.Equal(t, "", GetType(":"))
	require.Equal(t, "", GetType(""))
}

func TestToObjectRelationString(t *testing.T) {
	require.Equal(t, "document:1#viewer", ToObjectRelationString("document:1", "viewer"))
	require.Equal(t, "#viewer", ToObjectRelationString("", "viewer"))
	require.Equal(t, "#", ToObjectRelationString("", ""))
}

func TestTupleKeyToString(t *testing.T) {
	require.Equal(t, "document:1#viewer@jon", TupleKeyToString(NewTupleKey("document:1", "viewer", "jon")))
	require.Equal(t, "document:1#viewer@user:bob", TupleKeyToString(NewTupleKey("document:1", "viewer", "user:bob")))
	require.Equal(t, "document:1#viewer@", TupleKeyToString(NewTupleKey("document:1", "viewer", "")))
	require.Equal(t, "document:1#@jon", TupleKeyToString(NewTupleKey("document:1", "", "jon")))
	require.Equal(t, "#viewer@jon", TupleKeyToString(NewTupleKey("", "viewer", "jon")))
	require.Equal(t, "#@", TupleKeyToString(NewTupleKey("", "", "")))
}

func TestIsWildcard(t *testing.T) {
	require.True(t, IsWildcard("*"))
	require.True(t, IsWildcard("user:*"))
	require.False(t, IsWildcard("user:jon"))
	require.False(t, IsWildcard("jon"))
}

func TestIsTypedWildcard(t *testing.T) {
	require.False(t, IsTypedWildcard("*"))
	require.True(t, IsTypedWildcard("user:*"))
	require.False(t, IsTypedWildcard("user:jon"))
	require.False(t, IsTypedWildcard("jon"))
}

func TestIsValidUser(t *testing.T) {
	for _, tc := range []struct {
		name  string
		valid bool
	}{
		{
			name:  "anne@openfga",
			valid: true,
		},
		{
			name:  "*",
			valid: true,
		},
		{
			name:  "user:*",
			valid: true,
		},
		{
			name:  "user:10",
			valid: true,
		},
		{
			name:  "github:org-iam#member",
			valid: true,
		},
		{
			name:  "john:albert:doe",
			valid: false,
		},
		{
			name:  "john#albert#doe",
			valid: false,
		},
		{
			name:  "invalid#test:go",
			valid: false,
		},
		{
			name:  "anne@openfga .com", // empty space
			valid: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := IsValidUser(tc.name)
			require.Equal(t, tc.valid, got)
		})
	}
}

func TestGetUsertypeFromUser(t *testing.T) {
	for _, tc := range []struct {
		name string
		want UserType
	}{
		{
			name: "anne@openfga",
			want: User,
		},
		{
			name: "user:10",
			want: User,
		},
		{
			name: "*",
			want: UserSet,
		},
		{
			name: "user:*",
			want: UserSet,
		},
		{
			name: "github:org-iam#member",
			want: UserSet,
		},
		{
			name: "github|jon.allie@openfga",
			want: User,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := GetUserTypeFromUser(tc.name)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestGetObjectRelationAsString(t *testing.T) {
	for _, tc := range []struct {
		name  string
		input *openfgav1.ObjectRelation
		want  string
	}{
		{
			name: "object_and_relation",
			input: &openfgav1.ObjectRelation{
				Object:   "team:fga",
				Relation: "member",
			},
			want: "team:fga#member",
		},
		{
			name: "just_object",
			input: &openfgav1.ObjectRelation{
				Object: "team:fga",
			},
			want: "team:fga",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := GetObjectRelationAsString(tc.input)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestTypedPublicWildcard(t *testing.T) {
	require.Equal(t, "user:*", TypedPublicWildcard("user"))
	require.Equal(t, "group:*", TypedPublicWildcard("group"))
	require.Equal(t, ":*", TypedPublicWildcard("")) // Does not panic
}

func TestParseTupleString(t *testing.T) {
	tests := []struct {
		name        string
		str         string
		tuple       *openfgav1.TupleKey
		expectedErr bool
	}{
		{
			name:  "well_formed_user_object",
			str:   "document:1#viewer@user:jon",
			tuple: NewTupleKey("document:1", "viewer", "user:jon"),
		},
		{
			name:  "well_formed_user_userset",
			str:   "document:1#viewer@group:eng#member",
			tuple: NewTupleKey("document:1", "viewer", "group:eng#member"),
		},
		{
			name:  "well_formed_user_wildcard",
			str:   "document:1#viewer@user:*",
			tuple: NewTupleKey("document:1", "viewer", "user:*"),
		},
		{
			name:        "missing_user_field",
			str:         "document:1#viewer",
			expectedErr: true,
		},
		{
			name:        "missing_relation_separator",
			str:         "document:1viewer@user:jon",
			expectedErr: true,
		},
		{
			name:        "missing_object_id",
			str:         "document#viewer@user:jon",
			expectedErr: true,
		},
		{
			name:        "malformed_object_type",
			str:         "docu#ment:1#viewer@user:jon",
			expectedErr: true,
		},
		{
			name:        "malformed_object_id",
			str:         "document:#1#viewer@user:jon",
			expectedErr: true,
		},
		{
			name:        "malformed_relation",
			str:         "document:1#vie:wer@user:jon",
			expectedErr: true,
		},
		{
			name:        "malformed_user_type",
			str:         "document:1#viewer@use#r:jon",
			expectedErr: true,
		},
		{
			name:        "malformed_user_userset",
			str:         "document:1#viewer@group:e:eng#member",
			expectedErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tuple, err := ParseTupleString(test.str)
			require.Equal(t, test.expectedErr, err != nil)

			if !test.expectedErr {
				require.NotNil(t, tuple)
				require.Equal(t, test.tuple, tuple)
			}
		})
	}
}
