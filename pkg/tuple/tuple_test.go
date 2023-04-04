package tuple

import (
	"testing"

	"github.com/stretchr/testify/require"
	openfgav1 "go.buf.build/openfga/go/openfga/api/openfga/v1"
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
	require.Equal(t, true, IsWildcard("*"))
	require.Equal(t, true, IsWildcard("user:*"))
	require.Equal(t, false, IsWildcard("user:jon"))
	require.Equal(t, false, IsWildcard("jon"))
}

func TestIsTypedWildcard(t *testing.T) {
	require.Equal(t, false, IsTypedWildcard("*"))
	require.Equal(t, true, IsTypedWildcard("user:*"))
	require.Equal(t, false, IsTypedWildcard("user:jon"))
	require.Equal(t, false, IsTypedWildcard("jon"))
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
			name:  "document:10",
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
			name: "document:10",
			want: User,
		},
		{
			name: "*",
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
