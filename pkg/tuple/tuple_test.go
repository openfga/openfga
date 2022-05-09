package tuple

import (
	"testing"
)

func TestSplitObjectId(t *testing.T) {
	for _, tc := range []struct {
		name         string
		objectId     string
		expectedType string
		expectedOID  string
	}{
		{
			name: "empty",
		},
		{
			name:         "type only",
			objectId:     "foo:",
			expectedType: "foo",
		},
		{
			name:        "no separator",
			objectId:    "foo",
			expectedOID: "foo",
		},
		{
			name:         "missing type",
			objectId:     ":foo",
			expectedType: "",
			expectedOID:  "foo",
		},
		{
			name:         "valid input",
			objectId:     "foo:bar",
			expectedType: "foo",
			expectedOID:  "bar",
		},
		{
			name:         "separator in OID",
			objectId:     "url:https://bar/baz",
			expectedType: "url",
			expectedOID:  "https://bar/baz",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			td, oid := SplitObject(tc.objectId)
			if td != tc.expectedType {
				t.Errorf("SplitObject(%s) type was %s, want %s", tc.objectId, td, tc.expectedType)
			}
			if oid != tc.expectedOID {
				t.Errorf("SplitObject(%s) object id was %s, want %s", tc.objectId, oid, tc.expectedOID)
			}
		})
	}
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
			name:           "userset with no separator",
			objectRelation: "github|foo@bar.com",
			expectedObject: "github|foo@bar.com",
		},
		{
			name:             "valid input",
			objectRelation:   "foo:bar#baz",
			expectedObject:   "foo:bar",
			expectedRelation: "baz",
		},
		{
			name:           "trailing separator",
			objectRelation: "foo:bar#",
			expectedObject: "foo:bar",
		},
		{
			name:             "# in objectid",
			objectRelation:   "foo:bar#baz#reader",
			expectedObject:   "foo:bar#baz",
			expectedRelation: "reader",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			obj, rel := SplitObjectRelation(tc.objectRelation)
			if obj != tc.expectedObject {
				t.Errorf("SplitObjectRelation(%s) object was %s, want %s", tc.objectRelation, obj, tc.expectedObject)
			}
			if rel != tc.expectedRelation {
				t.Errorf("SplitObjectRelation(%s) relation was %s, want %s", tc.objectRelation, rel, tc.expectedRelation)
			}
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
			name:           "invalid object (missing type)",
			objectRelation: "foo#bar",
			expected:       false,
		},
		{
			name:           "user literal",
			objectRelation: "github|foo@bar.com",
			expected:       false,
		},
		{
			name:           "valid",
			objectRelation: "foo:bar#baz",
			expected:       true,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := IsObjectRelation(tc.objectRelation)
			if got != tc.expected {
				t.Errorf("IsObjectRelation(%s) = %v, want %v", tc.objectRelation, got, tc.expected)
			}
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := IsValidObject(tc.name)
			if got != tc.valid {
				t.Errorf("IsValidObject(%s) = %v, want %v", tc.name, got, tc.valid)
			}
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
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := IsValidRelation(tc.name)
			if got != tc.valid {
				t.Errorf("IsValidRelation(%s) = %v, want %v", tc.name, got, tc.valid)
			}
		})
	}
}

func TestIsValidUser(t *testing.T) {
	for _, tc := range []struct {
		name  string
		valid bool
	}{
		{
			name:  "anne@auth0.com",
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
			name:  "anne@auth0 .com", // empty space
			valid: false,
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			got := IsValidUser(tc.name)
			if got != tc.valid {
				t.Errorf("IsValidUser(%s) = %v, want %v", tc.name, got, tc.valid)
			}
		})
	}
}

func TestGetUsertypeFromUser(t *testing.T) {
	for _, tc := range []struct {
		name string
		want UserType
	}{
		{
			name: "anne@auth0.com",
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
			name: "github|jon.allie@auth0.com",
			want: User,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := GetUserTypeFromUser(tc.name)
			if got != tc.want {
				t.Errorf("GetUserTypeFromUser(%s) = %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}
