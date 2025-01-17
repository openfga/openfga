package tuple

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

// MustNewStruct returns a new *structpb.Struct or panics
// on error. The new *structpb.Struct value is built from
// the map m.
func MustNewStruct(m map[string]any) *structpb.Struct {
	s, err := structpb.NewStruct(m)
	if err == nil {
		return s
	}
	panic(err)
}

func TestTupleKeys_Len(t *testing.T) {
	tuples := TupleKeys{
		NewTupleKey("document:A", "relationA", "user:A"),
		NewTupleKey("document:B", "relationB", "user:B"),
	}
	require.Equal(t, 2, tuples.Len())
}

func TestTupleKeys_Swap(t *testing.T) {
	tuples := TupleKeys{
		NewTupleKey("document:A", "relationA", "user:A"),
		NewTupleKey("document:B", "relationB", "user:B"),
	}
	tuples.Swap(0, 1)
	require.Equal(t, "document:B", tuples[0].GetObject())
	require.Equal(t, "document:A", tuples[1].GetObject())
}

func TestTupleKeys_Less(t *testing.T) {
	var cases = map[string]struct {
		value TupleKeys
		i     int
		j     int
		ij    bool
		ji    bool
	}{
		"object": {
			value: TupleKeys{
				NewTupleKey("document:A", "relationA", "user:A"),
				NewTupleKey("document:B", "relationA", "user:A"),
			},
			i:  0,
			j:  1,
			ij: true,
			ji: false,
		},
		"relation": {
			value: TupleKeys{
				NewTupleKey("document:A", "relationA", "user:A"),
				NewTupleKey("document:A", "relationB", "user:A"),
			},
			i:  0,
			j:  1,
			ij: true,
			ji: false,
		},
		"user": {
			value: TupleKeys{
				NewTupleKey("document:A", "relationA", "user:A"),
				NewTupleKey("document:A", "relationA", "user:B"),
			},
			i:  0,
			j:  1,
			ij: true,
			ji: false,
		},
		"condition": {
			value: TupleKeys{
				NewTupleKeyWithCondition("document:A", "relationA", "user:A", "testA", MustNewStruct(map[string]any{
					"key": "value",
				})),
				NewTupleKeyWithCondition("document:A", "relationA", "user:A", "testB", MustNewStruct(map[string]any{
					"key": "value",
				})),
			},
			i:  0,
			j:  1,
			ij: true,
			ji: false,
		},
		"nil_condition": {
			value: TupleKeys{
				NewTupleKey("document:A", "relationA", "user:A"),
				NewTupleKeyWithCondition("document:A", "relationA", "user:A", "test", MustNewStruct(map[string]any{
					"key": "value",
				})),
			},
			i:  0,
			j:  1,
			ij: true,
			ji: false,
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.ij, test.value.Less(test.i, test.j))
			require.Equal(t, test.ji, test.value.Less(test.j, test.i))
		})
	}
}

func TestTuple_Getters(t *testing.T) {
	tupleKey := &openfgav1.TupleKey{
		Object:   "document:A",
		Relation: "relationA",
		User:     "user:A",
	}
	tup := From(tupleKey)

	t.Run("get_object", func(t *testing.T) {
		require.Equal(t, "document:A", tup.GetObject())
	})

	t.Run("get_relation", func(t *testing.T) {
		require.Equal(t, "relationA", tup.GetRelation())
	})

	t.Run("get_user", func(t *testing.T) {
		require.Equal(t, "user:A", tup.GetUser())
	})
}

func TestTuple_String(t *testing.T) {
	tupleKey := &openfgav1.TupleKey{
		Object:   "document:A",
		Relation: "relationA",
		User:     "user:A",
	}

	result := From(tupleKey).String()
	require.Equal(t, "document:A#relationA@user:A", result)
}

func TestSplitObjectId(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		objectID                string
		expectedTypeAndRelation string
		expectedOID             string
	}{
		{
			name: "empty",
		},
		{
			name:                    "type_only",
			objectID:                "foo:",
			expectedTypeAndRelation: "foo",
		},

		{
			name:        "no_separator",
			objectID:    "foo",
			expectedOID: "foo",
		},
		{
			name:                    "missing_type",
			objectID:                ":foo",
			expectedTypeAndRelation: "",
			expectedOID:             "foo",
		},
		{
			name:                    "valid_input_with_relation",
			objectID:                "foo#bar:baz",
			expectedTypeAndRelation: "foo#bar",
			expectedOID:             "baz",
		},
		{
			name:                    "valid_input_without_relation",
			objectID:                "foo:bar",
			expectedTypeAndRelation: "foo",
			expectedOID:             "bar",
		},
		{
			name:                    "separator_in_OID",
			objectID:                "url:https://bar/baz",
			expectedTypeAndRelation: "url",
			expectedOID:             "https://bar/baz",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			td, oid := SplitObject(tc.objectID)

			require.Equal(t, tc.expectedTypeAndRelation, td)
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
	require.Equal(t, "document:1#viewer@group:fga#member", TupleKeyToString(NewTupleKey("document:1", "viewer", "group:fga#member")))
	require.Equal(t, "document:1#viewer@jon", TupleKeyToString(NewTupleKey("document:1", "viewer", "jon")))
	require.Equal(t, "document:1#viewer@user:bob", TupleKeyToString(NewTupleKey("document:1", "viewer", "user:bob")))
	require.Equal(t, "document:1#viewer@", TupleKeyToString(NewTupleKey("document:1", "viewer", "")))
	require.Equal(t, "document:1#@jon", TupleKeyToString(NewTupleKey("document:1", "", "jon")))
	require.Equal(t, "#viewer@jon", TupleKeyToString(NewTupleKey("", "viewer", "jon")))
	require.Equal(t, "#@", TupleKeyToString(NewTupleKey("", "", "")))
}

func TestTupleKeyWithConditionToString(t *testing.T) {
	require.Equal(t, "document:1#viewer@group:fga#member (condition condX)",
		TupleKeyWithConditionToString(NewTupleKeyWithCondition("document:1", "viewer", "group:fga#member", "condX", nil)))
	require.Equal(t, "document:1#viewer@user:maria (condition condX)",
		TupleKeyWithConditionToString(NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "condX", nil)))
	require.Equal(t, "document:1#viewer@user:* (condition condX)",
		TupleKeyWithConditionToString(NewTupleKeyWithCondition("document:1", "viewer", "user:*", "condX", nil)))
	require.Equal(t, "document:1#viewer@user:*",
		TupleKeyWithConditionToString(NewTupleKey("document:1", "viewer", "user:*")))
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
		{
			name:  "group:*#member",
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

func TestFromUserProto(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    *openfgav1.User
		expected string
	}{
		{
			name: "user_with_id",
			input: &openfgav1.User{
				User: &openfgav1.User_Object{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "id-123",
					},
				},
			},
			expected: "user:id-123",
		},
		{
			name: "user_with_id_and_relation",
			input: &openfgav1.User{
				User: &openfgav1.User_Userset{
					Userset: &openfgav1.UsersetUser{
						Type:     "user",
						Id:       "id-123",
						Relation: "member",
					},
				},
			},
			expected: "user:id-123#member",
		},
		{
			name: "user_with_wildcard",
			input: &openfgav1.User{
				User: &openfgav1.User_Wildcard{
					Wildcard: &openfgav1.TypedWildcard{
						Type: "user",
					},
				},
			},
			expected: "user:*",
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			actual := UserProtoToString(tc.input)
			require.Equal(t, tc.expected, actual)
		})
	}
}

func TestToUserProto(t *testing.T) {
	for _, tc := range []struct {
		name     string
		input    string
		expected *openfgav1.User
	}{
		{
			name:  "user_with_id",
			input: "user:id-123",
			expected: &openfgav1.User{
				User: &openfgav1.User_Object{
					Object: &openfgav1.Object{
						Type: "user",
						Id:   "id-123",
					},
				},
			},
		},
		{
			name:  "user_with_id_and_relation",
			input: "user:id-123#member",
			expected: &openfgav1.User{
				User: &openfgav1.User_Userset{
					Userset: &openfgav1.UsersetUser{
						Type:     "user",
						Id:       "id-123",
						Relation: "member",
					},
				},
			},
		},
		{
			name:  "user_with_wildcard",
			input: "user:*",
			expected: &openfgav1.User{
				User: &openfgav1.User_Wildcard{
					Wildcard: &openfgav1.TypedWildcard{
						Type: "user",
					},
				},
			},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			actual := StringToUserProto(tc.input)
			require.Equal(t, tc.expected, actual)
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

func TestFromUserParts(t *testing.T) {
	require.Equal(t, "jon", FromUserParts("", "jon", ""))
	require.Equal(t, "user:jon", FromUserParts("user", "jon", ""))
	require.Equal(t, "user:*", FromUserParts("user", "*", ""))
	require.Equal(t, "group:eng#member", FromUserParts("group", "eng", "member"))
}

func TestToUserParts(t *testing.T) {
	userObjectType, userObjectID, userRelation := ToUserParts("jon")
	require.Equal(t, "", userObjectType)
	require.Equal(t, "jon", userObjectID)
	require.Equal(t, "", userRelation)

	userObjectType, userObjectID, userRelation = ToUserParts("user:jon")
	require.Equal(t, "user", userObjectType)
	require.Equal(t, "jon", userObjectID)
	require.Equal(t, "", userRelation)

	userObjectType, userObjectID, userRelation = ToUserParts("user:*")
	require.Equal(t, "user", userObjectType)
	require.Equal(t, "*", userObjectID)
	require.Equal(t, "", userRelation)

	userObjectType, userObjectID, userRelation = ToUserParts("group:eng#member")
	require.Equal(t, "group", userObjectType)
	require.Equal(t, "eng", userObjectID)
	require.Equal(t, "member", userRelation)
}

func TestToUserPartsFromObjectRelation(t *testing.T) {
	userObjectType, userObjectID, userRelation := ToUserPartsFromObjectRelation(&openfgav1.ObjectRelation{Object: "group:eng", Relation: "member"})
	require.Equal(t, "group", userObjectType)
	require.Equal(t, "eng", userObjectID)
	require.Equal(t, "member", userRelation)

	userObjectType, userObjectID, userRelation = ToUserPartsFromObjectRelation(&openfgav1.ObjectRelation{Object: "user:*"})
	require.Equal(t, "user", userObjectType)
	require.Equal(t, "*", userObjectID)
	require.Equal(t, "", userRelation)
}

func TestIsSelfDefining(t *testing.T) {
	testCases := map[string]struct {
		tuples       []string
		selfDefining bool
	}{
		`true`: {
			selfDefining: true,
			tuples: []string{
				"group:1#member@group:1#member",
			},
		},
		`false`: {
			selfDefining: false,
			tuples: []string{
				"group:2#member@group:1#member",
				"group:1#member@group:2#member",
				"document:1#member@group:1#member",
				"group:1#member@document:1#member",
				"group:1#member@group:1#viewer",
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for _, tuple := range tc.tuples {
				tupleKey := MustParseTupleString(tuple)
				require.Equal(t, tc.selfDefining, IsSelfDefining(tupleKey), tupleKey)
			}
		})
	}
}

func TestUsersetMatchesTypeAndRelation(t *testing.T) {
	testCases := map[string]struct {
		inputs  [][]string // userset, relation, type
		matches bool
	}{
		`true`: {
			matches: true,
			inputs: [][]string{
				{"group:1#member", "member", "group"},
			},
		},
		`false`: {
			matches: false,
			inputs: [][]string{
				{"group:1#member", "unknown", "group"},
				{"group:1#member", "member", "unknown"},
			},
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			for _, input := range tc.inputs {
				actual := UsersetMatchTypeAndRelation(input[0], input[1], input[2])
				require.Equal(t, tc.matches, actual, input)
			}
		})
	}
}
