package eval

import (
	"context"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
)

func TestTupleConditionMet(t *testing.T) {
	tests := []struct {
		name         string
		tupleKey     *openfgav1.TupleKey
		model        *openfgav1.AuthorizationModel
		context      map[string]interface{}
		conditionMet bool
		expectedErr  string
	}{
		{
			name:     "condition_in_tuple_key_not_found_in_model",
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "unknown", nil),
			model: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type document
  relations
    define can_view: [user with correct_ip]

condition correct_ip(ip: string) {
	ip == "192.168.0.1"
}`),
			context:      map[string]interface{}{"ip": "192.168.0.1"},
			conditionMet: false,
			expectedErr:  "failed to evaluate relationship condition 'unknown': condition was not found",
		},
		{
			name:     "condition_not_met",
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "correct_ip", nil),
			model: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type document
  relations
    define can_view: [user with correct_ip]

condition correct_ip(ip: string) {
	ip == "192.168.0.1"
}`),
			context:      map[string]interface{}{"ip": "not_met"},
			conditionMet: false,
			expectedErr:  "",
		},
		{
			name:     "condition_met",
			tupleKey: tuple.NewTupleKeyWithCondition("document:1", "viewer", "user:maria", "correct_ip", nil),
			model: parser.MustTransformDSLToProto(`model
	schema 1.1
type user

type document
  relations
    define can_view: [user with correct_ip]

condition correct_ip(ip: string) {
	ip == "192.168.0.1"
}`),
			context:      map[string]interface{}{"ip": "192.168.0.1"},
			conditionMet: true,
			expectedErr:  "",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ts, err := typesystem.NewAndValidate(context.Background(), test.model)
			require.NoError(t, err)

			actualConditionMet, err := TupleConditionMet(test.tupleKey, ts, test.context)
			require.Equal(t, test.conditionMet, actualConditionMet)
			if err != nil {
				require.EqualError(t, err, test.expectedErr)
			} else {
				require.Empty(t, test.expectedErr)
			}
		})
	}
}
