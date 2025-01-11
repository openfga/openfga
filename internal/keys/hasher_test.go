package keys

import (
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
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

// ResetableStringWriter is an interface that groups the
// io.StringWriter and fmt.Stringer interfaces with a
// Reset method.
type ResetableStringWriter interface {
	io.StringWriter
	fmt.Stringer
	Reset()
}

// ErrWriteString is an error used exclusively for testing
// Write functions.
var ErrWriteString = errors.New("test error")

// An ErrorStringWriter counts the calls made to its WriteString
// method and returns an error when the number of calls is
// greater than or equal to TriggerAt.
type ErrorStringWriter struct {
	TriggerAt int // number of calls to WriteString before returning an error
	current   int // the number of calls to WriteString up to this point
}

// WriteString ignores string s and returns the length of string s
// in bytes with a nil error. When the number of calls to WriteString
// is greater than or equal to TriggerAt WriteString will return
// 0 bytes and an error.
func (e *ErrorStringWriter) WriteString(s string) (int, error) {
	e.current++
	if e.current >= e.TriggerAt {
		return 0, ErrWriteString
	}
	return len([]byte(s)), nil
}

// Reset sets the number of calls made to WriteString up to this
// point, to 0.
func (e *ErrorStringWriter) Reset() {
	e.current = 0
}

// String always returns an empty string value.
func (e *ErrorStringWriter) String() string {
	return ""
}

var validWriter strings.Builder // A global string builder intended for reuse across tests

func TestWriteValue(t *testing.T) {
	var cases = map[string]struct {
		writer ResetableStringWriter
		value  *structpb.Value
		output string
		error  bool
	}{
		"list": {
			writer: &validWriter,
			value: structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("A"),
					structpb.NewNullValue(),
					structpb.NewBoolValue(true),
					structpb.NewNumberValue(1111111111),
					structpb.NewStructValue(MustNewStruct(map[string]any{
						"key": "value",
					})),
				},
			}),
			output: "A,null,true,1111111111,'key:'value,",
		},
		"list_write_value_error": {
			writer: &ErrorStringWriter{},
			value: structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewStringValue("A"),
					structpb.NewNullValue(),
					structpb.NewBoolValue(true),
					structpb.NewNumberValue(1111111111),
					structpb.NewStructValue(MustNewStruct(map[string]any{
						"key": "value",
					})),
				},
			}),
			error: true,
		},
		"list_write_comma_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 2,
			},
			value: structpb.NewListValue(&structpb.ListValue{
				Values: []*structpb.Value{
					structpb.NewNullValue(),
					structpb.NewNullValue(),
				},
			}),
			error: true,
		},
		"nil": {
			writer: &validWriter,
			value:  nil,
			error:  true,
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			test.writer.Reset()
			err := WriteValue(test.writer, test.value)
			if test.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, test.writer.String())
			}
		})
	}
}

func TestWriteStruct(t *testing.T) {
	var cases = map[string]struct {
		writer ResetableStringWriter
		value  *structpb.Struct
		output string
		error  bool
	}{
		"general": {
			writer: &validWriter,
			value: MustNewStruct(map[string]any{
				"keyA": "valueA",
				"keyB": "valueB",
			}),
			output: "'keyA:'valueA,'keyB:'valueB,",
		},
		"fields_write_key_error": {
			writer: &ErrorStringWriter{},
			value: MustNewStruct(map[string]any{
				"keyA": "valueA",
				"keyB": "valueB",
			}),
			error: true,
		},
		"fields_write_value_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 2,
			},
			value: MustNewStruct(map[string]any{
				"keyA": "valueA",
				"keyB": "valueB",
			}),
			error: true,
		},
		"fields_write_comma_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 3,
			},
			value: MustNewStruct(map[string]any{
				"keyA": "valueA",
				"keyB": "valueB",
			}),
			error: true,
		},
		"nil": {
			writer: &validWriter,
			value:  nil,
			output: "",
		},
		"nil_error": {
			writer: &ErrorStringWriter{},
			value:  nil,
			output: "",
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			test.writer.Reset()
			err := WriteStruct(test.writer, test.value)
			if test.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, test.writer.String())
			}
		})
	}
}

func TestWriteTuples(t *testing.T) {
	var cases = map[string]struct {
		writer ResetableStringWriter
		tuples []*openfgav1.TupleKey
		output string
		error  bool
	}{
		"sans_condition": {
			writer: &validWriter,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:C", "relationC", "user:C"),
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
				tuple.NewTupleKey("document:B", "relationB", "user:B"),
			},
			output: "/document:A#relationA@user:A,document:B#relationB@user:B,document:C#relationC@user:C",
		},
		"write_forward_slash_error": {
			writer: &ErrorStringWriter{},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
			},
			error: true,
		},
		"write_object_relation_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 2,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
			},
			error: true,
		},
		"write_user_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 3,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
			},
			error: true,
		},
		"write_comma_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 3,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKey("document:A", "relationA", "user:A"),
				tuple.NewTupleKey("document:B", "relationB", "user:B"),
			},
			error: true,
		},
		"with_condition": {
			writer: &validWriter,
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"B",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"A",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
			},
			output: "/document:A#relationA with A 'key:'value,@user:A,document:A#relationA with B 'key:'value,@user:A",
		},
		"with_condition_write_with_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 3,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"B",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"A",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
			},
			error: true,
		},
		"with_condition_write_space_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 4,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"B",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"A",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
			},
			error: true,
		},
		"with_condition_write_context_error": {
			writer: &ErrorStringWriter{
				TriggerAt: 5,
			},
			tuples: []*openfgav1.TupleKey{
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"B",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
				tuple.NewTupleKeyWithCondition(
					"document:A",
					"relationA",
					"user:A",
					"A",
					MustNewStruct(map[string]any{
						"key": "value",
					}),
				),
			},
			error: true,
		},
	}

	for name, test := range cases {
		t.Run(name, func(t *testing.T) {
			test.writer.Reset()
			err := WriteTuples(test.writer, test.tuples...)
			if test.error {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, test.output, test.writer.String())
			}
		})
	}
}
