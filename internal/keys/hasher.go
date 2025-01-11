package keys

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"

	"golang.org/x/exp/maps"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

// ErrUnexpectedStructValue is an error used to indicate that
// an unexpected structpb.Value kind was encountered.
var ErrUnexpectedStructValue = errors.New("unexpected structpb value encountered")

// WriteValue writes value v to the writer w. An error
// is returned only when the underlying writer returns
// an error or an unexpected value kind is encountered.
func WriteValue(w io.StringWriter, v *structpb.Value) (err error) {
	switch val := v.GetKind().(type) {
	case *structpb.Value_BoolValue:
		_, err = w.WriteString(fmt.Sprintf("%v", val.BoolValue))
	case *structpb.Value_NullValue:
		_, err = w.WriteString("null")
	case *structpb.Value_StringValue:
		_, err = w.WriteString(val.StringValue)
	case *structpb.Value_NumberValue:
		_, err = w.WriteString(strconv.FormatFloat(val.NumberValue, 'f', -1, 64)) // -1 precision ensures we represent the 64-bit value with the maximum precision needed to represent it, see strconv#FormatFloat for more info.
	case *structpb.Value_ListValue:
		values := val.ListValue.GetValues()

		for n, vv := range values {
			if err = WriteValue(w, vv); err != nil {
				return
			}

			if n < len(values)-1 {
				if _, err = w.WriteString(","); err != nil {
					return
				}
			}
		}
	case *structpb.Value_StructValue:
		err = WriteStruct(w, val.StructValue)
	default:
		err = ErrUnexpectedStructValue
	}
	return
}

// WriteStruct writes Struct value s to writer w. When s is nil, a
// nil error is returned. An error is returned only when the underlying
// writer returns an error. The struct fields are written in the sorted
// order of their names. A comma separates fields.
func WriteStruct(w io.StringWriter, s *structpb.Struct) (err error) {
	if s == nil {
		return
	}

	fields := s.GetFields()
	keys := maps.Keys(fields)
	sort.Strings(keys)

	for _, key := range keys {
		if _, err = w.WriteString(fmt.Sprintf("'%s:'", key)); err != nil {
			return
		}

		if err = WriteValue(w, fields[key]); err != nil {
			return
		}

		if _, err = w.WriteString(","); err != nil {
			return
		}
	}
	return
}

// WriteTuples writes the set of tuples to writer w in ascending sorted order.
// The intention of this function is to write the tuples as a unique string.
// Tuples are separated by commas, and when present, conditions are included
// in the tuple string representation. WriteTuples returns an error only when
// the underlying writer returns an error.
func WriteTuples(w io.StringWriter, tuples ...*openfgav1.TupleKey) (err error) {
	sortedTuples := make(tuple.TupleKeys, len(tuples))

	// copy tuples slice to avoid mutating the original slice during sorting.
	copy(sortedTuples, tuples)

	// sort tulpes for a deterministic write
	sort.Sort(sortedTuples)

	// prefix to avoid overlap with previous strings written
	_, err = w.WriteString("/")
	if err != nil {
		return
	}

	for n, tupleKey := range sortedTuples {
		_, err = w.WriteString(tupleKey.GetObject() + "#" + tupleKey.GetRelation())
		if err != nil {
			return
		}

		cond := tupleKey.GetCondition()
		if cond != nil {
			// " with " is separated by spaces as those are invalid in relation names
			// and we need to ensure this cache key is unique
			// resultant cache key format is "object:object_id#relation with {condition} {context}@user:user_id"
			_, err = w.WriteString(" with " + cond.GetName())
			if err != nil {
				return
			}

			// if the condition also has context, we need an additional separator
			// which cannot be present in condition names
			if cond.GetContext() != nil {
				_, err = w.WriteString(" ")
				if err != nil {
					return
				}
			}

			// now write context to hash. Is a noop if context is nil.
			if err = WriteStruct(w, cond.GetContext()); err != nil {
				return
			}
		}

		if _, err = w.WriteString("@" + tupleKey.GetUser()); err != nil {
			return
		}

		if n < len(tuples)-1 {
			if _, err = w.WriteString(","); err != nil {
				return
			}
		}
	}
	return
}
