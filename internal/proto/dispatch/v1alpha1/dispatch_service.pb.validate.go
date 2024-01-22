// Code generated by protoc-gen-validate. DO NOT EDIT.
// source: dispatch/v1alpha1/dispatch_service.proto

package dispatchv1alpha1

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"net/mail"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/types/known/anypb"
)

// ensure the imports are used
var (
	_ = bytes.MinRead
	_ = errors.New("")
	_ = fmt.Print
	_ = utf8.UTFMax
	_ = (*regexp.Regexp)(nil)
	_ = (*strings.Reader)(nil)
	_ = net.IPv4len
	_ = time.Duration(0)
	_ = (*url.URL)(nil)
	_ = (*mail.Address)(nil)
	_ = anypb.Any{}
	_ = sort.Sort
)

// Validate checks the field values on ReverseExpandResolutionMetadata with the
// rules defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *ReverseExpandResolutionMetadata) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on ReverseExpandResolutionMetadata with
// the rules defined in the proto definition for this message. If any rules
// are violated, the result is a list of violation errors wrapped in
// ReverseExpandResolutionMetadataMultiError, or nil if none found.
func (m *ReverseExpandResolutionMetadata) ValidateAll() error {
	return m.validate(true)
}

func (m *ReverseExpandResolutionMetadata) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for ResolutionDepth

	// no validation rules for DispatchCount

	// no validation rules for QueryCount

	if len(errors) > 0 {
		return ReverseExpandResolutionMetadataMultiError(errors)
	}

	return nil
}

// ReverseExpandResolutionMetadataMultiError is an error wrapping multiple
// validation errors returned by ReverseExpandResolutionMetadata.ValidateAll()
// if the designated constraints aren't met.
type ReverseExpandResolutionMetadataMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m ReverseExpandResolutionMetadataMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m ReverseExpandResolutionMetadataMultiError) AllErrors() []error { return m }

// ReverseExpandResolutionMetadataValidationError is the validation error
// returned by ReverseExpandResolutionMetadata.Validate if the designated
// constraints aren't met.
type ReverseExpandResolutionMetadataValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e ReverseExpandResolutionMetadataValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e ReverseExpandResolutionMetadataValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e ReverseExpandResolutionMetadataValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e ReverseExpandResolutionMetadataValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e ReverseExpandResolutionMetadataValidationError) ErrorName() string {
	return "ReverseExpandResolutionMetadataValidationError"
}

// Error satisfies the builtin error interface
func (e ReverseExpandResolutionMetadataValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sReverseExpandResolutionMetadata.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = ReverseExpandResolutionMetadataValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = ReverseExpandResolutionMetadataValidationError{}

// Validate checks the field values on UserRef with the rules defined in the
// proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *UserRef) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on UserRef with the rules defined in the
// proto definition for this message. If any rules are violated, the result is
// a list of violation errors wrapped in UserRefMultiError, or nil if none found.
func (m *UserRef) ValidateAll() error {
	return m.validate(true)
}

func (m *UserRef) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	switch v := m.UserRef.(type) {
	case *UserRef_Object:
		if v == nil {
			err := UserRefValidationError{
				field:  "UserRef",
				reason: "oneof value cannot be a typed-nil",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

		if all {
			switch v := interface{}(m.GetObject()).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, UserRefValidationError{
						field:  "Object",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, UserRefValidationError{
						field:  "Object",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(m.GetObject()).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return UserRefValidationError{
					field:  "Object",
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	case *UserRef_TypedWildcard:
		if v == nil {
			err := UserRefValidationError{
				field:  "UserRef",
				reason: "oneof value cannot be a typed-nil",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

		if all {
			switch v := interface{}(m.GetTypedWildcard()).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, UserRefValidationError{
						field:  "TypedWildcard",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, UserRefValidationError{
						field:  "TypedWildcard",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(m.GetTypedWildcard()).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return UserRefValidationError{
					field:  "TypedWildcard",
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	case *UserRef_Userset:
		if v == nil {
			err := UserRefValidationError{
				field:  "UserRef",
				reason: "oneof value cannot be a typed-nil",
			}
			if !all {
				return err
			}
			errors = append(errors, err)
		}

		if all {
			switch v := interface{}(m.GetUserset()).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, UserRefValidationError{
						field:  "Userset",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, UserRefValidationError{
						field:  "Userset",
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(m.GetUserset()).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return UserRefValidationError{
					field:  "Userset",
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	default:
		_ = v // ensures v is used
	}

	if len(errors) > 0 {
		return UserRefMultiError(errors)
	}

	return nil
}

// UserRefMultiError is an error wrapping multiple validation errors returned
// by UserRef.ValidateAll() if the designated constraints aren't met.
type UserRefMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m UserRefMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m UserRefMultiError) AllErrors() []error { return m }

// UserRefValidationError is the validation error returned by UserRef.Validate
// if the designated constraints aren't met.
type UserRefValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e UserRefValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e UserRefValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e UserRefValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e UserRefValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e UserRefValidationError) ErrorName() string { return "UserRefValidationError" }

// Error satisfies the builtin error interface
func (e UserRefValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sUserRef.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = UserRefValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = UserRefValidationError{}

// Validate checks the field values on Userset with the rules defined in the
// proto definition for this message. If any rules are violated, the first
// error encountered is returned, or nil if there are no violations.
func (m *Userset) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on Userset with the rules defined in the
// proto definition for this message. If any rules are violated, the result is
// a list of violation errors wrapped in UsersetMultiError, or nil if none found.
func (m *Userset) ValidateAll() error {
	return m.validate(true)
}

func (m *Userset) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if all {
		switch v := interface{}(m.GetObject()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, UsersetValidationError{
					field:  "Object",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, UsersetValidationError{
					field:  "Object",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetObject()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return UsersetValidationError{
				field:  "Object",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	// no validation rules for Relation

	if len(errors) > 0 {
		return UsersetMultiError(errors)
	}

	return nil
}

// UsersetMultiError is an error wrapping multiple validation errors returned
// by Userset.ValidateAll() if the designated constraints aren't met.
type UsersetMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m UsersetMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m UsersetMultiError) AllErrors() []error { return m }

// UsersetValidationError is the validation error returned by Userset.Validate
// if the designated constraints aren't met.
type UsersetValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e UsersetValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e UsersetValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e UsersetValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e UsersetValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e UsersetValidationError) ErrorName() string { return "UsersetValidationError" }

// Error satisfies the builtin error interface
func (e UsersetValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sUserset.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = UsersetValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = UsersetValidationError{}

// Validate checks the field values on TypedPublicWildcard with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *TypedPublicWildcard) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on TypedPublicWildcard with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// TypedPublicWildcardMultiError, or nil if none found.
func (m *TypedPublicWildcard) ValidateAll() error {
	return m.validate(true)
}

func (m *TypedPublicWildcard) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for Type

	if len(errors) > 0 {
		return TypedPublicWildcardMultiError(errors)
	}

	return nil
}

// TypedPublicWildcardMultiError is an error wrapping multiple validation
// errors returned by TypedPublicWildcard.ValidateAll() if the designated
// constraints aren't met.
type TypedPublicWildcardMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m TypedPublicWildcardMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m TypedPublicWildcardMultiError) AllErrors() []error { return m }

// TypedPublicWildcardValidationError is the validation error returned by
// TypedPublicWildcard.Validate if the designated constraints aren't met.
type TypedPublicWildcardValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e TypedPublicWildcardValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e TypedPublicWildcardValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e TypedPublicWildcardValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e TypedPublicWildcardValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e TypedPublicWildcardValidationError) ErrorName() string {
	return "TypedPublicWildcardValidationError"
}

// Error satisfies the builtin error interface
func (e TypedPublicWildcardValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sTypedPublicWildcard.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = TypedPublicWildcardValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = TypedPublicWildcardValidationError{}

// Validate checks the field values on ReverseExpandRequest with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *ReverseExpandRequest) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on ReverseExpandRequest with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// ReverseExpandRequestMultiError, or nil if none found.
func (m *ReverseExpandRequest) ValidateAll() error {
	return m.validate(true)
}

func (m *ReverseExpandRequest) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	// no validation rules for StoreId

	if all {
		switch v := interface{}(m.GetModel()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ReverseExpandRequestValidationError{
					field:  "Model",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ReverseExpandRequestValidationError{
					field:  "Model",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetModel()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ReverseExpandRequestValidationError{
				field:  "Model",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	// no validation rules for TargetObjectType

	// no validation rules for TargetRelation

	if all {
		switch v := interface{}(m.GetSourceUser()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ReverseExpandRequestValidationError{
					field:  "SourceUser",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ReverseExpandRequestValidationError{
					field:  "SourceUser",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetSourceUser()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ReverseExpandRequestValidationError{
				field:  "SourceUser",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	for idx, item := range m.GetContextualTuples() {
		_, _ = idx, item

		if all {
			switch v := interface{}(item).(type) {
			case interface{ ValidateAll() error }:
				if err := v.ValidateAll(); err != nil {
					errors = append(errors, ReverseExpandRequestValidationError{
						field:  fmt.Sprintf("ContextualTuples[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			case interface{ Validate() error }:
				if err := v.Validate(); err != nil {
					errors = append(errors, ReverseExpandRequestValidationError{
						field:  fmt.Sprintf("ContextualTuples[%v]", idx),
						reason: "embedded message failed validation",
						cause:  err,
					})
				}
			}
		} else if v, ok := interface{}(item).(interface{ Validate() error }); ok {
			if err := v.Validate(); err != nil {
				return ReverseExpandRequestValidationError{
					field:  fmt.Sprintf("ContextualTuples[%v]", idx),
					reason: "embedded message failed validation",
					cause:  err,
				}
			}
		}

	}

	if all {
		switch v := interface{}(m.GetContext()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ReverseExpandRequestValidationError{
					field:  "Context",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ReverseExpandRequestValidationError{
					field:  "Context",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetContext()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ReverseExpandRequestValidationError{
				field:  "Context",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if all {
		switch v := interface{}(m.GetResolutionMetadata()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ReverseExpandRequestValidationError{
					field:  "ResolutionMetadata",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ReverseExpandRequestValidationError{
					field:  "ResolutionMetadata",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetResolutionMetadata()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ReverseExpandRequestValidationError{
				field:  "ResolutionMetadata",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	if len(errors) > 0 {
		return ReverseExpandRequestMultiError(errors)
	}

	return nil
}

// ReverseExpandRequestMultiError is an error wrapping multiple validation
// errors returned by ReverseExpandRequest.ValidateAll() if the designated
// constraints aren't met.
type ReverseExpandRequestMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m ReverseExpandRequestMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m ReverseExpandRequestMultiError) AllErrors() []error { return m }

// ReverseExpandRequestValidationError is the validation error returned by
// ReverseExpandRequest.Validate if the designated constraints aren't met.
type ReverseExpandRequestValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e ReverseExpandRequestValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e ReverseExpandRequestValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e ReverseExpandRequestValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e ReverseExpandRequestValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e ReverseExpandRequestValidationError) ErrorName() string {
	return "ReverseExpandRequestValidationError"
}

// Error satisfies the builtin error interface
func (e ReverseExpandRequestValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sReverseExpandRequest.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = ReverseExpandRequestValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = ReverseExpandRequestValidationError{}

// Validate checks the field values on ReverseExpandResponse with the rules
// defined in the proto definition for this message. If any rules are
// violated, the first error encountered is returned, or nil if there are no violations.
func (m *ReverseExpandResponse) Validate() error {
	return m.validate(false)
}

// ValidateAll checks the field values on ReverseExpandResponse with the rules
// defined in the proto definition for this message. If any rules are
// violated, the result is a list of violation errors wrapped in
// ReverseExpandResponseMultiError, or nil if none found.
func (m *ReverseExpandResponse) ValidateAll() error {
	return m.validate(true)
}

func (m *ReverseExpandResponse) validate(all bool) error {
	if m == nil {
		return nil
	}

	var errors []error

	if all {
		switch v := interface{}(m.GetObject()).(type) {
		case interface{ ValidateAll() error }:
			if err := v.ValidateAll(); err != nil {
				errors = append(errors, ReverseExpandResponseValidationError{
					field:  "Object",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		case interface{ Validate() error }:
			if err := v.Validate(); err != nil {
				errors = append(errors, ReverseExpandResponseValidationError{
					field:  "Object",
					reason: "embedded message failed validation",
					cause:  err,
				})
			}
		}
	} else if v, ok := interface{}(m.GetObject()).(interface{ Validate() error }); ok {
		if err := v.Validate(); err != nil {
			return ReverseExpandResponseValidationError{
				field:  "Object",
				reason: "embedded message failed validation",
				cause:  err,
			}
		}
	}

	// no validation rules for ConditionalResultStatus

	if len(errors) > 0 {
		return ReverseExpandResponseMultiError(errors)
	}

	return nil
}

// ReverseExpandResponseMultiError is an error wrapping multiple validation
// errors returned by ReverseExpandResponse.ValidateAll() if the designated
// constraints aren't met.
type ReverseExpandResponseMultiError []error

// Error returns a concatenation of all the error messages it wraps.
func (m ReverseExpandResponseMultiError) Error() string {
	var msgs []string
	for _, err := range m {
		msgs = append(msgs, err.Error())
	}
	return strings.Join(msgs, "; ")
}

// AllErrors returns a list of validation violation errors.
func (m ReverseExpandResponseMultiError) AllErrors() []error { return m }

// ReverseExpandResponseValidationError is the validation error returned by
// ReverseExpandResponse.Validate if the designated constraints aren't met.
type ReverseExpandResponseValidationError struct {
	field  string
	reason string
	cause  error
	key    bool
}

// Field function returns field value.
func (e ReverseExpandResponseValidationError) Field() string { return e.field }

// Reason function returns reason value.
func (e ReverseExpandResponseValidationError) Reason() string { return e.reason }

// Cause function returns cause value.
func (e ReverseExpandResponseValidationError) Cause() error { return e.cause }

// Key function returns key value.
func (e ReverseExpandResponseValidationError) Key() bool { return e.key }

// ErrorName returns error name.
func (e ReverseExpandResponseValidationError) ErrorName() string {
	return "ReverseExpandResponseValidationError"
}

// Error satisfies the builtin error interface
func (e ReverseExpandResponseValidationError) Error() string {
	cause := ""
	if e.cause != nil {
		cause = fmt.Sprintf(" | caused by: %v", e.cause)
	}

	key := ""
	if e.key {
		key = "key for "
	}

	return fmt.Sprintf(
		"invalid %sReverseExpandResponse.%s: %s%s",
		key,
		e.field,
		e.reason,
		cause)
}

var _ error = ReverseExpandResponseValidationError{}

var _ interface {
	Field() string
	Reason() string
	Key() bool
	Cause() error
	ErrorName() string
} = ReverseExpandResponseValidationError{}
