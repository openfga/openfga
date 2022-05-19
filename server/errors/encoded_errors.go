package errors

import (
	"net/http"
	"regexp"
	"strings"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const cFirstValidationErrorCode int32 = 2000
const cFirstInternalErrorCode int32 = 4000
const cFirstUnknownEndpointErrorCode int32 = 5000

type ErrorResponse struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	codeInt int32
}

// EncodedError allows customized error with code in string and specified http status field
type EncodedError struct {
	HTTPStatusCode int
	ActualError    ErrorResponse
}

// Error returns the encoded message
func (e *EncodedError) Error() string {
	return e.ActualError.Message
}

// CodeValue returns the encoded code in integer
func (e *EncodedError) CodeValue() int32 {
	return e.ActualError.codeInt
}

// HTTPStatus returns the HTTP Status code
func (e *EncodedError) HTTPStatus() int {
	return e.HTTPStatusCode
}

// Code returns the encoded code in string
func (e *EncodedError) Code() string {
	return e.ActualError.Code
}

func sanitizedMessage(message string) string {
	parsedMessages := strings.Split(message, "| caused by:")
	lastMessage := parsedMessages[len(parsedMessages)-1]
	lastMessage = strings.TrimSpace(lastMessage)

	sanitizedErrorMessage := regexp.MustCompile(`unexpected EOF`).ReplaceAllString(lastMessage, "malformed JSON")

	sanitizedErrorMessage = regexp.MustCompile(`rpc error: code = [a-zA-Z0-9\(\)]* desc = `).ReplaceAllString(sanitizedErrorMessage, "")
	return strings.TrimSpace(strings.TrimPrefix(sanitizedErrorMessage, "proto:"))
}

// NewEncodedError returns the encoded error with the correct http status code etc.
func NewEncodedError(errorCode int32, message string) EncodedError {

	if !IsValidEncodedError(errorCode) {
		return EncodedError{
			HTTPStatusCode: http.StatusInternalServerError,
			ActualError: ErrorResponse{
				Code:    openfgapb.InternalErrorCode(errorCode).String(),
				Message: sanitizedMessage(message),
				codeInt: errorCode,
			},
		}
	}

	var httpStatusCode int
	var code string
	if errorCode >= cFirstValidationErrorCode && errorCode < cFirstInternalErrorCode {
		httpStatusCode = http.StatusBadRequest
		code = openfgapb.ErrorCode(errorCode).String()
	} else if errorCode >= cFirstInternalErrorCode && errorCode < cFirstUnknownEndpointErrorCode {
		httpStatusCode = http.StatusInternalServerError
		code = openfgapb.InternalErrorCode(errorCode).String()
	} else if errorCode >= cFirstUnknownEndpointErrorCode && errorCode < cFirstUnknownEndpointErrorCode {
		httpStatusCode = http.StatusInternalServerError
		code = openfgapb.InternalErrorCode(errorCode).String()
	} else {
		httpStatusCode = http.StatusNotFound
		code = openfgapb.NotFoundErrorCode(errorCode).String()
	}
	return EncodedError{
		HTTPStatusCode: httpStatusCode,
		ActualError: ErrorResponse{
			Code:    code,
			Message: sanitizedMessage(message),
			codeInt: errorCode,
		},
	}
}

// IsValidEncodedError returns whether the error code is a valid encoded error
func IsValidEncodedError(errorCode int32) bool {
	return errorCode >= cFirstValidationErrorCode
}

func getCustomizedErrorCode(field string, reason string) int32 {
	switch field {
	case "Assertions":
		if strings.HasPrefix(reason, "value must contain no more than") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_ASSERTIONS_TOO_MANY_ITEMS)
		}
	case "AuthorizationModelId":
		if strings.HasPrefix(reason, "value length must be at most") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_AUTHORIZATION_MODEL_ID_TOO_LONG)
		}
	case "Base":
		if strings.HasPrefix(reason, "value is required") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_DIFFERENCE_BASE_MISSING_VALUE)
		}
	case "Id":
		if strings.HasPrefix(reason, "value length must be at most") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_ID_TOO_LONG)
		}
	case "Object":
		if strings.HasPrefix(reason, "value length must be at most") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_OBJECT_TOO_LONG)
		}
		if strings.HasPrefix(reason, "value does not match regex pattern") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_OBJECT_INVALID_PATTERN)
		}
	case "PageSize":
		if strings.HasPrefix(reason, "value must be inside range") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_PAGE_SIZE_INVALID)
		}
	case "Params":
		if strings.HasPrefix(reason, "value is required") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_PARAM_MISSING_VALUE)
		}
	case "Relation":
		if strings.HasPrefix(reason, "value length must be at most") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_RELATION_TOO_LONG)
		}
	case "Relations":
		if strings.HasPrefix(reason, "value must contain at least") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_RELATIONS_TOO_FEW_ITEMS)
		}
	case "Subtract":
		if strings.HasPrefix(reason, "value is required") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_SUBTRACT_BASE_MISSING_VALUE)
		}
	case "StoreId":
		if strings.HasPrefix(reason, "value length must be") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_STORE_ID_INVALID_LENGTH)
		}
	case "TupleKey":
		if strings.HasPrefix(reason, "value is required") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_TUPLE_KEY_VALUE_NOT_SPECIFIED)
		}
	case "TupleKeys":
		if strings.HasPrefix(reason, "value must contain between") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_TUPLE_KEYS_TOO_MANY_OR_TOO_FEW_ITEMS)
		}
	case "Type":
		if strings.HasPrefix(reason, "value length must be at") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_TYPE_INVALID_LENGTH)
		}
		if strings.HasPrefix(reason, "value does not match regex pattern") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_TYPE_INVALID_PATTERN)
		}
	case "TypeDefinitions":
		if strings.HasPrefix(reason, "value must contain at least") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_TYPE_DEFINITIONS_TOO_FEW_ITEMS)
		}

	}
	// We will need to check for regex pattern
	if strings.HasPrefix(field, "Relations[") {
		if strings.HasPrefix(reason, "value length must be at most") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_RELATIONS_TOO_LONG)
		}
		if strings.HasPrefix(reason, "value does not match regex pattern") {
			return int32(openfgapb.ErrorCode_ERROR_CODE_RELATIONS_INVALID_PATTERN)
		}
	}

	// When we get to here, this is not a type or message that we know well.
	// We needs to return the generic error type
	return int32(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR)

}

func ConvertToEncodedErrorCode(statusError *status.Status) int32 {
	code := int32(statusError.Code())
	if code >= cFirstValidationErrorCode {
		return code
	}

	switch statusError.Code() {
	case codes.OK:
		return int32(codes.OK)
	case codes.Canceled:
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_CANCELLED)
	case codes.Unknown:
		// we will return InternalError as our implementation of
		// InternalError does not have a status code - which will result
		// in unknown error
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_INTERNAL_ERROR)
	case codes.DeadlineExceeded:
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_DEADLINE_EXCEEDED)
	case codes.NotFound:
		return int32(openfgapb.NotFoundErrorCode_NOT_FOUND_ERROR_CODE_UNDEFINED_ENDPOINT)
	case codes.AlreadyExists:
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_ALREADY_EXISTS)
	case codes.ResourceExhausted:
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_RESOURCE_EXHAUSTED)
	case codes.FailedPrecondition:
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_FAILED_PRECONDITION)
	case codes.Aborted:
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_ABORTED)
	case codes.OutOfRange:
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_OUT_OF_RANGE)
	case codes.Unimplemented:
		return int32(openfgapb.NotFoundErrorCode_NOT_FOUND_ERROR_CODE_UNIMPLEMENTED)
	case codes.Internal:
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_INTERNAL_ERROR)
	case codes.Unavailable:
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_UNAVAILABLE)
	case codes.DataLoss:
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_DATA_LOSS)
	case codes.InvalidArgument:
		break
	default:
		// Unknown code - internal error
		return int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_INTERNAL_ERROR)
	}
	// When we get to here, the cause is InvalidArgument (likely flagged by the framework's validator).
	// We will try to find out the actual cause if possible. Otherwise, the default response will
	// be openfgapb.ErrorCode_validation_error

	lastMessage := sanitizedMessage(statusError.Message())
	lastMessageSplitted := strings.SplitN(lastMessage, ": ", 2)
	if len(lastMessageSplitted) < 2 {
		// I don't know how to process this message.
		// The safest thing is to return the generic validation error
		return int32(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR)
	}
	errorObjectSplitted := strings.Split(lastMessageSplitted[0], ".")
	if len(errorObjectSplitted) != 2 {
		// I don't know is the type.
		// Return generic error type
		return int32(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR)
	}
	return getCustomizedErrorCode(errorObjectSplitted[1], lastMessageSplitted[1])
}
