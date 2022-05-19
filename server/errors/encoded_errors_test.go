package errors

import (
	"net/http"
	"testing"

	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestEncodedError(t *testing.T) {
	type encodedTests struct {
		_name                  string
		errorCode              int32
		message                string
		expectedCode           int
		expectedCodeString     string
		expectedHTTPStatusCode int
		isValidEncodedError    bool
	}
	var tests = []encodedTests{
		{
			_name:                  "invalid error",
			errorCode:              20,
			message:                "error message",
			expectedHTTPStatusCode: http.StatusInternalServerError,
			expectedCode:           20,
			expectedCodeString:     "20",
			isValidEncodedError:    false,
		},
		{
			_name:                  "validation error",
			errorCode:              int32(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusBadRequest,
			expectedCode:           2000,
			expectedCodeString:     "ERROR_CODE_VALIDATION_ERROR",
			isValidEncodedError:    true,
		},
		{
			_name:                  "internal error",
			errorCode:              int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_INTERNAL_ERROR),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusInternalServerError,
			expectedCode:           4000,
			expectedCodeString:     "INTERNAL_ERROR_CODE_INTERNAL_ERROR",
			isValidEncodedError:    true,
		},
		{
			_name:                  "undefined endpoint",
			errorCode:              int32(openfgapb.NotFoundErrorCode_NOT_FOUND_ERROR_CODE_UNDEFINED_ENDPOINT),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusNotFound,
			expectedCode:           5000,
			expectedCodeString:     "NOT_FOUND_ERROR_CODE_UNDEFINED_ENDPOINT",
			isValidEncodedError:    true,
		},
	}
	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			actualError := NewEncodedError(test.errorCode, test.message)
			if actualError.HTTPStatusCode != test.expectedHTTPStatusCode {
				t.Errorf("[%s]: http status code expect %d: actual %d", test._name, test.expectedHTTPStatusCode, actualError.HTTPStatusCode)
			}

			if actualError.Code() != test.expectedCodeString {
				t.Errorf("[%s]: code string expect %s: actual %s", test._name, test.expectedCodeString, actualError.Code())
			}

			if actualError.CodeValue() != int32(test.expectedCode) {
				t.Errorf("[%s]: code expect %d: actual %d", test._name, test.expectedCode, actualError.CodeValue())
			}

			if IsValidEncodedError(actualError.CodeValue()) != test.isValidEncodedError {
				t.Errorf("[%s]: expect is valid error %v: actual %v", test._name, test.isValidEncodedError, IsValidEncodedError(actualError.CodeValue()))
			}
		})
	}
}

func TestConvertToEncodedErrorCode(t *testing.T) {
	type encodedTests struct {
		_name             string
		status            *status.Status
		expectedErrorCode int32
	}
	var tests = []encodedTests{
		{
			_name:             "normal code",
			status:            status.New(codes.Code(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR), "other error"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR),
		},
		{
			_name:             "no error",
			status:            status.New(codes.OK, "other error"),
			expectedErrorCode: int32(codes.OK),
		},
		{
			_name:             "cancelled",
			status:            status.New(codes.Canceled, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_CANCELLED),
		},
		{
			_name:             "unknown",
			status:            status.New(codes.Unknown, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_INTERNAL_ERROR),
		},
		{
			_name:             "deadline exceeded",
			status:            status.New(codes.DeadlineExceeded, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_DEADLINE_EXCEEDED),
		},
		{
			_name:             "not found",
			status:            status.New(codes.NotFound, "other error"),
			expectedErrorCode: int32(openfgapb.NotFoundErrorCode_NOT_FOUND_ERROR_CODE_UNDEFINED_ENDPOINT),
		},
		{
			_name:             "already exceed",
			status:            status.New(codes.AlreadyExists, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_ALREADY_EXISTS),
		},
		{
			_name:             "resource exhausted",
			status:            status.New(codes.ResourceExhausted, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_RESOURCE_EXHAUSTED),
		},
		{
			_name:             "failed precondition",
			status:            status.New(codes.FailedPrecondition, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_FAILED_PRECONDITION),
		},
		{
			_name:             "aborted",
			status:            status.New(codes.Aborted, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_ABORTED),
		},
		{
			_name:             "out of range",
			status:            status.New(codes.OutOfRange, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_OUT_OF_RANGE),
		},
		{
			_name:             "unimplemented",
			status:            status.New(codes.OutOfRange, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_OUT_OF_RANGE),
		},
		{
			_name:             "internal",
			status:            status.New(codes.Internal, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_INTERNAL_ERROR),
		},
		{
			_name:             "unavailable",
			status:            status.New(codes.Unavailable, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_UNAVAILABLE),
		},
		{
			_name:             "data loss",
			status:            status.New(codes.DataLoss, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_DATA_LOSS),
		},
		{
			_name:             "undefined error number",
			status:            status.New(25, "other error"),
			expectedErrorCode: int32(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_INTERNAL_ERROR),
		},
		{
			_name:             "invalid argument - unknown format",
			status:            status.New(codes.InvalidArgument, "other error"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR),
		},
		{
			_name:             "invalid argument - unknown format (2)",
			status:            status.New(codes.InvalidArgument, "no dot | foo :other error"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR),
		},
		{
			_name:             "invalid argument - unknown format (3)",
			status:            status.New(codes.InvalidArgument, "| foo :other error"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR),
		},
		{
			_name:             "invalid argument - unknown type",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.UnknowObject: value must be absolute"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR),
		},
		{
			_name:             "invalid argument - store id",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.StoreId: value length must be less than 26 runes"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_STORE_ID_INVALID_LENGTH),
		},
		{
			_name:             "invalid argument - issuer url other error",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.IssuerUrl: other error"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_VALIDATION_ERROR),
		},
		{
			_name:             "invalid argument - Assertions",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Assertions: value must contain no more than 40 runes"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_ASSERTIONS_TOO_MANY_ITEMS),
		},
		{
			_name:             "invalid argument - AuthorizationModelId",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.AuthorizationModelId: value length must be at most 40 runes"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_AUTHORIZATION_MODEL_ID_TOO_LONG),
		},
		{
			_name:             "invalid argument - Base",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Base: value is required"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_DIFFERENCE_BASE_MISSING_VALUE),
		},
		{
			_name:             "invalid argument - Id",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Id: value length must be at most 40 runes"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_ID_TOO_LONG),
		},
		{
			_name:             "invalid argument - Object length",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Object: value length must be at most 256 bytes"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_OBJECT_TOO_LONG),
		},
		{
			_name:             "invalid argument - Object invalid pattern",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Object: value does not match regex pattern"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_OBJECT_INVALID_PATTERN),
		},
		{
			_name:             "invalid argument - PageSize",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.PageSize: value must be inside range 1 to 100"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_PAGE_SIZE_INVALID),
		},
		{
			_name:             "invalid argument - Params",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Params: value is required"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_PARAM_MISSING_VALUE),
		},
		{
			_name:             "invalid argument - Relation",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Relation: value length must be at most 50 bytes"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_RELATION_TOO_LONG),
		},
		{
			_name:             "invalid argument - Relations",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Relations: value must contain at least 1 pair"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_RELATIONS_TOO_FEW_ITEMS),
		},
		{
			_name:             "invalid argument - Relations[abc]",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Relations[abc]: value length must be at most 50 bytes"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_RELATIONS_TOO_LONG),
		},
		{
			_name:             "invalid argument - Relations[abc]",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Relations[abc]: value does not match regex pattern"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_RELATIONS_INVALID_PATTERN),
		},
		{
			_name:             "invalid argument - Subtract",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Subtract: value is required"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_SUBTRACT_BASE_MISSING_VALUE),
		},
		{
			_name:             "invalid argument - TupleKey",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.TupleKey: value is required"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_TUPLE_KEY_VALUE_NOT_SPECIFIED),
		},
		{
			_name:             "invalid argument - TupleKeys",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.TupleKeys: value must contain between 1 to 10 items"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_TUPLE_KEYS_TOO_MANY_OR_TOO_FEW_ITEMS),
		},
		{
			_name:             "invalid argument - Type length at least",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Type: value length must be at least 1"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_TYPE_INVALID_LENGTH),
		},
		{
			_name:             "invalid argument - Type length at most",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Type: value length must be at most 254 bytes"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_TYPE_INVALID_LENGTH),
		},
		{
			_name:             "invalid argument - Type regex",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Type: value does not match regex pattern \"^[^:#]*$\""),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_TYPE_INVALID_PATTERN),
		},
		{
			_name:             "invalid argument - TypeDefinitions",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.TypeDefinitions: value must contain at least 1 item"),
			expectedErrorCode: int32(openfgapb.ErrorCode_ERROR_CODE_TYPE_DEFINITIONS_TOO_FEW_ITEMS),
		},
	}
Tests:

	for _, test := range tests {
		code := ConvertToEncodedErrorCode(test.status)
		if code != test.expectedErrorCode {
			t.Errorf("[%s]: Expect error code %d actual %d", test._name, test.expectedErrorCode, code)
			continue Tests
		}
	}

}

func TestSanitizeErrorMessage(t *testing.T) {

	got := sanitizedMessage(`proto: (line 1:2): unknown field "foo"`) // uses a whitespace rune of U+00a0 (see https://pkg.go.dev/unicode#IsSpace)
	expected := `(line 1:2): unknown field "foo"`
	if got != expected {
		t.Errorf("expected '%s', but got '%s'", expected, got)
	}
}
