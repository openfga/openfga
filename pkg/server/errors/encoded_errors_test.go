package errors

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
)

func TestEncodedError(t *testing.T) {
	type encodedTests struct {
		_name                  string
		errorCode              int32
		message                string
		expectedCode           int
		expectedCodeString     string
		expectedHTTPStatusCode int
	}
	var tests = []encodedTests{
		{
			_name:                  "aborted_error",
			errorCode:              int32(codes.Aborted),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusConflict,
			expectedCode:           int(codes.Aborted),
			expectedCodeString:     "Aborted",
		},
		{
			_name:                  "invalid_error",
			errorCode:              20,
			message:                "error message",
			expectedHTTPStatusCode: http.StatusInternalServerError,
			expectedCode:           20,
			expectedCodeString:     "20",
		},
		{
			_name:                  "auth_error:_invalid subject",
			errorCode:              int32(openfgav1.AuthErrorCode_auth_failed_invalid_subject),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusUnauthorized,
			expectedCode:           1001,
			expectedCodeString:     "auth_failed_invalid_subject",
		},
		{
			_name:                  "auth_error:_invalid_audience",
			errorCode:              int32(openfgav1.AuthErrorCode_auth_failed_invalid_audience),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusUnauthorized,
			expectedCode:           1002,
			expectedCodeString:     "auth_failed_invalid_audience",
		},
		{
			_name:                  "auth_error:_invalid issuer",
			errorCode:              int32(openfgav1.AuthErrorCode_auth_failed_invalid_issuer),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusUnauthorized,
			expectedCode:           1003,
			expectedCodeString:     "auth_failed_invalid_issuer",
		},
		{
			_name:                  "auth_error:_invalid_claims",
			errorCode:              int32(openfgav1.AuthErrorCode_invalid_claims),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusUnauthorized,
			expectedCode:           1004,
			expectedCodeString:     "invalid_claims",
		},
		{
			_name:                  "auth_error:_invalid_bearer_token",
			errorCode:              int32(openfgav1.AuthErrorCode_auth_failed_invalid_bearer_token),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusUnauthorized,
			expectedCode:           1005,
			expectedCodeString:     "auth_failed_invalid_bearer_token",
		},
		{
			_name:                  "auth_error:_missing_bearer_token",
			errorCode:              int32(openfgav1.AuthErrorCode_bearer_token_missing),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusUnauthorized,
			expectedCode:           1010,
			expectedCodeString:     "bearer_token_missing",
		},
		{
			_name:                  "auth_error:_unauthorized",
			errorCode:              int32(openfgav1.AuthErrorCode_unauthenticated),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusUnauthorized,
			expectedCode:           1500,
			expectedCodeString:     "unauthenticated",
		},
		{
			_name:                  "validation_error",
			errorCode:              int32(openfgav1.ErrorCode_validation_error),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusBadRequest,
			expectedCode:           2000,
			expectedCodeString:     "validation_error",
		},
		{
			_name:                  "throttle_error",
			errorCode:              int32(openfgav1.UnprocessableContentErrorCode_throttled_timeout_error),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusUnprocessableEntity,
			expectedCode:           3500,
			expectedCodeString:     "throttled_timeout_error",
		},
		{
			_name:                  "internal_error",
			errorCode:              int32(openfgav1.InternalErrorCode_internal_error),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusInternalServerError,
			expectedCode:           4000,
			expectedCodeString:     "internal_error",
		},
		{
			_name:                  "undefined_endpoint",
			errorCode:              int32(openfgav1.NotFoundErrorCode_undefined_endpoint),
			message:                "error message",
			expectedHTTPStatusCode: http.StatusNotFound,
			expectedCode:           5000,
			expectedCodeString:     "undefined_endpoint",
		},
	}
	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			actualError := NewEncodedError(test.errorCode, test.message)

			require.Equal(t, test.expectedHTTPStatusCode, actualError.HTTPStatusCode)
			require.Equal(t, test.expectedCodeString, actualError.Code())
			require.Equal(t, int32(test.expectedCode), actualError.CodeValue())
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
			_name:             "normal_code",
			status:            status.New(codes.Code(openfgav1.ErrorCode_validation_error), "other error"),
			expectedErrorCode: int32(openfgav1.ErrorCode_validation_error),
		},
		{
			_name:             "no_error",
			status:            status.New(codes.OK, "other error"),
			expectedErrorCode: int32(codes.OK),
		},
		{
			_name:             "cancelled",
			status:            status.New(codes.Canceled, "other error"),
			expectedErrorCode: int32(openfgav1.ErrorCode_cancelled),
		},
		{
			_name:             "unknown",
			status:            status.New(codes.Unknown, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_internal_error),
		},
		{
			_name:             "deadline_exceeded",
			status:            status.New(codes.DeadlineExceeded, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_deadline_exceeded),
		},
		{
			_name:             "not_found",
			status:            status.New(codes.NotFound, "other error"),
			expectedErrorCode: int32(openfgav1.NotFoundErrorCode_undefined_endpoint),
		},
		{
			_name:             "already_exceed",
			status:            status.New(codes.AlreadyExists, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_already_exists),
		},
		{
			_name:             "resource_exhausted",
			status:            status.New(codes.ResourceExhausted, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_resource_exhausted),
		},
		{
			_name:             "failed_precondition",
			status:            status.New(codes.FailedPrecondition, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_failed_precondition),
		},
		{
			_name:             "aborted",
			status:            status.New(codes.Aborted, "other error"),
			expectedErrorCode: int32(codes.Aborted),
		},
		{
			_name:             "out_of_range",
			status:            status.New(codes.OutOfRange, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_out_of_range),
		},
		{
			_name:             "unimplemented",
			status:            status.New(codes.OutOfRange, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_out_of_range),
		},
		{
			_name:             "internal",
			status:            status.New(codes.Internal, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_internal_error),
		},
		{
			_name:             "unavailable",
			status:            status.New(codes.Unavailable, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_unavailable),
		},
		{
			_name:             "unauthenticated",
			status:            status.New(codes.Unauthenticated, "other error"),
			expectedErrorCode: int32(openfgav1.AuthErrorCode_unauthenticated),
		},
		{
			_name:             "data_loss",
			status:            status.New(codes.DataLoss, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_data_loss),
		},
		{
			_name:             "undefined_error_number",
			status:            status.New(25, "other error"),
			expectedErrorCode: int32(openfgav1.InternalErrorCode_internal_error),
		},
		{
			_name:             "invalid_argument_-_unknown_format",
			status:            status.New(codes.InvalidArgument, "other error"),
			expectedErrorCode: int32(openfgav1.ErrorCode_validation_error),
		},
		{
			_name:             "invalid_argument_-_unknown_format_(2)",
			status:            status.New(codes.InvalidArgument, "no dot | foo :other error"),
			expectedErrorCode: int32(openfgav1.ErrorCode_validation_error),
		},
		{
			_name:             "invalid_argument_-_unknown_format_(3)",
			status:            status.New(codes.InvalidArgument, "| foo :other error"),
			expectedErrorCode: int32(openfgav1.ErrorCode_validation_error),
		},
		{
			_name:             "invalid_argument_-_unknown_type",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.UnknowObject: value must be absolute"),
			expectedErrorCode: int32(openfgav1.ErrorCode_validation_error),
		},
		{
			_name:             "invalid_argument_-_store_id",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.StoreId: value length must be less than 26 runes"),
			expectedErrorCode: int32(openfgav1.ErrorCode_store_id_invalid_length),
		},
		{
			_name:             "invalid_argument_-_issuer_url_other_error",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.IssuerUrl: other error"),
			expectedErrorCode: int32(openfgav1.ErrorCode_validation_error),
		},
		{
			_name:             "invalid_argument_-_Assertions",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Assertions: value must contain no more than 40 runes"),
			expectedErrorCode: int32(openfgav1.ErrorCode_assertions_too_many_items),
		},
		{
			_name:             "invalid_argument_-_AuthorizationModelId",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.AuthorizationModelId: value length must be at most 40 runes"),
			expectedErrorCode: int32(openfgav1.ErrorCode_authorization_model_id_too_long),
		},
		{
			_name:             "invalid_argument_-_Base",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Base: value is required"),
			expectedErrorCode: int32(openfgav1.ErrorCode_difference_base_missing_value),
		},
		{
			_name:             "invalid_argument_-_Id",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Id: value length must be at most 40 runes"),
			expectedErrorCode: int32(openfgav1.ErrorCode_id_too_long),
		},
		{
			_name:             "invalid_argument_-_Object_length",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Object: value length must be at most 256 bytes"),
			expectedErrorCode: int32(openfgav1.ErrorCode_object_too_long),
		},
		{
			_name:             "invalid_argument_-_PageSize",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.PageSize: value must be inside range 1 to 100"),
			expectedErrorCode: int32(openfgav1.ErrorCode_page_size_invalid),
		},
		{
			_name:             "invalid_argument_-_Params",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Params: value is required"),
			expectedErrorCode: int32(openfgav1.ErrorCode_param_missing_value),
		},
		{
			_name:             "invalid_argument_-_Relation",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Relation: value length must be at most 50 bytes"),
			expectedErrorCode: int32(openfgav1.ErrorCode_relation_too_long),
		},
		{
			_name:             "invalid_argument_-_Relations",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Relations: value must contain at least 1 pair"),
			expectedErrorCode: int32(openfgav1.ErrorCode_relations_too_few_items),
		},
		{
			_name:             "invalid_argument_-_Relations[abc]",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Relations[abc]: value length must be at most 50 bytes"),
			expectedErrorCode: int32(openfgav1.ErrorCode_relations_too_long),
		},
		{
			_name:             "invalid_argument_-_Relations[abc]",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Relations[abc]: value does not match regex pattern"),
			expectedErrorCode: int32(openfgav1.ErrorCode_relations_invalid_pattern),
		},
		{
			_name:             "invalid_argument_-_Subtract",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Subtract: value is required"),
			expectedErrorCode: int32(openfgav1.ErrorCode_subtract_base_missing_value),
		},
		{
			_name:             "invalid_argument_-_TupleKey",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.TupleKey: value is required"),
			expectedErrorCode: int32(openfgav1.ErrorCode_tuple_key_value_not_specified),
		},
		{
			_name:             "invalid_argument_-_TupleKeys",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.TupleKeys: value must contain between 1 to 10 items"),
			expectedErrorCode: int32(openfgav1.ErrorCode_tuple_keys_too_many_or_too_few_items),
		},
		{
			_name:             "invalid_argument_-_Type_length_at_least",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Type: value length must be at least 1"),
			expectedErrorCode: int32(openfgav1.ErrorCode_type_invalid_length),
		},
		{
			_name:             "invalid_argument_-_Type_length_at_most",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Type: value length must be at most 254 bytes"),
			expectedErrorCode: int32(openfgav1.ErrorCode_type_invalid_length),
		},
		{
			_name:             "invalid_argument_-_Type_regex",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.Type: value does not match regex pattern \"^[^:#]*$\""),
			expectedErrorCode: int32(openfgav1.ErrorCode_type_invalid_pattern),
		},
		{
			_name:             "invalid_argument_-_TypeDefinitions",
			status:            status.New(codes.InvalidArgument, "invalid WriteTokenIssuersRequest.Params: embedded message failed validation | caused by: invalid WriteTokenIssuersRequestParams.TypeDefinitions: value must contain at least 1 item"),
			expectedErrorCode: int32(openfgav1.ErrorCode_type_definitions_too_few_items),
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			code := ConvertToEncodedErrorCode(test.status)

			require.Equal(t, test.expectedErrorCode, code)
		})
	}
}

func TestSanitizeErrorMessage(t *testing.T) {
	got := sanitizedMessage(`proto: (line 1:2): unknown field "foo"`) // uses a whitespace rune of U+00a0 (see https://pkg.go.dev/unicode#IsSpace)
	expected := `(line 1:2): unknown field "foo"`

	require.Equal(t, expected, got)
}
