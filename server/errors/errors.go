package errors

import (
	"fmt"

	"github.com/go-errors/errors"
	openfgaerrors "github.com/openfga/openfga/pkg/errors"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/storage"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const InternalServerErrorMsg = "Internal Server Error"

var (
	// AuthorizationModelResolutionTooComplex is used to avoid stack overflows
	AuthorizationModelResolutionTooComplex     = status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_AUTHORIZATION_MODEL_RESOLUTION_TOO_COMPLEX), "Authorization Model resolution required too many rewrite rules to be resolved. Check your authorization model for infinite recursion or too much nesting")
	InvalidWriteInput                          = status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_INVALID_WRITE_INPUT), "Invalid input. Make sure you provide at least one write, or at least one delete")
	CannotAllowDuplicateTypesInOneRequest      = status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_CANNOT_ALLOW_DUPLICATE_TYPES_IN_ONE_REQUEST), "Cannot allow duplicate types in one request")
	CannotAllowMultipleReferencesToOneRelation = status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_CANNOT_ALLOW_MULTIPLE_REFERENCES_TO_ONE_RELATION), "Invalid input. Please don't use the relation to define itself")
	InvalidContinuationToken                   = status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_INVALID_CONTINUATION_TOKEN), "Invalid continuation token")
	InvalidTupleSet                            = status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_INVALID_TUPLE_SET), "Invalid TupleSet. Make sure you provide a type, and either an objectId or a userSet")
	InvalidCheckInput                          = status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_INVALID_CHECK_INPUT), "Invalid input. Make sure you provide a user, object and relation")
	InvalidExpandInput                         = status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_INVALID_EXPAND_INPUT), "Invalid input. Make sure you provide an object and a relation")
	UnsupportedUserSet                         = status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_UNSUPPORTED_USER_SET), "Userset is not supported (right now)")
	StoreIDNotFound                            = status.Error(codes.Code(openfgapb.NotFoundErrorCode_NOT_FOUND_ERROR_CODE_STORE_ID_NOT_FOUND), "Store ID not found")
	MismatchObjectType                         = status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_QUERY_STRING_TYPE_CONTINUATION_TOKEN_MISMATCH), "The type in the querystring and the continuation token don't match")
	RequestCancelled                           = status.Error(codes.Code(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_CANCELLED), "Request Cancelled")
)

type InternalError struct {
	public   error
	internal error
}

func (e InternalError) Error() string {
	return e.public.Error()
}

func (e InternalError) InternalError() string {
	return e.internal.Error()
}

func (e InternalError) Internal() error {
	return e.internal
}

func NewInternalError(public string, internal error) InternalError {
	if public == "" {
		public = InternalServerErrorMsg
	}

	return InternalError{
		public:   status.Error(codes.Code(openfgapb.InternalErrorCode_INTERNAL_ERROR_CODE_INTERNAL_ERROR), public),
		internal: openfgaerrors.ErrorWithStack(internal),
	}
}

func AssertionsNotForAuthorizationModelFound(modelID string) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_AUTHORIZATION_MODEL_ASSERTIONS_NOT_FOUND), fmt.Sprintf("No assertions found for authorization model '%s'", modelID))
}

func AuthorizationModelNotFound(modelID string) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_AUTHORIZATION_MODEL_NOT_FOUND), fmt.Sprintf("Authorization Model '%s' not found", modelID))
}

func LatestAuthorizationModelNotFound(store string) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_LATEST_AUTHORIZATION_MODEL_NOT_FOUND), fmt.Sprintf("No authorization models found for store '%s'", store))
}

func TypeNotFound(t string) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_TYPE_NOT_FOUND), fmt.Sprintf("Type '%s' not found", t))
}

func RelationNotFound(relation string, typeDefinition string, tuple *openfgapb.TupleKey) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_RELATION_NOT_FOUND), fmt.Sprintf("Relation '%s' not found in type definition '%s' for tuple (%s)", relation, typeDefinition, tuple.String()))
}

func EmptyRelationDefinition(typeDefinition, relation string) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_EMPTY_RELATION_DEFINITION), fmt.Sprintf("Type definition '%s' contains an empty relation definition '%s'", typeDefinition, relation))
}

func ExceededEntityLimit(entity string, limit int) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_EXCEEDED_ENTITY_LIMIT),
		fmt.Sprintf("The number of %s exceeds the allowed limit of %d", entity, limit))
}

func InvalidUser(user string) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_INVALID_USER), fmt.Sprintf("User '%s' is invalid", user))
}

func InvalidTuple(reason string, tuple *openfgapb.TupleKey) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_INVALID_TUPLE), fmt.Sprintf("Invalid tuple '%s'. Reason: %s", tuple.String(), reason))
}

func InvalidContextualTuple(tk *openfgapb.TupleKey) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_INVALID_CONTEXTUAL_TUPLE), fmt.Sprintf("Invalid contextual tuple: %s. Please provide a user, object and relation.", tk.String()))
}

func DuplicateContextualTuple(tk *openfgapb.TupleKey) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_DUPLICATE_CONTEXTUAL_TUPLE), fmt.Sprintf("Duplicate contextual tuple in request: %s.", tk.String()))
}

// InvalidObjectFormat is used when an object does not have a type and id part
func InvalidObjectFormat(tuple *openfgapb.TupleKey) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_INVALID_OBJECT_FORMAT), fmt.Sprintf("Invalid object format for tuple '%s'", tuple.String()))
}

func UnknownRelation(relation string) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_UNKNOWN_RELATION), fmt.Sprintf("Authorization model contains an unknown relation '%s'", relation))
}

func DuplicateTupleInWrite(tk *openfgapb.TupleKey) error {
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_CANNOT_ALLOW_DUPLICATE_TUPLES_IN_ONE_REQUEST), fmt.Sprintf("duplicate tuple in write: user: '%s', relation: '%s', object: '%s'", tk.GetUser(), tk.GetRelation(), tk.GetObject()))
}

func WriteFailedDueToInvalidInput(err error) error {
	if err != nil {
		return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_WRITE_FAILED_DUE_TO_INVALID_INPUT), err.Error())
	}
	return status.Error(codes.Code(openfgapb.ErrorCode_ERROR_CODE_WRITE_FAILED_DUE_TO_INVALID_INPUT), "Write failed due to invalid input")
}

// HandleError is used to hide internal errors from users. Use `public` to return an error message to the user.
func HandleError(public string, err error) error {
	if errors.Is(err, storage.ErrInvalidContinuationToken) {
		return InvalidContinuationToken
	} else if errors.Is(err, storage.ErrMismatchObjectType) {
		return MismatchObjectType
	} else if errors.Is(err, storage.ErrCancelled) {
		return RequestCancelled
	}
	return NewInternalError(public, err)
}

// HandleTupleValidateError provide common routines for handling tuples validation error
func HandleTupleValidateError(err error) error {
	switch t := err.(type) {
	case *tuple.InvalidTupleError:
		return InvalidTuple(t.Reason, t.TupleKey)
	case *tuple.InvalidObjectFormatError:
		return InvalidObjectFormat(t.TupleKey)
	case *tuple.TypeNotFoundError:
		return TypeNotFound(t.TypeName)
	case *tuple.RelationNotFoundError:
		return RelationNotFound(t.Relation, t.TypeName, t.TupleKey)
	}

	return HandleError("", err)
}
