package server

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/featureflags"
)

func TestCheck_Validation(t *testing.T) {
	t.Parallel()

	testServer := &Server{featureFlagClient: featureflags.NewNoopFeatureFlagClient()}
	ctx := t.Context()

	tests := []struct {
		name                 string
		givenRequest         *openfgav1.CheckRequest
		expectedErrorMessage string
	}{
		{
			name:                 "missing store id",
			givenRequest:         &openfgav1.CheckRequest{},
			expectedErrorMessage: "invalid CheckRequest.StoreId: value does not match regex pattern \"^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$\"",
		},
		{
			name: "missing tuple_key",
			givenRequest: &openfgav1.CheckRequest{
				StoreId: "01K3RZVNE3NJ4FYKK99QN013G2",
			},
			expectedErrorMessage: "invalid CheckRequest.TupleKey: value is required",
		},
		{
			name: "missing object",
			givenRequest: &openfgav1.CheckRequest{
				StoreId: "01K3RZVNE3NJ4FYKK99QN013G2",
				TupleKey: &openfgav1.CheckRequestTupleKey{
					User:     "user:anne",
					Relation: "viewer",
				},
			},
			expectedErrorMessage: "invalid CheckRequest.TupleKey: embedded message failed validation | caused by: invalid CheckRequestTupleKey.Object: value does not match regex pattern \"^[^\\\\s]{2,256}$\"",
		},
		{
			name: "missing relation",
			givenRequest: &openfgav1.CheckRequest{
				StoreId: "01K3RZVNE3NJ4FYKK99QN013G2",
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object: "doc:1",
					User:   "user:anne",
				},
			},
			expectedErrorMessage: "invalid CheckRequest.TupleKey: embedded message failed validation | caused by: invalid CheckRequestTupleKey.Relation: value does not match regex pattern \"^[^:#@\\\\s]{1,50}$\"",
		},
		{
			name: "missing user",
			givenRequest: &openfgav1.CheckRequest{
				StoreId: "01K3RZVNE3NJ4FYKK99QN013G2",
				TupleKey: &openfgav1.CheckRequestTupleKey{
					Object:   "doc:1",
					Relation: "viewer",
				},
			},
			expectedErrorMessage: "invalid CheckRequest.TupleKey: embedded message failed validation | caused by: invalid CheckRequestTupleKey.User: value does not match regex pattern \"^[^\\\\s]{2,512}$\"",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			actualResponse, err := testServer.Check(ctx, test.givenRequest)

			gRPCStatus, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, codes.InvalidArgument, gRPCStatus.Code())
			require.Equal(t, test.expectedErrorMessage, gRPCStatus.Message())
			require.Nil(t, actualResponse)
		})
	}
}
