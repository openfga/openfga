package test

import (
	"context"
	"fmt"
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"

	"github.com/openfga/openfga/pkg/server"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
)

func TestDeleteStore(t *testing.T, s *server.Server) {
	ctx := context.Background()

	createStoreResponse, err := s.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: "acme",
	})
	require.NoError(t, err)

	type deleteStoreTest struct {
		_name   string
		request *openfgav1.DeleteStoreRequest
		err     error
	}
	var tests = []deleteStoreTest{
		{
			_name: "Execute_Delete_Store_With_Non_Existent_Store_Succeeds",
			request: &openfgav1.DeleteStoreRequest{
				StoreId: "unknownstore",
			},
			err: serverErrors.InvalidArgumentError(
				fmt.Errorf(`invalid DeleteStoreRequest.StoreId: value does not match regex pattern "^[ABCDEFGHJKMNPQRSTVWXYZ0-9]{26}$"`)),
		},
		{
			_name: "Execute_Succeeds",
			request: &openfgav1.DeleteStoreRequest{
				StoreId: createStoreResponse.GetId(),
			},
		},
	}

	for _, test := range tests {
		t.Run(test._name, func(t *testing.T) {
			_, err := s.DeleteStore(ctx, test.request)

			if test.err != nil {
				require.ErrorIs(t, err, test.err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
