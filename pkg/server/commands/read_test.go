package commands

import (
	"context"
	"fmt"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/wrapperspb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/memory"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
)

func TestReadCommand(t *testing.T) {
	t.Run("throws_error_if_input_is_invalid", func(t *testing.T) {
		testCases := map[string]struct {
			request *openfgav1.ReadRequest
		}{
			`missing_object_type`: {
				request: &openfgav1.ReadRequest{
					TupleKey: &openfgav1.ReadRequestTupleKey{
						Object: "openfga/iam",
					},
				},
			},
			`missing_object_type_and_id_1`: {
				request: &openfgav1.ReadRequest{
					TupleKey: &openfgav1.ReadRequestTupleKey{
						Object: ":",
					},
				},
			},
			`missing_object_type_and_id_2`: {
				request: &openfgav1.ReadRequest{
					TupleKey: &openfgav1.ReadRequestTupleKey{
						Relation: "admin",
						User:     "github|jonallie",
					},
				},
			},
			`missing_object_id`: {
				request: &openfgav1.ReadRequest{
					TupleKey: &openfgav1.ReadRequestTupleKey{
						Object:   "repo:",
						Relation: "writer",
					},
				},
			},
			`missing_user_and_object_id`: {
				request: &openfgav1.ReadRequest{
					TupleKey: &openfgav1.ReadRequestTupleKey{
						Relation: "writer",
					},
				},
			},
		}
		for name, test := range testCases {
			t.Run(name, func(t *testing.T) {
				mockController := gomock.NewController(t)
				defer mockController.Finish()
				mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
				cmd := NewReadQuery(mockDatastore)
				resp, err := cmd.Execute(context.Background(), test.request)
				require.Nil(t, resp)
				require.ErrorIs(t, err, serverErrors.ValidationError(
					fmt.Errorf("the 'tuple_key' field was provided but the object type field is required and both the object id and user cannot be empty"),
				))
			})
		}
	})

	t.Run("calls_storage_read_page", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		filter := storage.ReadFilter{Object: "document:1", Relation: "reader", User: "user:maria"}
		storeID := ulid.Make().String()

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		opts := storage.ReadPageOptions{
			Pagination: storage.PaginationOptions{
				PageSize: storage.DefaultPageSize,
				From:     "",
			},
			Consistency: storage.ConsistencyOptions{Preference: openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY},
		}
		mockDatastore.EXPECT().ReadPage(gomock.Any(), storeID, filter, opts).Times(1)

		cmd := NewReadQuery(mockDatastore)
		_, err := cmd.Execute(context.Background(), &openfgav1.ReadRequest{
			StoreId:           storeID,
			TupleKey:          &openfgav1.ReadRequestTupleKey{Object: "document:1", Relation: "reader", User: "user:maria"},
			PageSize:          nil,
			ContinuationToken: "",
			Consistency:       openfgav1.ConsistencyPreference_HIGHER_CONSISTENCY,
		})
		require.NoError(t, err)
	})

	t.Run("calls_token_decoder", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		filter := storage.ReadFilter{Object: "document:1", Relation: "reader", User: "user:maria"}
		storeID := ulid.Make().String()
		pageSize := int32(45)

		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		opts := storage.ReadPageOptions{
			Pagination: storage.PaginationOptions{
				PageSize: int(pageSize),
				From:     "token",
			},
		}
		mockDatastore.EXPECT().ReadPage(gomock.Any(), storeID, filter, opts).Times(1)
		cmd := NewReadQuery(mockDatastore)
		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadRequest{
			StoreId:           storeID,
			TupleKey:          &openfgav1.ReadRequestTupleKey{Object: "document:1", Relation: "reader", User: "user:maria"},
			PageSize:          wrapperspb.Int32(pageSize),
			ContinuationToken: "token",
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Empty(t, resp.GetTuples())
		require.Empty(t, resp.GetContinuationToken())
	})

	t.Run("throws_error_if_continuation_token_is_invalid", func(t *testing.T) {
		mockController := gomock.NewController(t)
		defer mockController.Finish()

		storeID := ulid.Make().String()
		pageSize := int32(45)

		mockEncoder := mocks.NewMockEncoder(mockController)
		mockEncoder.EXPECT().Decode(gomock.Any()).Return([]byte{}, fmt.Errorf("error decoding token")).Times(1)
		mockDatastore := mocks.NewMockOpenFGADatastore(mockController)
		cmd := NewReadQuery(mockDatastore)
		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadRequest{
			StoreId:           storeID,
			TupleKey:          &openfgav1.ReadRequestTupleKey{Object: "document:1", Relation: "reader", User: "user:maria"},
			PageSize:          wrapperspb.Int32(pageSize),
			ContinuationToken: "token",
		})
		require.Nil(t, resp)
		require.ErrorIs(t, err, serverErrors.ErrInvalidContinuationToken)
	})

	t.Run("accepts_types_that_are_not_defined_in_current_model", func(t *testing.T) {
		datastore := memory.New()
		t.Cleanup(datastore.Close)

		model := `
			model
			  schema 1.1

			type user_new

			type document_new
			  relations
			    define viewer_new: [user, user:*]`
		tuples := []string{
			"document_old:budget#admin_old@user_old:maria",
		}

		storeID, _ := storagetest.BootstrapFGAStore(t, datastore, model, tuples)

		cmd := NewReadQuery(datastore)
		resp, err := cmd.Execute(context.Background(), &openfgav1.ReadRequest{
			StoreId:  storeID,
			TupleKey: &openfgav1.ReadRequestTupleKey{Object: "document_old:budget"},
		})
		require.NoError(t, err)
		require.Len(t, resp.GetTuples(), 1)
		require.Equal(t, "document_old:budget", resp.GetTuples()[0].GetKey().GetObject())
		require.Equal(t, "admin_old", resp.GetTuples()[0].GetKey().GetRelation())
		require.Equal(t, "user_old:maria", resp.GetTuples()[0].GetKey().GetUser())
	})
}
