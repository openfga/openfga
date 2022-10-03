package test

import (
	"context"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/encrypter"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/storage"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestReadTuplesQuery(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()
	store := id.Must(id.New()).String()

	model := &openfgapb.AuthorizationModel{
		Id:              id.Must(id.New()).String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{{Type: "repo"}},
	}

	err := datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(err)

	writes := []*openfgapb.TupleKey{
		{
			Object:   "repo:openfga/foo",
			Relation: "admin",
			User:     "github|jon.allie@openfga",
		},
		{
			Object:   "repo:openfga/bar",
			Relation: "admin",
			User:     "github|jon.allie@openfga",
		},
		{
			Object:   "repo:openfga/baz",
			Relation: "admin",
			User:     "github|jon.allie@openfga",
		},
	}
	err = datastore.Write(ctx, store, []*openfgapb.TupleKey{}, writes)
	require.NoError(err)

	cmd := commands.NewReadTuplesQuery(datastore, logger, encoder.NewBase64Encoder())

	firstRequest := &openfgapb.ReadTuplesRequest{
		StoreId:           store,
		PageSize:          wrapperspb.Int32(1),
		ContinuationToken: "",
	}
	firstResponse, err := cmd.Execute(ctx, firstRequest)
	require.NoError(err)

	require.Len(firstResponse.Tuples, 1)
	require.NotEmpty(firstResponse.ContinuationToken)

	var receivedTuples []*openfgapb.TupleKey
	for _, tuple := range firstResponse.Tuples {
		receivedTuples = append(receivedTuples, tuple.Key)
	}

	secondRequest := &openfgapb.ReadTuplesRequest{StoreId: store, ContinuationToken: firstResponse.ContinuationToken}
	secondResponse, err := cmd.Execute(ctx, secondRequest)
	require.NoError(err)
	require.Len(secondResponse.Tuples, 2)

	// no token <=> no more results
	require.Empty(secondResponse.ContinuationToken)

	for _, tuple := range secondResponse.Tuples {
		receivedTuples = append(receivedTuples, tuple.Key)
	}

	cmpOpts := []cmp.Option{
		cmpopts.IgnoreUnexported(openfgapb.TupleKey{}, openfgapb.Tuple{}, openfgapb.TupleChange{}, openfgapb.Assertion{}),
		cmpopts.IgnoreFields(openfgapb.Tuple{}, "Timestamp"),
		cmpopts.IgnoreFields(openfgapb.TupleChange{}, "Timestamp"),
		testutils.TupleKeyCmpTransformer,
	}

	if diff := cmp.Diff(writes, receivedTuples, cmpOpts...); diff != "" {
		t.Errorf("Tuple mismatch (-got +want):\n%s", diff)
	}
}

func TestReadTuplesQueryInvalidContinuationToken(t *testing.T, datastore storage.OpenFGADatastore) {
	require := require.New(t)
	ctx := context.Background()
	logger := logger.NewNoopLogger()

	encrypter, err := encrypter.NewGCMEncrypter("key")
	require.NoError(err)

	encoder := encoder.NewTokenEncoder(encrypter, encoder.NewBase64Encoder())

	store := testutils.CreateRandomString(10)

	model := &openfgapb.AuthorizationModel{
		Id:              id.Must(id.New()).String(),
		SchemaVersion:   typesystem.SchemaVersion1_0,
		TypeDefinitions: []*openfgapb.TypeDefinition{{Type: "repo"}},
	}

	err = datastore.WriteAuthorizationModel(ctx, store, model)
	require.NoError(err)

	_, err = commands.NewReadTuplesQuery(datastore, logger, encoder).Execute(ctx, &openfgapb.ReadTuplesRequest{
		StoreId:           store,
		ContinuationToken: "foo",
	})
	require.ErrorIs(err, serverErrors.InvalidContinuationToken)
}
