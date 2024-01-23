package graph

import (
	"context"
	"fmt"
	reflect "reflect"
	"testing"

	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/storagewrappers"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/stretchr/testify/require"
)

func TestSingleflightResolver(t *testing.T) {
	ds := memory.New()
	defer ds.Close()

	storeID := ulid.Make().String()

	var tuples = []*openfgav1.TupleKey{
		tuple.NewTupleKey("folder:1", "viewer", "user:jon"),
	}
	for i := 1; i <= 10; i++ {
		tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("folder:%d", i+1), "parent", fmt.Sprintf("folder:%d", i)))
		tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("folder:%d", i+1), "other_parent", fmt.Sprintf("folder:%d", i)))
		tuples = append(tuples, tuple.NewTupleKey(fmt.Sprintf("folder:%d", i+1), "other_other_parent", fmt.Sprintf("folder:%d", i)))
	}

	err := ds.Write(context.Background(), storeID, nil, tuples)
	require.NoError(t, err)

	typedefs := parser.MustTransformDSLToProto(`model
	schema 1.1

	type user

	type folder
	relations
	  define parent: [folder]
	  define other_parent: [folder]
	  define other_other_parent: [folder]
	  define viewer: [user] or viewer from parent or viewer from other_parent or viewer from other_other_parent`).TypeDefinitions

	ctx := typesystem.ContextWithTypesystem(context.Background(), typesystem.New(
		&openfgav1.AuthorizationModel{
			Id:              ulid.Make().String(),
			TypeDefinitions: typedefs,
			SchemaVersion:   typesystem.SchemaVersion1_1,
		},
	))

	checkReq := ResolveCheckRequest{
		StoreID:            storeID,
		TupleKey:           tuple.NewTupleKey("folder:10", "viewer", "user:jon"),
		ContextualTuples:   nil,
		ResolutionMetadata: &ResolutionMetadata{Depth: 10},
	}

	checkerWithoutSingleflight := NewLocalChecker(
		storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
	)
	defer checkerWithoutSingleflight.Close()

	req := checkReq
	respWithoutSingleflight, err := checkerWithoutSingleflight.ResolveCheck(ctx, &req)

	require.NoError(t, err)
	require.True(t, respWithoutSingleflight.GetAllowed())

	count := reflect.Indirect(reflect.ValueOf(deduplicatedDispatchesCounter)).FieldByName("valInt").Uint()
	require.Zero(t, count)
	count = reflect.Indirect(reflect.ValueOf(deduplicatedDBQueriesCounter)).FieldByName("valInt").Uint()
	require.Zero(t, count)

	checkerWithSingleflight := NewLocalChecker(
		storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
		WithSingleflightResolver(),
	)
	defer checkerWithSingleflight.Close()

	req = checkReq
	resWithSingleflight, err := checkerWithSingleflight.ResolveCheck(ctx, &req)

	require.NoError(t, err)
	require.True(t, resWithSingleflight.GetAllowed())

	count = reflect.Indirect(reflect.ValueOf(deduplicatedDispatchesCounter)).FieldByName("valInt").Uint()
	require.GreaterOrEqual(t, count, uint64(15))
	count = reflect.Indirect(reflect.ValueOf(deduplicatedDBQueriesCounter)).FieldByName("valInt").Uint()
	require.GreaterOrEqual(t, count, uint64(10))

	require.Less(t, resWithSingleflight.GetResolutionMetadata().DatastoreQueryCount, respWithoutSingleflight.GetResolutionMetadata().DatastoreQueryCount)
}
