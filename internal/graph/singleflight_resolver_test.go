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
	for i := 1; i <= 5; i++ {
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
		TupleKey:           tuple.NewTupleKey("folder:5", "viewer", "user:jon"),
		ContextualTuples:   nil,
		ResolutionMetadata: &ResolutionMetadata{Depth: 5},
	}

	//--------------------------------------

	checkerWithoutSingleflight := NewLocalChecker(
		storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
	)
	defer checkerWithoutSingleflight.Close()

	req := checkReq
	respWithoutSingleflight, err := checkerWithoutSingleflight.ResolveCheck(ctx, &req)

	require.NoError(t, err)
	require.True(t, respWithoutSingleflight.GetAllowed())

	fmt.Printf("Without singleflight:\n")
	fmt.Printf("%+v\n", respWithoutSingleflight.GetResolutionMetadata())

	r := reflect.ValueOf(deduplicatedDispatchesCounter)
	count := reflect.Indirect(r).FieldByName("valInt").Uint()
	require.Zero(t, count)

	//--------------------------------------

	checkerWithSingleflight := NewLocalChecker(
		storagewrappers.NewCombinedTupleReader(ds, []*openfgav1.TupleKey{}),
		WithSingleflightResolver(),
	)
	defer checkerWithSingleflight.Close()

	req = checkReq
	resWithSingleflight, err := checkerWithSingleflight.ResolveCheck(ctx, &req)

	fmt.Printf("With singleflight:\n")
	fmt.Printf("%+v\n", resWithSingleflight.GetResolutionMetadata())
	require.NoError(t, err)
	require.True(t, resWithSingleflight.GetAllowed())

	count = reflect.Indirect(r).FieldByName("valInt").Uint()
	require.Equal(t, uint64(8), count)
}
