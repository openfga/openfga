package local_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/openfga/openfga/internal/dispatch"
	"github.com/openfga/openfga/internal/dispatch/local"
	dispatchv1 "github.com/openfga/openfga/internal/proto/dispatch/v1alpha1"
	"github.com/openfga/openfga/pkg/storage/memory"
	storagetest "github.com/openfga/openfga/pkg/storage/test"
	"github.com/openfga/openfga/pkg/typesystem"
)

var cmpOpts = cmpopts.IgnoreUnexported(
	dispatchv1.DispatchCheckResponse{},
	dispatchv1.DispatchCheckResolutionMetadata{},
	dispatchv1.DispatchCheckResult{},
)

func TestDispatchCheck(t *testing.T) {

	t.Run("residual_object_ids_require_further_dispatch", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)

		typesystemResolver, typesystemResolverStop := typesystem.MemoizedTypesystemResolverFunc(ds)
		t.Cleanup(typesystemResolverStop)

		dispatcher := local.NewLocalDispatcher(
			ds,
			typesystemResolver,
			local.WithMaxConcurrentDispatches(1),
			local.WithMaxDispatchBatchSize(1),
		)

		modelStr := `
		model
		  schema 1.1

		type user

		type group
		  relations
			define member: [user, group#member]
		`

		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, []string{
			"group:x#member@group:1#member",
			"group:x#member@group:2#member",
		})

		ctx := context.Background()

		typesys, err := typesystem.NewAndValidate(ctx, model)
		require.NoError(t, err)

		dispatchResp, err := dispatcher.DispatchCheck(ctx, &dispatchv1.DispatchCheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: typesys.GetAuthorizationModelID(),
			ObjectType:           "group",
			ObjectIds:            []string{"x"},
			Relation:             "member",
			SubjectType:          "user",
			SubjectId:            "jon",
			SubjectRelation:      "",
			RequestMetadata:      dispatch.NewDispatchCheckRequestMetadata(dispatch.DefaultCheckResolutionDepth),
		})
		require.NoError(t, err)

		expectedResultsForeachObjectID := map[string]*dispatchv1.DispatchCheckResult{
			"x": {Allowed: false},
		}

		if diff := cmp.Diff(expectedResultsForeachObjectID, dispatchResp.GetResultsForeachObjectId(), cmpOpts); diff != "" {
			require.FailNow(t, "mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("reflexive_relationships_merged_with_final_result", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)

		typesystemResolver, typesystemResolverStop := typesystem.MemoizedTypesystemResolverFunc(ds)
		t.Cleanup(typesystemResolverStop)

		dispatcher := local.NewLocalDispatcher(
			ds,
			typesystemResolver,
		)

		modelStr := `
		model
		  schema 1.1

		type user

		type group
		  relations
			define member: [user, group#member]
		`

		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, []string{
			"group:y#member@group:x#member",
		})

		ctx := context.Background()

		typesys, err := typesystem.NewAndValidate(ctx, model)
		require.NoError(t, err)

		dispatchResp, err := dispatcher.DispatchCheck(ctx, &dispatchv1.DispatchCheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: typesys.GetAuthorizationModelID(),
			ObjectType:           "group",
			ObjectIds:            []string{"x", "y"},
			Relation:             "member",
			SubjectType:          "group",
			SubjectId:            "x",
			SubjectRelation:      "member",
			RequestMetadata:      dispatch.NewDispatchCheckRequestMetadata(dispatch.DefaultCheckResolutionDepth),
		})
		require.NoError(t, err)

		expectedResultsForeachObjectID := map[string]*dispatchv1.DispatchCheckResult{
			"x": {Allowed: true},
			"y": {Allowed: true},
		}

		if diff := cmp.Diff(expectedResultsForeachObjectID, dispatchResp.GetResultsForeachObjectId(), cmpOpts); diff != "" {
			require.FailNow(t, "mismatch (-want +got):\n%s", diff)
		}
	})

	t.Run("cyclical_userset_dispatch_resolves", func(t *testing.T) {
		ds := memory.New()
		t.Cleanup(ds.Close)

		typesystemResolver, typesystemResolverStop := typesystem.MemoizedTypesystemResolverFunc(ds)
		t.Cleanup(typesystemResolverStop)

		dispatcher := local.NewLocalDispatcher(
			ds,
			typesystemResolver,
		)

		modelStr := `
		model
		  schema 1.1

		type user

		type group
		  relations
			define member: [user, group#member]
		`

		storeID, model := storagetest.BootstrapFGAStore(t, ds, modelStr, []string{
			"group:y#member@group:y#member",
		})

		ctx := context.Background()

		typesys, err := typesystem.NewAndValidate(ctx, model)
		require.NoError(t, err)

		dispatchResp, err := dispatcher.DispatchCheck(ctx, &dispatchv1.DispatchCheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: typesys.GetAuthorizationModelID(),
			ObjectType:           "group",
			ObjectIds:            []string{"y"},
			Relation:             "member",
			SubjectType:          "user",
			SubjectId:            "jon",
			SubjectRelation:      "",
			RequestMetadata:      dispatch.NewDispatchCheckRequestMetadata(dispatch.DefaultCheckResolutionDepth),
		})
		require.NoError(t, err)

		expectedResultsForeachObjectID := map[string]*dispatchv1.DispatchCheckResult{
			"y": {Allowed: false},
		}

		if diff := cmp.Diff(expectedResultsForeachObjectID, dispatchResp.GetResultsForeachObjectId(), cmpOpts); diff != "" {
			require.FailNow(t, "mismatch (-want +got):\n%s", diff)
		}
	})
}

func BenchmarkLocalDispatcher(b *testing.B) {
	b.Run("widely_nested_userset_dispatching", func(b *testing.B) {
		ds := memory.New()
		b.Cleanup(ds.Close)

		typesystemResolver, typesystemResolverStop := typesystem.MemoizedTypesystemResolverFunc(ds)
		b.Cleanup(typesystemResolverStop)

		dispatcher := local.NewLocalDispatcher(
			ds,
			typesystemResolver,
		)

		modelStr := `
		model
		  schema 1.1

		type user

		type group
		  relations
			define member: [user, group#member]
		`

		var tuples []string
		for i := 0; i < 10000; i++ {
			tuples = append(tuples, fmt.Sprintf("group:x#member@group:%d#member", i))
		}

		storeID, model := storagetest.BootstrapFGAStore(b, ds, modelStr, tuples)

		ctx := context.Background()

		// prefetch the model so as to unbias the benchmark loop
		typesys, err := typesystemResolver(ctx, storeID, model.GetId())
		require.NoError(b, err)

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, err := dispatcher.DispatchCheck(ctx, &dispatchv1.DispatchCheckRequest{
				StoreId:              storeID,
				AuthorizationModelId: typesys.GetAuthorizationModelID(),
				ObjectType:           "group",
				ObjectIds:            []string{"x"},
				Relation:             "member",
				SubjectType:          "user",
				SubjectId:            "jon",
				SubjectRelation:      "",
				RequestMetadata:      dispatch.NewDispatchCheckRequestMetadata(dispatch.DefaultCheckResolutionDepth),
			})
			require.NoError(b, err)
		}
	})
}
