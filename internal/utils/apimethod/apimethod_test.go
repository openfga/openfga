package apimethod

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAPIMethod_String(t *testing.T) {
	require.Equal(t, "Check", Check.String())
	require.Equal(t, "BatchCheck", BatchCheck.String())
	require.Equal(t, "ListObjects", ListObjects.String())
	require.Equal(t, "StreamedListObjects", StreamedListObjects.String())
	require.Equal(t, "ListUsers", ListUsers.String())
	require.Equal(t, "Write", Write.String())
	require.Equal(t, "Read", Read.String())
	require.Equal(t, "Expand", Expand.String())
	require.Equal(t, "WriteAuthorizationModel", WriteAuthorizationModel.String())
	require.Equal(t, "ReadAuthorizationModel", ReadAuthorizationModel.String())
	require.Equal(t, "ReadAuthorizationModels", ReadAuthorizationModels.String())
	require.Equal(t, "CreateStore", CreateStore.String())
	require.Equal(t, "GetStore", GetStore.String())
	require.Equal(t, "DeleteStore", DeleteStore.String())
	require.Equal(t, "ListStores", ListStores.String())
	require.Equal(t, "ReadChanges", ReadChanges.String())
	require.Equal(t, "WriteAssertions", WriteAssertions.String())
	require.Equal(t, "ReadAssertions", ReadAssertions.String())

	// String mirrors the underlying constant value for an arbitrary method too.
	require.Equal(t, "custom", APIMethod("custom").String())
}
