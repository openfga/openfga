package storagewrappers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/internal/mocks"
	"github.com/openfga/openfga/pkg/storage"
)

// noCancelContext asserts the context passed to the inner datastore is detached
// from cancellation of the caller's context (context.WithoutCancel).
func noCancelMatcher(t *testing.T) gomock.Matcher {
	t.Helper()
	return gomock.Cond(func(x any) bool {
		ctx, ok := x.(context.Context)
		if !ok {
			return false
		}
		return ctx.Done() == nil
	})
}

func TestContextTracerWrapper_DetachesContext(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDS := mocks.NewMockOpenFGADatastore(ctrl)
	wrapper := NewContextWrapper(mockDS)
	require.NotNil(t, wrapper)

	// Caller context is cancellable; the wrapper must hand the inner store a detached one.
	cancelCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	matcher := noCancelMatcher(t)

	mockDS.EXPECT().
		Read(matcher, "store", gomock.Any(), gomock.Any()).
		Return(nil, nil)
	_, err := wrapper.Read(cancelCtx, "store", storage.ReadFilter{}, storage.ReadOptions{})
	require.NoError(t, err)

	mockDS.EXPECT().
		ReadPage(matcher, "store", gomock.Any(), gomock.Any()).
		Return(nil, "", nil)
	_, _, err = wrapper.ReadPage(cancelCtx, "store", storage.ReadFilter{}, storage.ReadPageOptions{})
	require.NoError(t, err)

	mockDS.EXPECT().
		ReadUserTuple(matcher, "store", gomock.Any(), gomock.Any()).
		Return(&openfgav1.Tuple{}, nil)
	_, err = wrapper.ReadUserTuple(cancelCtx, "store", storage.ReadUserTupleFilter{}, storage.ReadUserTupleOptions{})
	require.NoError(t, err)

	mockDS.EXPECT().
		ReadUsersetTuples(matcher, "store", gomock.Any(), gomock.Any()).
		Return(nil, nil)
	_, err = wrapper.ReadUsersetTuples(cancelCtx, "store", storage.ReadUsersetTuplesFilter{}, storage.ReadUsersetTuplesOptions{})
	require.NoError(t, err)

	mockDS.EXPECT().
		ReadStartingWithUser(matcher, "store", gomock.Any(), gomock.Any()).
		Return(nil, nil)
	_, err = wrapper.ReadStartingWithUser(cancelCtx, "store", storage.ReadStartingWithUserFilter{}, storage.ReadStartingWithUserOptions{})
	require.NoError(t, err)
}

func TestContextTracerWrapper_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockDS := mocks.NewMockOpenFGADatastore(ctrl)
	mockDS.EXPECT().Close().Times(1)

	wrapper := NewContextWrapper(mockDS)
	wrapper.Close()
}
