package health

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

type fakeTargetService struct {
	ready bool
	err   error
}

func (f fakeTargetService) IsReady(context.Context) (bool, error) {
	return f.ready, f.err
}

func newChecker(svc TargetService, name string) *Checker {
	return &Checker{TargetService: svc, TargetServiceName: name}
}

func TestChecker_Check(t *testing.T) {
	t.Run("serving_when_ready_and_empty_service", func(t *testing.T) {
		c := newChecker(fakeTargetService{ready: true}, "openfga")
		resp, err := c.Check(context.Background(), &healthv1pb.HealthCheckRequest{Service: ""})
		require.NoError(t, err)
		require.Equal(t, healthv1pb.HealthCheckResponse_SERVING, resp.GetStatus())
	})

	t.Run("serving_when_service_name_matches", func(t *testing.T) {
		c := newChecker(fakeTargetService{ready: true}, "openfga")
		resp, err := c.Check(context.Background(), &healthv1pb.HealthCheckRequest{Service: "openfga"})
		require.NoError(t, err)
		require.Equal(t, healthv1pb.HealthCheckResponse_SERVING, resp.GetStatus())
	})

	t.Run("not_serving_when_not_ready", func(t *testing.T) {
		c := newChecker(fakeTargetService{ready: false}, "openfga")
		resp, err := c.Check(context.Background(), &healthv1pb.HealthCheckRequest{Service: "openfga"})
		require.NoError(t, err)
		require.Equal(t, healthv1pb.HealthCheckResponse_NOT_SERVING, resp.GetStatus())
	})

	t.Run("not_serving_and_error_when_isready_errors", func(t *testing.T) {
		wantErr := errors.New("db unavailable")
		c := newChecker(fakeTargetService{err: wantErr}, "openfga")
		resp, err := c.Check(context.Background(), &healthv1pb.HealthCheckRequest{Service: "openfga"})
		require.ErrorIs(t, err, wantErr)
		require.Equal(t, healthv1pb.HealthCheckResponse_NOT_SERVING, resp.GetStatus())
	})

	t.Run("not_found_for_unregistered_service", func(t *testing.T) {
		c := newChecker(fakeTargetService{ready: true}, "openfga")
		_, err := c.Check(context.Background(), &healthv1pb.HealthCheckRequest{Service: "other"})
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.NotFound, st.Code())
	})
}

func TestChecker_Watch_Unimplemented(t *testing.T) {
	c := newChecker(fakeTargetService{ready: true}, "openfga")
	err := c.Watch(&healthv1pb.HealthCheckRequest{}, nil)
	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unimplemented, st.Code())
}

func TestChecker_AuthFuncOverride_BypassesAuth(t *testing.T) {
	c := newChecker(fakeTargetService{ready: true}, "openfga")
	type ctxKey struct{}
	ctx := context.WithValue(context.Background(), ctxKey{}, "v")
	got, err := c.AuthFuncOverride(ctx, "/grpc.health.v1.Health/Check")
	require.NoError(t, err)
	require.Equal(t, "v", got.Value(ctxKey{}))
}
