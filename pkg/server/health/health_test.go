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

const (
	targetServiceName    = "openfga.v1.OpenFGAService"
	livenessServiceName  = "openfga.v1.OpenFGAService.liveness"
	readinessServiceName = "openfga.v1.OpenFGAService.readiness"
)

type fakeTargetService struct {
	ready  bool
	err    error
	called bool
}

func (f *fakeTargetService) IsReady(context.Context) (bool, error) {
	f.called = true
	return f.ready, f.err
}

func TestCheck(t *testing.T) {
	errNotReady := errors.New("datastore unavailable")

	tests := []struct {
		name           string
		service        string
		target         *fakeTargetService
		expectedStatus healthv1pb.HealthCheckResponse_ServingStatus
		expectedErr    error
		expectedCalled bool
	}{
		{
			name:           "empty_service_ready",
			service:        "",
			target:         &fakeTargetService{ready: true},
			expectedStatus: healthv1pb.HealthCheckResponse_SERVING,
			expectedCalled: true,
		},
		{
			name:           "target_service_not_ready",
			service:        targetServiceName,
			target:         &fakeTargetService{ready: false},
			expectedStatus: healthv1pb.HealthCheckResponse_NOT_SERVING,
			expectedCalled: true,
		},
		{
			name:           "readiness_ready",
			service:        readinessServiceName,
			target:         &fakeTargetService{ready: true},
			expectedStatus: healthv1pb.HealthCheckResponse_SERVING,
			expectedCalled: true,
		},
		{
			name:           "readiness_not_ready",
			service:        readinessServiceName,
			target:         &fakeTargetService{ready: false},
			expectedStatus: healthv1pb.HealthCheckResponse_NOT_SERVING,
			expectedCalled: true,
		},
		{
			name:           "readiness_error",
			service:        readinessServiceName,
			target:         &fakeTargetService{err: errNotReady},
			expectedStatus: healthv1pb.HealthCheckResponse_NOT_SERVING,
			expectedErr:    errNotReady,
			expectedCalled: true,
		},
		{
			name:           "liveness_ignores_not_ready_target",
			service:        livenessServiceName,
			target:         &fakeTargetService{ready: false},
			expectedStatus: healthv1pb.HealthCheckResponse_SERVING,
		},
		{
			name:           "liveness_ignores_target_error",
			service:        livenessServiceName,
			target:         &fakeTargetService{err: errNotReady},
			expectedStatus: healthv1pb.HealthCheckResponse_SERVING,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			checker := &Checker{TargetService: test.target, TargetServiceName: targetServiceName}

			resp, err := checker.Check(context.Background(), &healthv1pb.HealthCheckRequest{Service: test.service})

			require.ErrorIs(t, err, test.expectedErr)
			require.Equal(t, test.expectedStatus, resp.GetStatus())
			require.Equal(t, test.expectedCalled, test.target.called)
		})
	}

	t.Run("unknown_service", func(t *testing.T) {
		target := &fakeTargetService{ready: true}
		checker := &Checker{TargetService: target, TargetServiceName: targetServiceName}

		resp, err := checker.Check(context.Background(), &healthv1pb.HealthCheckRequest{Service: "unknown"})

		require.Nil(t, resp)
		require.Equal(t, codes.NotFound, status.Code(err))
		require.False(t, target.called)
	})
}
