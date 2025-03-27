// Package health contains the service that check the health of an OpenFGA server.
package health

import (
	"context"

	grpcauth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"google.golang.org/grpc/codes"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
)

// TargetService defines an interface that services can implement for server health checks.
type TargetService interface {
	IsReady(ctx context.Context) (bool, error)
}

type Checker struct {
	healthv1pb.UnimplementedHealthServer
	TargetService
	TargetServiceName string
}

var _ grpcauth.ServiceAuthFuncOverride = (*Checker)(nil)

// AuthFuncOverride implements the grpc_auth.ServiceAuthFuncOverride interface by bypassing authn middleware.
func (o *Checker) AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	return ctx, nil
}

func (o *Checker) Check(ctx context.Context, req *healthv1pb.HealthCheckRequest) (*healthv1pb.HealthCheckResponse, error) {
	requestedService := req.GetService()
	if requestedService == "" || requestedService == o.TargetServiceName {
		ready, err := o.IsReady(ctx)
		if err != nil {
			return &healthv1pb.HealthCheckResponse{Status: healthv1pb.HealthCheckResponse_NOT_SERVING}, err
		}

		if !ready {
			return &healthv1pb.HealthCheckResponse{Status: healthv1pb.HealthCheckResponse_NOT_SERVING}, nil
		}

		return &healthv1pb.HealthCheckResponse{Status: healthv1pb.HealthCheckResponse_SERVING}, nil
	}

	return nil, status.Errorf(codes.NotFound, "service '%s' is not registered with the Health server", requestedService)
}

func (o *Checker) Watch(req *healthv1pb.HealthCheckRequest, server healthv1pb.Health_WatchServer) error {
	return status.Error(codes.Unimplemented, "unimplemented streaming endpoint")
}
