// Package health provides interfaces and mechanisms to implement server health checks.
package health

import (
	"context"

	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
)

// HealthCheckManager defines an interface to register and check the health of one
// or more services.
type HealthCheckManager interface {

	// RegisterService registers the provided serviceName under the health
	// server whom this HealthChecker is managing.
	RegisterService(serviceName string)

	// GetHealthServer returns the health server this HealthChecker is managing.
	GetHealthServer() *AuthlessHealthServer

	// Check returns a function that should be used to perform the health checks.
	Check(ctx context.Context) func() error
}

// HealthChecker defines an interface for determining if a system is
// ready to serve traffic.
type HealthChecker interface {
	IsReady(ctx context.Context) (bool, error)
}

// IgnoreAuthMixin is a struct that can be embedded to make a grpc handler ignore
// any auth requirements set by the go-ecosystem/go-grpc-middleware community middleware.
type IgnoreAuthMixin struct{}

var _ grpc_auth.ServiceAuthFuncOverride = (*IgnoreAuthMixin)(nil)

// AuthFuncOverride implements the grpc_auth.ServiceAuthFuncOverride by
// performing a no-op.
func (m IgnoreAuthMixin) AuthFuncOverride(ctx context.Context, fullMethodName string) (context.Context, error) {
	return ctx, nil
}

// AuthlessHealthServer implements the grpc health check protocol and ignores any auth
// middleware set by the go-ecosystem/go-grpc-middleware community.
//
// See https://github.com/grpc-ecosystem/go-grpc-middleware/blob/master/auth/auth.go
type AuthlessHealthServer struct {
	*health.Server
	IgnoreAuthMixin
}

// NewAuthlessHealthServer constructs a health server that bypasses grpc auth
// middleware.
func NewAuthlessHealthServer() *AuthlessHealthServer {
	return &AuthlessHealthServer{Server: health.NewServer()}
}

// SetServicesHealthy sets the provided services status to 'SERVING'.
func (s *AuthlessHealthServer) SetServicesHealthy(svcDesc ...*grpc.ServiceDesc) {
	for _, d := range svcDesc {
		s.SetServingStatus(d.ServiceName, healthv1pb.HealthCheckResponse_SERVING)
	}
}
