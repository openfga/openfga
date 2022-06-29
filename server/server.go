package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/go-errors/errors"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	httpmiddleware "github.com/openfga/openfga/internal/middleware/http"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/id"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/server/commands"
	serverErrors "github.com/openfga/openfga/server/errors"
	"github.com/openfga/openfga/server/gateway"
	"github.com/openfga/openfga/server/health"
	"github.com/openfga/openfga/storage"
	"github.com/rs/cors"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

const (
	AuthorizationModelIDHeader = "openfga-authorization-model-id"
)

var (
	ErrNilTokenEncoder error = fmt.Errorf("tokenEncoder must be a non-nil interface value")
	ErrNilTransport    error = fmt.Errorf("transport must be a non-nil interface value")
)

// A Server implements the OpenFGA service backend as both
// a GRPC and HTTP server.
type Server struct {
	openfgapb.UnimplementedOpenFGAServiceServer

	healthManager health.HealthCheckManager
	tracer        trace.Tracer
	meter         metric.Meter
	logger        logger.Logger
	datastore     storage.OpenFGADatastore
	encoder       encoder.Encoder
	config        *Config
	transport     gateway.Transport

	defaultServeMuxOpts []runtime.ServeMuxOption
}

type Dependencies struct {
	Datastore storage.OpenFGADatastore
	Tracer    trace.Tracer
	Meter     metric.Meter
	Logger    logger.Logger

	// TokenEncoder is the encoder used to encode continuation tokens for paginated views.
	// Defaults to Base64Encoder if none is provided.
	TokenEncoder encoder.Encoder
	Transport    gateway.Transport
}

type Config struct {
	ServiceName            string
	GRPCServer             GRPCServerConfig
	HTTPServer             HTTPServerConfig
	ResolveNodeLimit       uint32
	ChangelogHorizonOffset int
	UnaryInterceptors      []grpc.UnaryServerInterceptor
	MuxOptions             []runtime.ServeMuxOption
	RequestTimeout         time.Duration
}

type GRPCServerConfig struct {
	Addr      string
	TLSConfig *TLSConfig
}

type HTTPServerConfig struct {
	Addr               string
	TLSConfig          *TLSConfig
	CORSAllowedOrigins []string
	CORSAllowedHeaders []string
}

type TLSConfig struct {
	CertPath string
	KeyPath  string
}

// New creates a new Server which uses the supplied backends
// for managing data.
func New(dependencies *Dependencies, config *Config) (*Server, error) {
	tokenEncoder := dependencies.TokenEncoder
	if tokenEncoder == nil {
		tokenEncoder = encoder.NewBase64Encoder()
	} else {
		t := reflect.TypeOf(tokenEncoder)
		if reflect.ValueOf(tokenEncoder) == reflect.Zero(t) {
			return nil, ErrNilTokenEncoder
		}
	}

	transport := dependencies.Transport
	if transport == nil {
		transport = gateway.NewRPCTransport(dependencies.Logger)
	} else {
		t := reflect.TypeOf(transport)
		if reflect.ValueOf(transport) == reflect.Zero(t) {
			return nil, ErrNilTransport
		}
	}

	server := &Server{
		tracer:    dependencies.Tracer,
		meter:     dependencies.Meter,
		logger:    dependencies.Logger,
		datastore: dependencies.Datastore,
		encoder:   tokenEncoder,
		transport: transport,
		config:    config,
		defaultServeMuxOpts: []runtime.ServeMuxOption{
			runtime.WithForwardResponseOption(httpmiddleware.HTTPResponseModifier),
			runtime.WithErrorHandler(func(c context.Context, sr *runtime.ServeMux, mm runtime.Marshaler, w http.ResponseWriter, r *http.Request, e error) {
				actualCode := serverErrors.ConvertToEncodedErrorCode(status.Convert(e))
				if serverErrors.IsValidEncodedError(actualCode) {
					dependencies.Logger.ErrorWithContext(c, "grpc error", logger.Error(e), logger.String("request_url", r.URL.String()))
				}

				httpmiddleware.CustomHTTPErrorHandler(c, w, r, serverErrors.NewEncodedError(actualCode, e.Error()))
			}),
		},
	}

	healthManager := NewOpenFGAServerHealthChecker(server)
	healthManager.RegisterService(openfgapb.OpenFGAService_ServiceDesc.ServiceName)
	server.healthManager = healthManager

	errors.MaxStackDepth = logger.MaxDepthBacktraceStack

	return server, nil
}

func (s *Server) Read(ctx context.Context, req *openfgapb.ReadRequest) (*openfgapb.ReadResponse, error) {
	store := req.GetStoreId()
	tk := req.GetTupleKey()
	ctx, span := s.tracer.Start(ctx, "read", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(store)},
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
	))
	defer span.End()

	modelID, err := s.resolveAuthorizationModelID(ctx, store, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.KeyValue{Key: "authorization-model-id", Value: attribute.StringValue(modelID)})

	q := commands.NewReadQuery(s.datastore, s.tracer, s.logger, s.encoder)
	return q.Execute(ctx, &openfgapb.ReadRequest{
		StoreId:              store,
		TupleKey:             tk,
		AuthorizationModelId: modelID,
		PageSize:             req.GetPageSize(),
		ContinuationToken:    req.GetContinuationToken(),
	})
}

func (s *Server) ReadTuples(ctx context.Context, req *openfgapb.ReadTuplesRequest) (*openfgapb.ReadTuplesResponse, error) {

	ctx, span := s.tracer.Start(ctx, "readTuples", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(req.GetStoreId())},
	))
	defer span.End()

	q := commands.NewReadTuplesQuery(s.datastore, s.encoder, s.logger)
	return q.Execute(ctx, req)
}

func (s *Server) Write(ctx context.Context, req *openfgapb.WriteRequest) (*openfgapb.WriteResponse, error) {
	store := req.GetStoreId()
	ctx, span := s.tracer.Start(ctx, "write", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(store)},
	))
	defer span.End()

	modelID, err := s.resolveAuthorizationModelID(ctx, store, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}

	cmd := commands.NewWriteCommand(s.datastore, s.tracer, s.logger)
	return cmd.Execute(ctx, &openfgapb.WriteRequest{
		StoreId:              store,
		AuthorizationModelId: modelID,
		Writes:               req.GetWrites(),
		Deletes:              req.GetDeletes(),
	})
}

func (s *Server) Check(ctx context.Context, req *openfgapb.CheckRequest) (*openfgapb.CheckResponse, error) {
	store := req.GetStoreId()
	tk := req.GetTupleKey()
	ctx, span := s.tracer.Start(ctx, "check", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(req.GetStoreId())},
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
	))
	defer span.End()

	modelID, err := s.resolveAuthorizationModelID(ctx, store, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.KeyValue{Key: "authorization-model-id", Value: attribute.StringValue(modelID)})

	q := commands.NewCheckQuery(s.datastore, s.tracer, s.meter, s.logger, s.config.ResolveNodeLimit)

	res, err := q.Execute(ctx, &openfgapb.CheckRequest{
		StoreId:              store,
		TupleKey:             tk,
		ContextualTuples:     req.GetContextualTuples(),
		AuthorizationModelId: modelID,
		Trace:                req.GetTrace(),
	})
	if err != nil {
		return nil, err
	}

	span.SetAttributes(attribute.KeyValue{Key: "allowed", Value: attribute.BoolValue(res.GetAllowed())})
	return res, nil
}

func (s *Server) Expand(ctx context.Context, req *openfgapb.ExpandRequest) (*openfgapb.ExpandResponse, error) {
	store := req.GetStoreId()
	tk := req.GetTupleKey()
	ctx, span := s.tracer.Start(ctx, "expand", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(store)},
		attribute.KeyValue{Key: "object", Value: attribute.StringValue(tk.GetObject())},
		attribute.KeyValue{Key: "relation", Value: attribute.StringValue(tk.GetRelation())},
		attribute.KeyValue{Key: "user", Value: attribute.StringValue(tk.GetUser())},
	))
	defer span.End()

	modelID, err := s.resolveAuthorizationModelID(ctx, store, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.KeyValue{Key: "authorization-model-id", Value: attribute.StringValue(modelID)})

	q := commands.NewExpandQuery(s.datastore, s.tracer, s.logger)
	return q.Execute(ctx, &openfgapb.ExpandRequest{
		StoreId:              store,
		AuthorizationModelId: modelID,
		TupleKey:             tk,
	})
}

func (s *Server) ReadAuthorizationModel(ctx context.Context, req *openfgapb.ReadAuthorizationModelRequest) (*openfgapb.ReadAuthorizationModelResponse, error) {
	ctx, span := s.tracer.Start(ctx, "readAuthorizationModel", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(req.GetStoreId())},
		attribute.KeyValue{Key: "authorization-model-id", Value: attribute.StringValue(req.GetId())},
	))
	defer span.End()

	q := commands.NewReadAuthorizationModelQuery(s.datastore, s.logger)
	return q.Execute(ctx, req)
}

func (s *Server) WriteAuthorizationModel(ctx context.Context, req *openfgapb.WriteAuthorizationModelRequest) (*openfgapb.WriteAuthorizationModelResponse, error) {
	ctx, span := s.tracer.Start(ctx, "writeAuthorizationModel", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(req.GetStoreId())},
	))
	defer span.End()

	c := commands.NewWriteAuthorizationModelCommand(s.datastore, s.logger)
	res, err := c.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusCreated))

	return res, nil
}

func (s *Server) ReadAuthorizationModels(ctx context.Context, req *openfgapb.ReadAuthorizationModelsRequest) (*openfgapb.ReadAuthorizationModelsResponse, error) {
	ctx, span := s.tracer.Start(ctx, "readAuthorizationModels", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(req.GetStoreId())},
	))
	defer span.End()

	c := commands.NewReadAuthorizationModelsQuery(s.datastore, s.encoder, s.logger)
	return c.Execute(ctx, req)
}

func (s *Server) WriteAssertions(ctx context.Context, req *openfgapb.WriteAssertionsRequest) (*openfgapb.WriteAssertionsResponse, error) {
	store := req.GetStoreId()
	ctx, span := s.tracer.Start(ctx, "writeAssertions", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(store)},
	))
	defer span.End()

	modelID, err := s.resolveAuthorizationModelID(ctx, store, req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.KeyValue{Key: "authorization-model-id", Value: attribute.StringValue(modelID)})

	c := commands.NewWriteAssertionsCommand(s.datastore, s.logger)
	res, err := c.Execute(ctx, &openfgapb.WriteAssertionsRequest{
		StoreId:              store,
		AuthorizationModelId: modelID,
		Assertions:           req.GetAssertions(),
	})
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusNoContent))

	return res, nil
}

func (s *Server) ReadAssertions(ctx context.Context, req *openfgapb.ReadAssertionsRequest) (*openfgapb.ReadAssertionsResponse, error) {
	ctx, span := s.tracer.Start(ctx, "readAssertions", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(req.GetStoreId())},
	))
	defer span.End()
	modelID, err := s.resolveAuthorizationModelID(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
	if err != nil {
		return nil, err
	}
	span.SetAttributes(attribute.KeyValue{Key: "authorization-model-id", Value: attribute.StringValue(modelID)})
	q := commands.NewReadAssertionsQuery(s.datastore, s.logger)
	return q.Execute(ctx, req.GetStoreId(), req.GetAuthorizationModelId())
}

func (s *Server) ReadChanges(ctx context.Context, req *openfgapb.ReadChangesRequest) (*openfgapb.ReadChangesResponse, error) {
	ctx, span := s.tracer.Start(ctx, "ReadChangesQuery", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(req.GetStoreId())},
		attribute.KeyValue{Key: "type", Value: attribute.StringValue(req.GetType())},
	))
	defer span.End()

	q := commands.NewReadChangesQuery(s.datastore, s.tracer, s.logger, s.encoder, s.config.ChangelogHorizonOffset)
	return q.Execute(ctx, req)
}

func (s *Server) CreateStore(ctx context.Context, req *openfgapb.CreateStoreRequest) (*openfgapb.CreateStoreResponse, error) {
	ctx, span := s.tracer.Start(ctx, "createStore")
	defer span.End()

	c := commands.NewCreateStoreCommand(s.datastore, s.logger)
	res, err := c.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusCreated))

	return res, nil
}

func (s *Server) DeleteStore(ctx context.Context, req *openfgapb.DeleteStoreRequest) (*openfgapb.DeleteStoreResponse, error) {
	ctx, span := s.tracer.Start(ctx, "deleteStore")
	defer span.End()

	cmd := commands.NewDeleteStoreCommand(s.datastore, s.logger)
	res, err := cmd.Execute(ctx, req)
	if err != nil {
		return nil, err
	}

	s.transport.SetHeader(ctx, httpmiddleware.XHttpCode, strconv.Itoa(http.StatusNoContent))

	return res, nil
}

func (s *Server) GetStore(ctx context.Context, req *openfgapb.GetStoreRequest) (*openfgapb.GetStoreResponse, error) {
	ctx, span := s.tracer.Start(ctx, "getStore", trace.WithAttributes(
		attribute.KeyValue{Key: "store", Value: attribute.StringValue(req.GetStoreId())},
	))
	defer span.End()

	q := commands.NewGetStoreQuery(s.datastore, s.logger)
	return q.Execute(ctx, req)
}

func (s *Server) ListStores(ctx context.Context, req *openfgapb.ListStoresRequest) (*openfgapb.ListStoresResponse, error) {
	ctx, span := s.tracer.Start(ctx, "listStores")
	defer span.End()

	q := commands.NewListStoresQuery(s.datastore, s.encoder, s.logger)
	return q.Execute(ctx, req)
}

// IsReady reports whether this OpenFGA server instance is ready to accept
// traffic.
func (s *Server) IsReady(ctx context.Context) (bool, error) {

	// for now we only depend on the datastore being ready, but in the future
	// server readiness may also depend on other criteria in addition to the
	// datastore being ready.
	return s.datastore.IsReady(ctx)
}

// Run starts server execution, and blocks until complete, returning any server errors. To close the
// server cancel the provided ctx.
func (s *Server) Run(ctx context.Context) error {

	interceptors := []grpc.UnaryServerInterceptor{
		grpc_validator.UnaryServerInterceptor(),
	}
	interceptors = append(interceptors, s.config.UnaryInterceptors...)

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(interceptors...),
	}

	if s.config.GRPCServer.TLSConfig != nil {
		creds, err := credentials.NewServerTLSFromFile(s.config.GRPCServer.TLSConfig.CertPath, s.config.GRPCServer.TLSConfig.KeyPath)
		if err != nil {
			return err
		}
		opts = append(opts, grpc.Creds(creds))
	}
	// nosemgrep: grpc-server-insecure-connection
	grpcServer := grpc.NewServer(opts...)
	openfgapb.RegisterOpenFGAServiceServer(grpcServer, s)
	healthv1pb.RegisterHealthServer(grpcServer, s.healthManager.GetHealthServer())
	reflection.Register(grpcServer)

	rpcAddr := s.config.GRPCServer.Addr
	lis, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			s.logger.Error("failed to start grpc server", logger.Error(err))
		}
	}()

	s.logger.Info(fmt.Sprintf("grpc server listening on '%s'...", rpcAddr))

	// Set a request timeout.
	runtime.DefaultContextTimeout = s.config.RequestTimeout

	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(otelgrpc.UnaryClientInterceptor()),
	}
	if s.config.GRPCServer.TLSConfig != nil {
		creds, err := credentials.NewClientTLSFromFile(s.config.GRPCServer.TLSConfig.CertPath, "")
		if err != nil {
			return err
		}
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
	} else {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(timeoutCtx, rpcAddr, dialOpts...)
	if err != nil {
		return err
	}
	defer conn.Close()

	healthClient := healthv1pb.NewHealthClient(conn)

	muxOpts := []runtime.ServeMuxOption{
		runtime.WithHealthzEndpoint(healthClient),
	}
	muxOpts = append(muxOpts, s.defaultServeMuxOpts...) // register the defaults first
	muxOpts = append(muxOpts, s.config.MuxOptions...)   // any provided options override defaults if they are duplicates

	mux := runtime.NewServeMux(muxOpts...)

	if err := openfgapb.RegisterOpenFGAServiceHandler(ctx, mux, conn); err != nil {
		return err
	}

	httpServer := &http.Server{
		Addr: s.config.HTTPServer.Addr,
		Handler: cors.New(cors.Options{
			AllowedOrigins:   s.config.HTTPServer.CORSAllowedOrigins,
			AllowCredentials: true,
			AllowedHeaders:   s.config.HTTPServer.CORSAllowedHeaders,
			AllowedMethods: []string{http.MethodGet, http.MethodPost,
				http.MethodHead, http.MethodPatch, http.MethodDelete, http.MethodPut},
		}).Handler(mux),
	}

	go func() {
		s.logger.Info(fmt.Sprintf("HTTP server listening on '%s'...", httpServer.Addr))

		var err error
		if s.config.HTTPServer.TLSConfig != nil {
			err = httpServer.ListenAndServeTLS(s.config.HTTPServer.TLSConfig.CertPath, s.config.HTTPServer.TLSConfig.KeyPath)
		} else {
			err = httpServer.ListenAndServe()
		}
		if err != http.ErrServerClosed {
			s.logger.ErrorWithContext(ctx, "HTTP server closed with unexpected error", logger.Error(err))
		}
	}()

	// start the health checks last to avoid a race with the HTTP server startup process
	go func() {
		if err := s.healthManager.Check(ctx)(); err != nil {
			s.logger.Fatal("server health checks failed", logger.Error(err))
		}
	}()

	<-ctx.Done()
	s.logger.InfoWithContext(ctx, "Termination signal received! Gracefully shutting down")

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		s.logger.ErrorWithContext(ctx, "HTTP server shutdown failed", logger.Error(err))
		return err
	}

	grpcServer.GracefulStop()

	return nil
}

// Util to find the latest authorization model ID to be used through all the request lifecycle.
// This allows caching of types. If the user inserts a new authorization model and doesn't
// provide this field (which should be rate limited more aggressively) the in-flight requests won't be
// affected and newer calls will use the updated authorization model.
func (s *Server) resolveAuthorizationModelID(ctx context.Context, store, modelID string) (string, error) {
	var err error

	if modelID != "" {
		if !id.IsValid(modelID) {
			return "", serverErrors.AuthorizationModelNotFound(modelID)
		}
	} else {
		if modelID, err = s.datastore.FindLatestAuthorizationModelID(ctx, store); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return "", serverErrors.LatestAuthorizationModelNotFound(store)
			}
			return "", serverErrors.HandleError("", err)
		}
	}

	s.transport.SetHeader(ctx, AuthorizationModelIDHeader, modelID)

	return modelID, nil
}

// serverHealthChecker implements the HealthCheckManager interface for an OpenFGA server
// specifically.
type serverHealthChecker struct {
	healthServer  *health.AuthlessHealthServer
	openfgaServer *Server
	serviceNames  map[string]struct{}
}

var _ health.HealthCheckManager = &serverHealthChecker{}

// NewOpenFGAServerHealthChecker constructs a HealthCheckManager that can be used
// to report the health status of the provided OpenFGA server.
func NewOpenFGAServerHealthChecker(s *Server) health.HealthCheckManager {
	return &serverHealthChecker{
		healthServer:  health.NewAuthlessHealthServer(),
		openfgaServer: s,
		serviceNames:  map[string]struct{}{},
	}
}

// RegisterService registers the provided serviceName with this server health checker and
// sets it's serving status to 'NOT SERVING'
func (s *serverHealthChecker) RegisterService(serviceName string) {
	s.serviceNames[serviceName] = struct{}{}
	s.healthServer.Server.SetServingStatus(serviceName, healthv1pb.HealthCheckResponse_NOT_SERVING)
}

// GetHealthServer returns the underlying health server managed by this health
// checker.
func (s *serverHealthChecker) GetHealthServer() *health.AuthlessHealthServer {
	return s.healthServer
}

// Check reports whether the server managed by this server health checker is
// ready to accept traffic.
func (s *serverHealthChecker) Check(ctx context.Context) func() error {
	return func() error {

		backoffPolicy := backoff.NewExponentialBackOff()
		backoffPolicy.MaxElapsedTime = 1 * time.Minute
		ticker := backoff.NewTicker(backoffPolicy)
		defer ticker.Stop()

		// continuously monitor health status on a ticker interval
		for {

			select {
			case _, ok := <-ticker.C:
				if !ok {
					return fmt.Errorf("server healthcheck deadline exceeded")
				}
			case <-ctx.Done():
				return nil
			}

			ready, err := s.openfgaServer.IsReady(ctx)
			if err != nil {
				s.openfgaServer.logger.Debug("server readiness check failed with an error", logger.Error(err))
			}

			if ready {
				for service := range s.serviceNames {
					s.healthServer.SetServingStatus(service, healthv1pb.HealthCheckResponse_SERVING)
				}

				return nil
			}
		}
	}
}
