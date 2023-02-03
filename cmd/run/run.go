package run

import (
	"context"
	"errors"
	"fmt"
	"html/template"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	goruntime "runtime"
	"strings"
	"syscall"
	"time"

	"github.com/cenkalti/backoff/v4"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	grpc_validator "github.com/grpc-ecosystem/go-grpc-middleware/validator"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"github.com/openfga/openfga/assets"
	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/internal/authn"
	"github.com/openfga/openfga/internal/authn/oidc"
	"github.com/openfga/openfga/internal/authn/presharedkey"
	"github.com/openfga/openfga/internal/build"
	"github.com/openfga/openfga/internal/gateway"
	"github.com/openfga/openfga/internal/middleware"
	httpmiddleware "github.com/openfga/openfga/internal/middleware/http"
	"github.com/openfga/openfga/pkg/encoder"
	"github.com/openfga/openfga/pkg/logger"
	"github.com/openfga/openfga/pkg/server"
	serverErrors "github.com/openfga/openfga/pkg/server/errors"
	"github.com/openfga/openfga/pkg/server/health"
	"github.com/openfga/openfga/pkg/storage"
	"github.com/openfga/openfga/pkg/storage/caching"
	"github.com/openfga/openfga/pkg/storage/common"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/storage/mysql"
	"github.com/openfga/openfga/pkg/storage/postgres"
	"github.com/openfga/openfga/pkg/telemetry"
	"github.com/rs/cors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

func NewRunCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run the OpenFGA server",
		Long:  "Run the OpenFGA server.",
		Run:   run,
		Args:  cobra.NoArgs,
	}

	bindRunFlags(cmd)

	return cmd
}

// DatastoreConfig defines OpenFGA server configurations for datastore specific settings.
type DatastoreConfig struct {

	// Engine is the datastore engine to use (e.g. 'memory', 'postgres', 'mysql')
	Engine string
	URI    string

	// MaxCacheSize is the maximum number of cache keys that the storage cache can store before evicting
	// old keys. The storage cache is used to cache query results for various static resources
	// such as type definitions.
	MaxCacheSize int

	// MaxOpenConns is the maximum number of open connections to the database.
	MaxOpenConns int

	// MaxIdleConns is the maximum number of connections to the datastore in the idle connection pool.
	MaxIdleConns int

	// ConnMaxIdleTime is the maximum amount of time a connection to the datastore may be idle.
	ConnMaxIdleTime time.Duration

	// ConnMaxLifetime is the maximum amount of time a connection to the datastore may be reused.
	ConnMaxLifetime time.Duration
}

// GRPCConfig defines OpenFGA server configurations for grpc server specific settings.
type GRPCConfig struct {
	Addr string
	TLS  *TLSConfig
}

// HTTPConfig defines OpenFGA server configurations for HTTP server specific settings.
type HTTPConfig struct {
	Enabled bool
	Addr    string
	TLS     *TLSConfig

	// UpstreamTimeout is the timeout duration for proxying HTTP requests upstream
	// to the grpc endpoint. It cannot be smaller than Config.ListObjectsDeadline.
	UpstreamTimeout time.Duration

	CORSAllowedOrigins []string
	CORSAllowedHeaders []string
}

// TLSConfig defines configuration specific to Transport Layer Security (TLS) settings.
type TLSConfig struct {
	Enabled  bool
	CertPath string `mapstructure:"cert"`
	KeyPath  string `mapstructure:"key"`
}

// AuthnConfig defines OpenFGA server configurations for authentication specific settings.
type AuthnConfig struct {

	// Method is the authentication method that should be enforced (e.g. 'none', 'preshared', 'oidc')
	Method                   string
	*AuthnOIDCConfig         `mapstructure:"oidc"`
	*AuthnPresharedKeyConfig `mapstructure:"preshared"`
}

// AuthnOIDCConfig defines configurations for the 'oidc' method of authentication.
type AuthnOIDCConfig struct {
	Issuer   string
	Audience string
}

// AuthnPresharedKeyConfig defines configurations for the 'preshared' method of authentication.
type AuthnPresharedKeyConfig struct {
	// Keys define the preshared keys to verify authn tokens against.
	Keys []string
}

// LogConfig defines OpenFGA server configurations for log specific settings. For production we
// recommend using the 'json' log format.
type LogConfig struct {
	// Format is the log format to use in the log output (e.g. 'text' or 'json')
	Format string

	// Level is the log level to use in the log output (e.g. 'none', 'debug', or 'info')
	Level string
}

type TraceConfig struct {
	Enabled     bool
	OTLP        OTLPTraceConfig `mapstructure:"otlp"`
	SampleRatio float64
}

type OTLPTraceConfig struct {
	Endpoint string
}

// PlaygroundConfig defines OpenFGA server configurations for the Playground specific settings.
type PlaygroundConfig struct {
	Enabled bool
	Port    int
}

// ProfilerConfig defines server configurations specific to pprof profiling.
type ProfilerConfig struct {
	Enabled bool
	Addr    string
}

type OpenTelemetryMetricsConfig struct {
	Endpoint string
	Protocol string
}

// OpenTelemetryConfig defines configurations for OpenTelemetry telemetry settings.
type OpenTelemetryConfig struct {
	OpenTelemetryMetricsConfig `mapstructure:"metrics"`
}

type Config struct {
	// If you change any of these settings, please update the documentation at https://github.com/openfga/openfga.dev/blob/main/docs/content/intro/setup-openfga.mdx

	// ListObjectsDeadline defines the maximum amount of time to accumulate ListObjects results
	// before the server will respond. This is to protect the server from misuse of the
	// ListObjects endpoints. It cannot be larger than HTTPConfig.UpstreamTimeout.
	ListObjectsDeadline time.Duration

	// ListObjectsMaxResults defines the maximum number of ListObjects results to accumulate
	// before the server will respond. This is to protect the server from misuse of the
	// ListObjects endpoints.
	ListObjectsMaxResults uint32

	// MaxTuplesPerWrite defines the maximum number of tuples per Write endpoint.
	MaxTuplesPerWrite int

	// MaxTypesPerAuthorizationModel defines the maximum number of type definitions per authorization model for the WriteAuthorizationModel endpoint.
	MaxTypesPerAuthorizationModel int

	// ChangelogHorizonOffset is an offset in minutes from the current time. Changes that occur after this offset will not be included in the response of ReadChanges.
	ChangelogHorizonOffset int

	// Experimentals is a list of the experimental features to enable in the OpenFGA server.
	Experimentals []string

	// ResolveNodeLimit indicates how deeply nested an authorization model can be.
	ResolveNodeLimit uint32

	Datastore     DatastoreConfig
	GRPC          GRPCConfig
	HTTP          HTTPConfig
	Authn         AuthnConfig
	Log           LogConfig
	Trace         TraceConfig
	Playground    PlaygroundConfig
	Profiler      ProfilerConfig
	OpenTelemetry OpenTelemetryConfig `mapstructure:"otel"`
}

// DefaultConfig returns the OpenFGA server default configurations.
func DefaultConfig() *Config {
	return &Config{
		MaxTuplesPerWrite:             100,
		MaxTypesPerAuthorizationModel: 100,
		ChangelogHorizonOffset:        0,
		ResolveNodeLimit:              25,
		Experimentals:                 []string{},
		ListObjectsDeadline:           3 * time.Second, // there is a 3-second timeout elsewhere
		ListObjectsMaxResults:         1000,
		Datastore: DatastoreConfig{
			Engine:       "memory",
			MaxCacheSize: 100000,
			MaxIdleConns: 10,
			MaxOpenConns: 30,
		},
		GRPC: GRPCConfig{
			Addr: "0.0.0.0:8081",
			TLS:  &TLSConfig{Enabled: false},
		},
		HTTP: HTTPConfig{
			Enabled:            true,
			Addr:               "0.0.0.0:8080",
			TLS:                &TLSConfig{Enabled: false},
			UpstreamTimeout:    5 * time.Second,
			CORSAllowedOrigins: []string{"*"},
			CORSAllowedHeaders: []string{"*"},
		},
		Authn: AuthnConfig{
			Method:                  "none",
			AuthnPresharedKeyConfig: &AuthnPresharedKeyConfig{},
			AuthnOIDCConfig:         &AuthnOIDCConfig{},
		},
		Log: LogConfig{
			Format: "text",
			Level:  "info",
		},
		Trace: TraceConfig{
			Enabled: false,
			OTLP: OTLPTraceConfig{
				Endpoint: "0.0.0.0:4317",
			},
			SampleRatio: 0.2,
		},
		Playground: PlaygroundConfig{
			Enabled: true,
			Port:    3000,
		},
		Profiler: ProfilerConfig{
			Enabled: false,
			Addr:    ":3001",
		},
		OpenTelemetry: OpenTelemetryConfig{
			OpenTelemetryMetricsConfig: OpenTelemetryMetricsConfig{
				Protocol: "grpc",
				Endpoint: "0.0.0.0:4317",
			},
		},
	}
}

// MustDefaultConfigWithRandomPorts returns the DefaultConfig, but with random ports for the grpc and http addresses.
// This function may panic if somehow a random port cannot be chosen.
func MustDefaultConfigWithRandomPorts() *Config {
	config := DefaultConfig()

	// Since this is used for tests, turn the following off:
	config.Playground.Enabled = false

	l, err := net.Listen("tcp", "")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	httpPort := l.Addr().(*net.TCPAddr).Port

	l, err = net.Listen("tcp", "")
	if err != nil {
		panic(err)
	}
	defer l.Close()
	grpcPort := l.Addr().(*net.TCPAddr).Port

	config.GRPC.Addr = fmt.Sprintf("0.0.0.0:%d", grpcPort)
	config.HTTP.Addr = fmt.Sprintf("0.0.0.0:%d", httpPort)

	return config
}

// ReadConfig returns the OpenFGA server configuration based on the values provided in the server's 'config.yaml' file.
// The 'config.yaml' file is loaded from '/etc/openfga', '$HOME/.openfga', or the current working directory. If no configuration
// file is present, the default values are returned.
func ReadConfig() (*Config, error) {
	config := DefaultConfig()

	viper.SetConfigName("config")
	viper.SetConfigType("yaml")

	configPaths := []string{"/etc/openfga", "$HOME/.openfga", "."}
	for _, path := range configPaths {
		viper.AddConfigPath(path)
	}

	err := viper.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); !ok {
			return nil, fmt.Errorf("failed to load server config: %w", err)
		}
	}

	if err := viper.Unmarshal(config); err != nil {
		return nil, fmt.Errorf("failed to unmarshal server config: %w", err)
	}

	return config, nil
}

func VerifyConfig(cfg *Config) error {
	if cfg.ListObjectsDeadline > cfg.HTTP.UpstreamTimeout {
		return fmt.Errorf("config 'http.upstreamTimeout' (%s) cannot be lower than 'listObjectsDeadline' config (%s)", cfg.HTTP.UpstreamTimeout, cfg.ListObjectsDeadline)
	}

	if cfg.Log.Format != "text" && cfg.Log.Format != "json" {
		return fmt.Errorf("config 'log.format' must be one of ['text', 'json']")
	}

	if cfg.Log.Level != "none" &&
		cfg.Log.Level != "debug" &&
		cfg.Log.Level != "info" &&
		cfg.Log.Level != "warn" &&
		cfg.Log.Level != "error" &&
		cfg.Log.Level != "panic" &&
		cfg.Log.Level != "fatal" {
		return fmt.Errorf("config 'log.level' must be one of ['none', 'debug', 'info', 'warn', 'error', 'panic', 'fatal']")
	}

	if cfg.Playground.Enabled {
		if !cfg.HTTP.Enabled {
			return errors.New("the HTTP server must be enabled to run the openfga playground")
		}

		if !(cfg.Authn.Method == "none" || cfg.Authn.Method == "preshared") {
			return errors.New("the playground only supports authn methods 'none' and 'preshared'")
		}
	}

	if cfg.HTTP.TLS.Enabled {
		if cfg.HTTP.TLS.CertPath == "" || cfg.HTTP.TLS.KeyPath == "" {
			return errors.New("'http.tls.cert' and 'http.tls.key' configs must be set")
		}
	}

	if cfg.GRPC.TLS.Enabled {
		if cfg.GRPC.TLS.CertPath == "" || cfg.GRPC.TLS.KeyPath == "" {
			return errors.New("'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
		}
	}

	return nil
}

func run(_ *cobra.Command, _ []string) {
	config, err := ReadConfig()
	if err != nil {
		panic(err)
	}

	if err := RunServer(context.Background(), config); err != nil {
		panic(err)
	}
}

func RunServer(ctx context.Context, config *Config) error {
	if err := VerifyConfig(config); err != nil {
		return err
	}

	logger := logger.MustNewLogger(config.Log.Format, config.Log.Level)

	tp := sdktrace.NewTracerProvider()
	if config.Trace.Enabled {
		tp = telemetry.MustNewTracerProvider(config.Trace.OTLP.Endpoint, config.Trace.SampleRatio)
	}

	logger.Info(fmt.Sprintf("🧪 experimental features enabled: %v", config.Experimentals))

	var experimentals []server.ExperimentalFeatureFlag
	for _, feature := range config.Experimentals {
		experimentals = append(experimentals, server.ExperimentalFeatureFlag(feature))
	}

	meter := metric.NewNoopMeter()
	var err error
	if util.Contains(config.Experimentals, "otel-metrics") {

		protocol := config.OpenTelemetry.Protocol
		endpoint := config.OpenTelemetry.Endpoint

		logger.Info(fmt.Sprintf("🕵 OpenTelemetry 'otlp' metrics exported to '%s' via protocol '%s'", endpoint, protocol))

		meter, err = telemetry.NewOTLPMeter(ctx, logger, protocol, endpoint)
		if err != nil {
			return fmt.Errorf("failed to initialize otlp metrics meter: %w", err)
		}
	}

	dsCfg := common.NewConfig(
		common.WithLogger(logger),
		common.WithMaxTuplesPerWrite(config.MaxTuplesPerWrite),
		common.WithMaxTypesPerAuthorizationModel(config.MaxTypesPerAuthorizationModel),
		common.WithMaxOpenConns(config.Datastore.MaxOpenConns),
		common.WithMaxIdleConns(config.Datastore.MaxIdleConns),
		common.WithConnMaxIdleTime(config.Datastore.ConnMaxIdleTime),
		common.WithConnMaxLifetime(config.Datastore.ConnMaxLifetime),
	)

	var datastore storage.OpenFGADatastore
	switch config.Datastore.Engine {
	case "memory":
		datastore = memory.New(config.MaxTuplesPerWrite, config.MaxTypesPerAuthorizationModel)
	case "mysql":
		datastore, err = mysql.New(config.Datastore.URI, dsCfg)
		if err != nil {
			return fmt.Errorf("failed to initialize mysql datastore: %w", err)
		}
	case "postgres":
		datastore, err = postgres.New(config.Datastore.URI, dsCfg)
		if err != nil {
			return fmt.Errorf("failed to initialize postgres datastore: %w", err)
		}
	default:
		return fmt.Errorf("storage engine '%s' is unsupported", config.Datastore.Engine)
	}
	datastore = caching.NewCachedOpenFGADatastore(storage.NewContextWrapper(datastore), config.Datastore.MaxCacheSize)

	logger.Info(fmt.Sprintf("using '%v' storage engine", config.Datastore.Engine))

	var authenticator authn.Authenticator
	switch config.Authn.Method {
	case "none":
		logger.Warn("authentication is disabled")
		authenticator = authn.NoopAuthenticator{}
	case "preshared":
		logger.Info("using 'preshared' authentication")
		authenticator, err = presharedkey.NewPresharedKeyAuthenticator(config.Authn.Keys)
	case "oidc":
		logger.Info("using 'oidc' authentication")
		authenticator, err = oidc.NewRemoteOidcAuthenticator(config.Authn.Issuer, config.Authn.Audience)
	default:
		return fmt.Errorf("unsupported authentication method '%v'", config.Authn.Method)
	}
	if err != nil {
		return fmt.Errorf("failed to initialize authenticator: %w", err)
	}

	unaryInterceptors := []grpc.UnaryServerInterceptor{
		grpc_validator.UnaryServerInterceptor(),
		otelgrpc.UnaryServerInterceptor(otelgrpc.WithTracerProvider(tp)),
		middleware.NewRequestIDInterceptor(logger),
		middleware.NewStoreIDInterceptor(),
		middleware.NewLoggingInterceptor(logger),
		grpc_auth.UnaryServerInterceptor(middleware.AuthFunc(authenticator)),
	}

	streamingInterceptors := []grpc.StreamServerInterceptor{
		grpc_validator.StreamServerInterceptor(),
		otelgrpc.StreamServerInterceptor(otelgrpc.WithTracerProvider(tp)),
		middleware.NewStreamingRequestIDInterceptor(logger),
		middleware.NewStreamingStoreIDInterceptor(),
		middleware.NewStreamingLoggingInterceptor(logger),
		grpc_auth.StreamServerInterceptor(middleware.AuthFunc(authenticator)),
	}

	opts := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(unaryInterceptors...),
		grpc.ChainStreamInterceptor(streamingInterceptors...),
	}

	if config.GRPC.TLS.Enabled {
		if config.GRPC.TLS.CertPath == "" || config.GRPC.TLS.KeyPath == "" {
			return errors.New("'grpc.tls.cert' and 'grpc.tls.key' configs must be set")
		}
		creds, err := credentials.NewServerTLSFromFile(config.GRPC.TLS.CertPath, config.GRPC.TLS.KeyPath)
		if err != nil {
			return err
		}

		opts = append(opts, grpc.Creds(creds))

		logger.Info("grpc TLS is enabled, serving connections using the provided certificate")
	} else {
		logger.Warn("grpc TLS is disabled, serving connections using insecure plaintext")
	}

	if config.Profiler.Enabled {
		mux := http.NewServeMux()
		mux.HandleFunc("/debug/pprof/", pprof.Index)
		mux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
		mux.HandleFunc("/debug/pprof/profile", pprof.Profile)
		mux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
		mux.HandleFunc("/debug/pprof/trace", pprof.Trace)

		go func() {
			logger.Info(fmt.Sprintf("🔬 starting pprof profiler on '%s'", config.Profiler.Addr))

			if err := http.ListenAndServe(config.Profiler.Addr, mux); err != nil {
				if err != http.ErrServerClosed {
					logger.Fatal("failed to start pprof profiler", zap.Error(err))
				}
			}
		}()
	}

	svr := server.New(&server.Dependencies{
		Datastore:    datastore,
		Logger:       logger,
		Meter:        meter,
		TokenEncoder: encoder.NewBase64Encoder(),
		Transport:    gateway.NewRPCTransport(logger),
	}, &server.Config{
		ResolveNodeLimit:       config.ResolveNodeLimit,
		ChangelogHorizonOffset: config.ChangelogHorizonOffset,
		ListObjectsDeadline:    config.ListObjectsDeadline,
		ListObjectsMaxResults:  config.ListObjectsMaxResults,
		Experimentals:          experimentals,
	})

	logger.Info(
		"🚀 starting openfga service...",
		zap.String("version", build.Version),
		zap.String("date", build.Date),
		zap.String("commit", build.Commit),
		zap.String("go-version", goruntime.Version()),
	)

	// nosemgrep: grpc-server-insecure-connection
	grpcServer := grpc.NewServer(opts...)
	openfgapb.RegisterOpenFGAServiceServer(grpcServer, svr)
	healthServer := &health.Checker{TargetService: svr, TargetServiceName: openfgapb.OpenFGAService_ServiceDesc.ServiceName}
	healthv1pb.RegisterHealthServer(grpcServer, healthServer)
	reflection.Register(grpcServer)

	lis, err := net.Listen("tcp", config.GRPC.Addr)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			if !errors.Is(err, grpc.ErrServerStopped) {
				logger.Fatal("failed to start grpc server", zap.Error(err))
			}

			logger.Info("grpc server shut down..")
		}
	}()
	logger.Info(fmt.Sprintf("grpc server listening on '%s'...", config.GRPC.Addr))

	var httpServer *http.Server
	if config.HTTP.Enabled {
		// Set a request timeout.
		runtime.DefaultContextTimeout = config.HTTP.UpstreamTimeout

		dialOpts := []grpc.DialOption{
			grpc.WithBlock(),
		}
		if config.GRPC.TLS.Enabled {
			creds, err := credentials.NewClientTLSFromFile(config.GRPC.TLS.CertPath, "")
			if err != nil {
				logger.Fatal("", zap.Error(err))
			}
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(creds))
		} else {
			dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		}

		timeoutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		conn, err := grpc.DialContext(timeoutCtx, config.GRPC.Addr, dialOpts...)
		if err != nil {
			logger.Fatal("", zap.Error(err))
		}
		defer conn.Close()

		muxOpts := []runtime.ServeMuxOption{
			runtime.WithForwardResponseOption(httpmiddleware.HTTPResponseModifier),
			runtime.WithErrorHandler(func(c context.Context, sr *runtime.ServeMux, mm runtime.Marshaler, w http.ResponseWriter, r *http.Request, e error) {
				intCode := serverErrors.ConvertToEncodedErrorCode(status.Convert(e))
				httpmiddleware.CustomHTTPErrorHandler(c, w, r, serverErrors.NewEncodedError(intCode, e.Error()))
			}),
			runtime.WithStreamErrorHandler(func(ctx context.Context, e error) *status.Status {
				intCode := serverErrors.ConvertToEncodedErrorCode(status.Convert(e))
				encodedErr := serverErrors.NewEncodedError(intCode, e.Error())
				return status.Convert(&encodedErr)
			}),
			runtime.WithHealthzEndpoint(healthv1pb.NewHealthClient(conn)),
		}
		mux := runtime.NewServeMux(muxOpts...)
		if err := openfgapb.RegisterOpenFGAServiceHandler(ctx, mux, conn); err != nil {
			return err
		}

		httpServer = &http.Server{
			Addr: config.HTTP.Addr,
			Handler: cors.New(cors.Options{
				AllowedOrigins:   config.HTTP.CORSAllowedOrigins,
				AllowCredentials: true,
				AllowedHeaders:   config.HTTP.CORSAllowedHeaders,
				AllowedMethods: []string{http.MethodGet, http.MethodPost,
					http.MethodHead, http.MethodPatch, http.MethodDelete, http.MethodPut},
			}).Handler(mux),
		}

		go func() {
			var err error
			if config.HTTP.TLS.Enabled {
				if config.HTTP.TLS.CertPath == "" || config.HTTP.TLS.KeyPath == "" {
					logger.Fatal("'http.tls.cert' and 'http.tls.key' configs must be set")
				}
				err = httpServer.ListenAndServeTLS(config.HTTP.TLS.CertPath, config.HTTP.TLS.KeyPath)
			} else {
				err = httpServer.ListenAndServe()
			}
			if err != http.ErrServerClosed {
				logger.Fatal("HTTP server closed with unexpected error", zap.Error(err))
			}
		}()
		logger.Info(fmt.Sprintf("HTTP server listening on '%s'...", httpServer.Addr))
	}

	var playground *http.Server
	if config.Playground.Enabled {
		if !config.HTTP.Enabled {
			return errors.New("the HTTP server must be enabled to run the openfga playground")
		}

		authMethod := config.Authn.Method
		if !(authMethod == "none" || authMethod == "preshared") {
			return errors.New("the playground only supports authn methods 'none' and 'preshared'")
		}

		playgroundAddr := fmt.Sprintf(":%d", config.Playground.Port)
		logger.Info(fmt.Sprintf("🛝 starting openfga playground on http://localhost%s/playground", playgroundAddr))

		tmpl, err := template.ParseFS(assets.EmbedPlayground, "playground/index.html")
		if err != nil {
			return fmt.Errorf("failed to parse playground index.html as Go template: %w", err)
		}

		fileServer := http.FileServer(http.FS(assets.EmbedPlayground))

		policy := backoff.NewExponentialBackOff()
		policy.MaxElapsedTime = 3 * time.Second

		var conn net.Conn
		err = backoff.Retry(
			func() error {
				conn, err = net.Dial("tcp", config.HTTP.Addr)
				return err
			},
			policy,
		)
		if err != nil {
			return fmt.Errorf("failed to establish playground connection to HTTP server: %w", err)
		}

		playgroundAPIToken := ""
		if authMethod == "preshared" {
			playgroundAPIToken = config.Authn.AuthnPresharedKeyConfig.Keys[0]
		}

		mux := http.NewServeMux()
		mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			if strings.HasPrefix(r.URL.Path, "/playground") {
				if r.URL.Path == "/playground" || r.URL.Path == "/playground/index.html" {
					err = tmpl.Execute(w, struct {
						HTTPServerURL      string
						PlaygroundAPIToken string
					}{
						HTTPServerURL:      conn.RemoteAddr().String(),
						PlaygroundAPIToken: playgroundAPIToken,
					})
					if err != nil {
						w.WriteHeader(http.StatusInternalServerError)
						logger.Error("failed to execute/render the playground web template", zap.Error(err))
					}

					return
				}

				fileServer.ServeHTTP(w, r)
				return
			}

			http.NotFound(w, r)
		}))

		playground = &http.Server{Addr: playgroundAddr, Handler: mux}

		go func() {
			err = playground.ListenAndServe()
			if err != http.ErrServerClosed {
				logger.Fatal("failed to start the openfga playground server", zap.Error(err))
			}
			logger.Info("shutdown the openfga playground server")
		}()
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-done:
	case <-ctx.Done():
	}
	logger.Info("attempting to shutdown gracefully")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if playground != nil {
		if err := playground.Shutdown(ctx); err != nil {
			logger.Info("failed to gracefully shutdown playground server", zap.Error(err))
		}
	}

	if httpServer != nil {
		if err := httpServer.Shutdown(ctx); err != nil {
			logger.Info("failed to shutdown the http server", zap.Error(err))
		}
	}

	grpcServer.GracefulStop()

	authenticator.Close()

	datastore.Close()

	_ = tp.ForceFlush(ctx)
	_ = tp.Shutdown(ctx)

	logger.Info("server exited. goodbye 👋")

	return nil
}
