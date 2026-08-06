// Package healthcheck contains the command to probe the health of a running
// OpenFGA server. It is a self-contained replacement for the external
// grpc_health_probe binary that used to be bundled in the released image.
package healthcheck

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/openfga/openfga/cmd/util"
	"github.com/openfga/openfga/pkg/server/config"
)

const (
	targetFlag  = "target"
	serviceFlag = "service"
	timeoutFlag = "timeout"

	targetGRPC = "grpc"
	targetHTTP = "http"
)

// NewHealthCheckCommand returns a cobra command that probes the health of an
// OpenFGA server. It reads the same configuration (flags, config file, and
// OPENFGA_* environment variables) as the `run` command, so a probe running in
// the same container as the server needs no extra configuration to match the
// server's address and TLS settings.
func NewHealthCheckCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "healthcheck",
		Short: "Probe the health of a running OpenFGA server",
		Long: `The healthcheck command probes the health of a running OpenFGA server via the ` +
			`gRPC Health Checking Protocol or the HTTP /healthz endpoint. It exits 0 when the ` +
			`server reports SERVING and non-zero otherwise, which makes it suitable for a ` +
			`container HEALTHCHECK or a Kubernetes exec probe. Configuration is read from the ` +
			`same flags, config file, and OPENFGA_* environment variables as the run command.`,
		RunE: runHealthCheck,
		Args: cobra.NoArgs,
	}

	flags := cmd.Flags()
	defaultConfig := config.DefaultConfig()

	flags.String(targetFlag, targetGRPC, "the server endpoint to probe: 'grpc' or 'http'")
	flags.String(serviceFlag, "", "the gRPC health service name to check (empty checks overall server health)")
	flags.Duration(timeoutFlag, 5*time.Second, "the maximum time to wait for the health check to complete")

	// Mirror the run command's address and TLS flags so the probe can be
	// pointed at the same endpoints. These bind to the same viper keys the
	// server uses, so a config file or OPENFGA_* env vars apply unchanged.
	flags.String("grpc-addr", defaultConfig.GRPC.Addr, "the host:port address of the grpc server to probe")
	flags.Bool("grpc-tls-enabled", defaultConfig.GRPC.TLS.Enabled, "whether the grpc server is served over TLS")
	flags.String("grpc-tls-cert", defaultConfig.GRPC.TLS.CertPath, "the (absolute) file path of the certificate the grpc server presents, used to verify it")

	flags.String("http-addr", defaultConfig.HTTP.Addr, "the host:port address of the HTTP server to probe")
	flags.Bool("http-tls-enabled", defaultConfig.HTTP.TLS.Enabled, "whether the HTTP server is served over TLS")
	flags.String("http-tls-cert", defaultConfig.HTTP.TLS.CertPath, "the (absolute) file path of the certificate the HTTP server presents, used to verify it")

	cmd.PreRun = bindHealthCheckFlagsFunc(flags)

	return cmd
}

// bindHealthCheckFlagsFunc binds the cobra flags to the viper keys the server
// already uses, bridging CLI flags, config file, and environment variables.
func bindHealthCheckFlagsFunc(flags *pflag.FlagSet) func(*cobra.Command, []string) {
	return func(_ *cobra.Command, _ []string) {
		util.MustBindPFlag(targetFlag, flags.Lookup(targetFlag))
		util.MustBindEnv(targetFlag, "OPENFGA_HEALTHCHECK_TARGET")

		util.MustBindPFlag(serviceFlag, flags.Lookup(serviceFlag))
		util.MustBindEnv(serviceFlag, "OPENFGA_HEALTHCHECK_SERVICE")

		util.MustBindPFlag(timeoutFlag, flags.Lookup(timeoutFlag))
		util.MustBindEnv(timeoutFlag, "OPENFGA_HEALTHCHECK_TIMEOUT")

		util.MustBindPFlag("grpc.addr", flags.Lookup("grpc-addr"))
		util.MustBindEnv("grpc.addr", "OPENFGA_GRPC_ADDR")

		util.MustBindPFlag("grpc.tls.enabled", flags.Lookup("grpc-tls-enabled"))
		util.MustBindEnv("grpc.tls.enabled", "OPENFGA_GRPC_TLS_ENABLED")

		util.MustBindPFlag("grpc.tls.cert", flags.Lookup("grpc-tls-cert"))
		util.MustBindEnv("grpc.tls.cert", "OPENFGA_GRPC_TLS_CERT")

		util.MustBindPFlag("http.addr", flags.Lookup("http-addr"))
		util.MustBindEnv("http.addr", "OPENFGA_HTTP_ADDR")

		util.MustBindPFlag("http.tls.enabled", flags.Lookup("http-tls-enabled"))
		util.MustBindEnv("http.tls.enabled", "OPENFGA_HTTP_TLS_ENABLED")

		util.MustBindPFlag("http.tls.cert", flags.Lookup("http-tls-cert"))
		util.MustBindEnv("http.tls.cert", "OPENFGA_HTTP_TLS_CERT")
	}
}

// options holds the resolved configuration for a single health check probe.
type options struct {
	addr       string
	tlsEnabled bool
	tlsCert    string
	service    string
}

func runHealthCheck(cmd *cobra.Command, _ []string) error {
	ctx, cancel := context.WithTimeout(cmd.Context(), viper.GetDuration(timeoutFlag))
	defer cancel()

	target := viper.GetString(targetFlag)
	switch target {
	case targetGRPC:
		return checkGRPC(ctx, options{
			addr:       viper.GetString("grpc.addr"),
			tlsEnabled: viper.GetBool("grpc.tls.enabled"),
			tlsCert:    viper.GetString("grpc.tls.cert"),
			service:    viper.GetString(serviceFlag),
		})
	case targetHTTP:
		return checkHTTP(ctx, options{
			addr:       viper.GetString("http.addr"),
			tlsEnabled: viper.GetBool("http.tls.enabled"),
			tlsCert:    viper.GetString("http.tls.cert"),
		})
	default:
		return fmt.Errorf("invalid --target %q: must be %q or %q", target, targetGRPC, targetHTTP)
	}
}

// checkGRPC probes the server's gRPC Health/Check RPC. When TLS is enabled it
// verifies the server against the configured certificate, using the host from
// the address as the expected server name.
func checkGRPC(ctx context.Context, opts options) error {
	var creds credentials.TransportCredentials
	if opts.tlsEnabled {
		host, _, err := net.SplitHostPort(opts.addr)
		if err != nil {
			return fmt.Errorf("invalid grpc address %q: %w", opts.addr, err)
		}

		tlsConfig, err := tlsConfigFromCert(opts.tlsCert, host)
		if err != nil {
			return err
		}
		creds = credentials.NewTLS(tlsConfig)
	} else {
		creds = insecure.NewCredentials()
	}

	conn, err := grpc.NewClient(opts.addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("failed to create grpc client for %q: %w", opts.addr, err)
	}
	defer conn.Close()

	resp, err := healthv1pb.NewHealthClient(conn).Check(ctx, &healthv1pb.HealthCheckRequest{
		Service: opts.service,
	})
	if err != nil {
		return fmt.Errorf("grpc health check failed: %w", err)
	}

	if resp.GetStatus() != healthv1pb.HealthCheckResponse_SERVING {
		return fmt.Errorf("server is not serving: status %s", resp.GetStatus())
	}

	return nil
}

// checkHTTP probes the server's HTTP /healthz endpoint, which the gateway
// proxies to the same gRPC Health/Check RPC. A non-2xx response means the
// server is not serving.
func checkHTTP(ctx context.Context, opts options) error {
	scheme := "http"
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if opts.tlsEnabled {
		scheme = "https"

		host, _, err := net.SplitHostPort(opts.addr)
		if err != nil {
			return fmt.Errorf("invalid http address %q: %w", opts.addr, err)
		}

		tlsConfig, err := tlsConfigFromCert(opts.tlsCert, host)
		if err != nil {
			return err
		}
		transport.TLSClientConfig = tlsConfig
	}

	url := fmt.Sprintf("%s://%s/healthz", scheme, opts.addr)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to build health check request: %w", err)
	}

	client := &http.Client{Transport: transport}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http health check failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("server is not serving: HTTP %d", resp.StatusCode)
	}

	return nil
}

// tlsConfigFromCert builds a *tls.Config that verifies the server's certificate
// against the PEM certificate at certPath, expecting serverName as the host.
func tlsConfigFromCert(certPath, serverName string) (*tls.Config, error) {
	if certPath == "" {
		return nil, errors.New("TLS is enabled but no certificate path was provided")
	}

	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read TLS certificate %q: %w", certPath, err)
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		return nil, fmt.Errorf("failed to parse TLS certificate %q", certPath)
	}

	return &tls.Config{
		RootCAs:    pool,
		ServerName: serverName,
		MinVersion: tls.VersionTLS12,
	}, nil
}
