// Package healthcheck contains the command that checks the health of a running OpenFGA server.
package healthcheck

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/netip"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/openfga/openfga/cmd/util"
	serverconfig "github.com/openfga/openfga/pkg/server/config"
)

const timeoutFlag = "timeout"

// NewHealthcheckCommand returns the command that checks the health of a running OpenFGA server.
func NewHealthcheckCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "healthcheck",
		Short: "Check the health of a running OpenFGA server",
		Long: `Check the health of a running OpenFGA server using the gRPC Health Checking Protocol.

The command exits with a non-zero status unless the server reports SERVING, so it can be used as a
container HEALTHCHECK or a Kubernetes exec probe. The gRPC address and TLS setting are read from the
same flags, OPENFGA_* environment variables and config file as the run command. An unspecified host
(such as 0.0.0.0) is checked on the loopback interface. When TLS is enabled the server certificate
is not verified, in the same way as Kubernetes HTTPS probes.`,
		Args:         cobra.NoArgs,
		SilenceUsage: true,
		RunE:         runHealthcheck,
	}

	defaultConfig := serverconfig.DefaultConfig()
	flags := cmd.Flags()

	flags.String("grpc-addr", defaultConfig.GRPC.Addr, "the host:port address of the gRPC server to check")
	flags.Bool("grpc-tls-enabled", defaultConfig.GRPC.TLS.Enabled, "connect to the gRPC server using TLS")
	flags.Duration(timeoutFlag, 5*time.Second, "the maximum time to wait for the check to complete")

	cmd.PreRun = bindFlagsFunc(flags)

	return cmd
}

// bindFlagsFunc binds the flags to the same config keys used by the run command.
func bindFlagsFunc(flags *pflag.FlagSet) func(*cobra.Command, []string) {
	return func(_ *cobra.Command, _ []string) {
		util.MustBindPFlag("grpc.addr", flags.Lookup("grpc-addr"))
		util.MustBindEnv("grpc.addr", "OPENFGA_GRPC_ADDR")

		util.MustBindPFlag("grpc.tls.enabled", flags.Lookup("grpc-tls-enabled"))
		util.MustBindEnv("grpc.tls.enabled", "OPENFGA_GRPC_TLS_ENABLED")
	}
}

func runHealthcheck(cmd *cobra.Command, _ []string) error {
	timeout, err := cmd.Flags().GetDuration(timeoutFlag)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(cmd.Context(), timeout)
	defer cancel()

	if err := check(ctx, viper.GetString("grpc.addr"), viper.GetBool("grpc.tls.enabled")); err != nil {
		return err
	}

	_, err = fmt.Fprintln(cmd.OutOrStdout(), "status: SERVING")
	return err
}

func check(ctx context.Context, addr string, tlsEnabled bool) error {
	target, err := dialTarget(addr)
	if err != nil {
		return err
	}

	creds := insecure.NewCredentials()
	if tlsEnabled {
		creds = credentials.NewTLS(&tls.Config{
			// The check only reads the health status of the server it is pointed at and sends no
			// credentials, so the server identity is not verified.
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
		})
	}

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(creds))
	if err != nil {
		return fmt.Errorf("failed to create gRPC client for %q: %w", target, err)
	}
	defer conn.Close()

	resp, err := healthv1pb.NewHealthClient(conn).Check(ctx, &healthv1pb.HealthCheckRequest{})
	if err != nil {
		return fmt.Errorf("health check against %q failed: %w", target, err)
	}

	if status := resp.GetStatus(); status != healthv1pb.HealthCheckResponse_SERVING {
		return fmt.Errorf("server at %q is not serving: %s", target, status)
	}

	return nil
}

// dialTarget returns the address to dial for the configured server address. An unspecified host,
// which the server uses to listen on all interfaces, is replaced by the loopback address.
func dialTarget(addr string) (string, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("invalid gRPC address %q: %w", addr, err)
	}

	if host == "" {
		host = "127.0.0.1"
	} else if ip, err := netip.ParseAddr(host); err == nil && ip.IsUnspecified() {
		host = "127.0.0.1"
		if ip.Is6() {
			host = "::1"
		}
	}

	return net.JoinHostPort(host, port), nil
}
