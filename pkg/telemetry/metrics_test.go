package telemetry

import (
	"context"
	"fmt"
	"net"
	"os"
	"path"
	"runtime"
	"testing"

	"github.com/openfga/openfga/pkg/logger"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	_, filename, _, _ := runtime.Caller(0)
	dir := path.Join(path.Dir(filename), "../../..")
	err := os.Chdir(dir)
	if err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func availableListeningAddress() string {
	listener, err := net.Listen("tcp", "")
	if err != nil {
		panic(err)
	}
	defer listener.Close()
	port := listener.Addr().(*net.TCPAddr).Port
	return fmt.Sprintf("%s:%d", "0.0.0.0", port)
}

func TestNewOTLPMeterWithHttp(t *testing.T) {
	meter, err := NewOTLPMeter(logger.NewNoopLogger(), context.Background(), "http", "localhost:4318")
	require.NoError(t, err)
	require.NotNil(t, meter)
}

func TestNewOTLPMeterWithGrpc(t *testing.T) {
	meter, err := NewOTLPMeter(logger.NewNoopLogger(), context.Background(), "grpc", "localhost:4317")
	require.NoError(t, err)
	require.NotNil(t, meter)
}

func TestNewOTLPMeterUnknownProtocol(t *testing.T) {
	_, err := NewOTLPMeter(logger.NewNoopLogger(), context.Background(), "unknown", "localhost:4317")
	require.Error(t, err)
}
