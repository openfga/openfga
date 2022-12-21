package telemetry

import (
	"context"
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

func TestNewOTLPMeterWithHTTP(t *testing.T) {
	meter, err := NewOTLPMeter(context.Background(), logger.NewNoopLogger(), "http", "localhost:4318")
	require.NoError(t, err)
	require.NotNil(t, meter)
}

func TestNewOTLPMeterWithGRGPC(t *testing.T) {
	meter, err := NewOTLPMeter(context.Background(), logger.NewNoopLogger(), "grpc", "localhost:4317")
	require.NoError(t, err)
	require.NotNil(t, meter)
}

func TestNewOTLPMeterUnknownProtocol(t *testing.T) {
	_, err := NewOTLPMeter(context.Background(), logger.NewNoopLogger(), "unknown", "localhost:4317")
	require.Error(t, err)
}
