// Package testutils contains code that is useful in tests.
package testutils

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/go-cmp/cmp"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/tuple"
)

const (
	AllChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var PostgresImages = []string{"postgres14", "postgres17"}

var (
	TupleCmpTransformer = cmp.Transformer("Sort", func(in []*openfgav1.Tuple) []*openfgav1.Tuple {
		out := append([]*openfgav1.Tuple(nil), in...) // Copy input to avoid mutating it

		sort.SliceStable(out, func(i, j int) bool {
			if out[i].GetKey().GetObject() != out[j].GetKey().GetObject() {
				return out[i].GetKey().GetObject() < out[j].GetKey().GetObject()
			}

			if out[i].GetKey().GetRelation() != out[j].GetKey().GetRelation() {
				return out[i].GetKey().GetRelation() < out[j].GetKey().GetRelation()
			}

			if out[i].GetKey().GetUser() != out[j].GetKey().GetUser() {
				return out[i].GetKey().GetUser() < out[j].GetKey().GetUser()
			}

			return true
		})

		return out
	})
	TupleKeyCmpTransformer = cmp.Transformer("Sort", func(in []*openfgav1.TupleKey) []*openfgav1.TupleKey {
		out := append([]*openfgav1.TupleKey(nil), in...) // Copy input to avoid mutating it

		sort.SliceStable(out, func(i, j int) bool {
			if out[i].GetObject() != out[j].GetObject() {
				return out[i].GetObject() < out[j].GetObject()
			}

			if out[i].GetRelation() != out[j].GetRelation() {
				return out[i].GetRelation() < out[j].GetRelation()
			}

			if out[i].GetUser() != out[j].GetUser() {
				return out[i].GetUser() < out[j].GetUser()
			}

			return true
		})

		return out
	})
)

func ConvertTuplesToTupleKeys(input []*openfgav1.Tuple) []*openfgav1.TupleKey {
	converted := make([]*openfgav1.TupleKey, len(input))
	for i := range input {
		converted[i] = input[i].GetKey()
	}
	return converted
}

func ConvertTuplesKeysToTuples(input []*openfgav1.TupleKey) []*openfgav1.Tuple {
	converted := make([]*openfgav1.Tuple, len(input))
	for i := range input {
		converted[i] = &openfgav1.Tuple{Key: tuple.NewTupleKey(input[i].GetObject(), input[i].GetRelation(), input[i].GetUser())}
	}
	return converted
}

// Shuffle returns the input but with order of elements randomized.
func Shuffle(arr []*openfgav1.TupleKey) []*openfgav1.TupleKey {
	// copy array to avoid data races :(
	copied := make([]*openfgav1.TupleKey, len(arr))
	for i := range arr {
		copied[i] = tuple.NewTupleKeyWithCondition(arr[i].GetObject(),
			arr[i].GetRelation(),
			arr[i].GetUser(),
			arr[i].GetCondition().GetName(),
			arr[i].GetCondition().GetContext(),
		)
	}
	rand.Shuffle(len(copied), func(i, j int) {
		copied[i], copied[j] = copied[j], copied[i]
	})
	return copied
}

func CreateRandomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = AllChars[rand.Intn(len(AllChars))]
	}
	return string(b)
}

func MustNewStruct(t require.TestingT, v map[string]interface{}) *structpb.Struct {
	conditionContext, err := structpb.NewStruct(v)
	require.NoError(t, err)
	return conditionContext
}

// MakeSliceWithGenerator generates a slice of length 'n' and populates the contents
// with values based on the generator provided.
func MakeSliceWithGenerator[T any](n uint64, generator func(n uint64) any) []T {
	s := make([]T, 0, n)

	for i := uint64(0); i < n; i++ {
		s = append(s, generator(i).(T))
	}

	return s
}

// NumericalStringGenerator generates a string representation of the provided
// uint value.
func NumericalStringGenerator(n uint64) any {
	return strconv.FormatUint(n, 10)
}

func MakeStringWithRuneset(n uint64, runeSet []rune) string {
	var s string
	for i := uint64(0); i < n; i++ {
		s += string(runeSet[rand.Intn(len(runeSet))])
	}

	return s
}

// MustTransformDSLToProtoWithID interprets the provided string s as an FGA model and
// attempts to parse it using the official OpenFGA language parser. The model returned
// includes an auto-generated model id which assists with producing models for testing
// purposes.
func MustTransformDSLToProtoWithID(s string) *openfgav1.AuthorizationModel {
	model := parser.MustTransformDSLToProto(s)
	model.Id = ulid.Make().String()

	return model
}

// CreateGrpcConnection creates a grpc connection to an address and closes it when the test ends.
func CreateGrpcConnection(t *testing.T, grpcAddress string, opts ...grpc.DialOption) *grpc.ClientConn {
	t.Helper()

	defaultOptions := []grpc.DialOption{
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: grpcbackoff.DefaultConfig}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	defaultOptions = append(defaultOptions, opts...)

	// nolint:staticcheck // ignoring gRPC deprecations
	conn, err := grpc.Dial(
		grpcAddress, defaultOptions...,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}

// EnsureServiceHealthy is a test helper that ensures that a service's grpc and http health endpoints are responding OK.
// If the http address is empty, it doesn't check the http health endpoint.
// If the service doesn't respond healthy in 30 seconds it fails the test.
func EnsureServiceHealthy(t testing.TB, grpcAddr, httpAddr string, transportCredentials credentials.TransportCredentials) {
	t.Helper()

	creds := insecure.NewCredentials()
	if transportCredentials != nil {
		creds = transportCredentials
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: grpcbackoff.DefaultConfig}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Log("creating connection to address", grpcAddr)
	// nolint:staticcheck // ignoring gRPC deprecations
	conn, err := grpc.DialContext(
		ctx,
		grpcAddr,
		dialOpts...,
	)
	require.NoError(t, err, "error creating grpc connection to server")
	t.Cleanup(func() {
		conn.Close()
	})

	client := healthv1pb.NewHealthClient(conn)

	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 30 * time.Second

	err = backoff.Retry(func() error {
		resp, err := client.Check(context.Background(), &healthv1pb.HealthCheckRequest{
			Service: openfgav1.OpenFGAService_ServiceDesc.ServiceName,
		})
		if err != nil {
			t.Log(time.Now(), "not serving yet at address", grpcAddr, err)
			return err
		}

		if resp.GetStatus() != healthv1pb.HealthCheckResponse_SERVING {
			t.Log(time.Now(), resp.GetStatus())
			return errors.New("not serving")
		}

		return nil
	}, policy)
	require.NoError(t, err, "server did not reach healthy status")

	if httpAddr != "" {
		resp, err := retryablehttp.Get(fmt.Sprintf("http://%s/healthz", httpAddr))
		require.NoError(t, err, "http endpoint not healthy")

		t.Cleanup(func() {
			err := resp.Body.Close()
			require.NoError(t, err)
		})

		require.Equal(t, http.StatusOK, resp.StatusCode, "unexpected status code received from server")
	}
}

// MustDefaultConfigWithRandomPorts returns default server config but with random ports for the grpc and http addresses
// and with the playground, tracing and metrics turned off.
// This function may panic if somehow a random port cannot be chosen.
func MustDefaultConfigWithRandomPorts() *serverconfig.Config {
	config := serverconfig.MustDefaultConfig()
	config.Experimentals = append(config.Experimentals, "enable-check-optimizations", "enable-list-objects-optimizations")

	httpPort, httpPortReleaser := TCPRandomPort()
	defer httpPortReleaser()
	grpcPort, grpcPortReleaser := TCPRandomPort()
	defer grpcPortReleaser()

	config.GRPC.Addr = fmt.Sprintf("localhost:%d", grpcPort)
	config.HTTP.Addr = fmt.Sprintf("localhost:%d", httpPort)

	return config
}

// TCPRandomPort tries to find a random TCP Port. If it can't find one, it panics. Else, it returns the port and a function that releases the port.
// It is the responsibility of the caller to call the release function right before trying to listen on the given port.
func TCPRandomPort() (int, func()) {
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		panic(err)
	}
	return l.Addr().(*net.TCPAddr).Port, func() {
		l.Close()
	}
}
