// Package testutils contains code that is useful in tests.
package testutils

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/go-cmp/cmp"
	"github.com/hashicorp/go-retryablehttp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	AllChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
)

var (
	TupleCmpTransformer = cmp.Transformer("Sort", func(in []*openfgav1.Tuple) []*openfgav1.Tuple {
		out := append([]*openfgav1.Tuple(nil), in...) // Copy input to avoid mutating it

		sort.SliceStable(out, func(i, j int) bool {
			if out[i].GetKey().Object != out[j].GetKey().Object {
				return out[i].GetKey().Object < out[j].GetKey().Object
			}

			if out[i].GetKey().Relation != out[j].GetKey().Relation {
				return out[i].GetKey().Relation < out[j].GetKey().Relation
			}

			if out[i].GetKey().User != out[j].GetKey().User {
				return out[i].GetKey().User < out[j].GetKey().User
			}

			return true
		})

		return out
	})
	TupleKeyCmpTransformer = cmp.Transformer("Sort", func(in []*openfgav1.TupleKey) []*openfgav1.TupleKey {
		out := append([]*openfgav1.TupleKey(nil), in...) // Copy input to avoid mutating it

		sort.SliceStable(out, func(i, j int) bool {
			if out[i].Object != out[j].Object {
				return out[i].Object < out[j].Object
			}

			if out[i].Relation != out[j].Relation {
				return out[i].Relation < out[j].Relation
			}

			if out[i].User != out[j].User {
				return out[i].User < out[j].User
			}

			return true
		})

		return out
	})
)

func CreateRandomString(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = AllChars[rand.Intn(len(AllChars))]
	}
	return string(b)
}

func MustNewStruct(t *testing.T, v map[string]interface{}) *structpb.Struct {
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

	conn, err := grpc.Dial(
		grpcAddress, defaultOptions...,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})

	return conn
}

// EnsureServiceHealthy is a test helper that ensures that a service's grpc health endpoint is responding OK. It can also
// ensure that the HTTP /healthz endpoint is responding OK. If the service doesn't respond healthy in 30 seconds it fails the test.
func EnsureServiceHealthy(t testing.TB, grpcAddr, httpAddr string, transportCredentials credentials.TransportCredentials, httpHealthCheck bool) {
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

	if httpHealthCheck {
		resp, err := retryablehttp.Get(fmt.Sprintf("http://%s/healthz", httpAddr))
		require.NoError(t, err, "http endpoint not healthy")
		require.Equal(t, http.StatusOK, resp.StatusCode, "unexpected status code received from server")
	}
}
