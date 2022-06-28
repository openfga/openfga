//go:build functional
// +build functional

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/openfga/openfga/pkg/id"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type OpenFGATester interface {
	GetGRPCPort() string
	GetHTTPPort() string
	Cleanup() func()
}

type serverHandle struct {
	grpcPort string
	httpPort string
	cleanup  func()
}

func (s *serverHandle) GetGRPCPort() string {
	return s.grpcPort
}

func (s *serverHandle) GetHTTPPort() string {
	return s.httpPort
}

func (s *serverHandle) Cleanup() func() {
	return s.cleanup
}

// newOpenFGATester spins up an openfga container with the default service ports
// exposed for testing purposes. Before running functional tests it is assumed
// the openfga/openfga container is already built and available to the docker engine.
func newOpenFGATester(t *testing.T, presharedKey string) (OpenFGATester, error) {

	dockerClient, err := client.NewClientWithOpts(client.FromEnv)
	require.NoError(t, err)

	containerCfg := container.Config{
		Env: []string{},
		ExposedPorts: nat.PortSet{
			nat.Port("8080/tcp"): {},
			nat.Port("8081/tcp"): {},
			nat.Port("3000/tcp"): {},
		},
		Image: "openfga/openfga",
		Cmd:   []string{"run"},
	}

	hostCfg := container.HostConfig{
		AutoRemove:      true,
		PublishAllPorts: true,
		PortBindings: nat.PortMap{
			"8080/tcp": []nat.PortBinding{},
			"8081/tcp": []nat.PortBinding{},
			"3000/tcp": []nat.PortBinding{},
		},
	}

	ulid, err := id.NewString()
	require.NoError(t, err)

	name := fmt.Sprintf("openfga-%s", ulid)

	cont, err := dockerClient.ContainerCreate(context.Background(), &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create openfga docker container")

	stopContainer := func() {

		timeout := 5 * time.Second

		err := dockerClient.ContainerStop(context.Background(), cont.ID, &timeout)
		if err != nil && !client.IsErrNotFound(err) {
			t.Fatalf("failed to stop openfga container: %v", err)
		}
	}

	err = dockerClient.ContainerStart(context.Background(), cont.ID, types.ContainerStartOptions{})
	require.NoError(t, err)

	t.Cleanup(stopContainer)

	// spin up a goroutine to survive any test panics or terminations to expire/stop the running container
	go func() {
		time.Sleep(2 * time.Minute)

		// swallow the error because by this point we've terminated
		_ = dockerClient.ContainerStop(context.Background(), cont.ID, nil)
	}()

	containerJSON, err := dockerClient.ContainerInspect(context.Background(), cont.ID)
	require.NoError(t, err)

	ports := containerJSON.NetworkSettings.Ports

	m, ok := ports["8080/tcp"]
	if !ok || len(m) == 0 {
		t.Fatalf("failed to get HTTP host port mapping from openfga container")
	}
	httpPort := m[0].HostPort

	m, ok = ports["8081/tcp"]
	if !ok || len(m) == 0 {
		t.Fatalf("failed to get grpc host port mapping from openfga container")
	}
	grpcPort := m[0].HostPort

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 30 * time.Second

	err = backoff.Retry(
		func() error {
			var err error

			timeoutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			_ = timeoutCtx
			_ = err
			// todo: use grpc health check here

			return nil
		},
		backoffPolicy,
	)
	require.NoError(t, err)

	return &serverHandle{
		grpcPort: grpcPort,
		httpPort: httpPort,
		cleanup:  stopContainer,
	}, nil
}

func randomID(t *testing.T) string {
	id, err := id.NewString()
	require.NoError(t, err)

	return id
}

func TestFunctionalGRPC(t *testing.T) {

	require := require.New(t)

	tester, err := newOpenFGATester(t, "presharedkey")
	require.NoError(err)
	defer tester.Cleanup()

	t.Run("TestCreateStore", func(t *testing.T) { GRPCCreateStoreTest(t, tester) })

	t.Run("TestCheck", func(t *testing.T) { GRPCCheckTest(t, tester) })
}

func GRPCCreateStoreTest(t *testing.T, tester OpenFGATester) {

	type output struct {
		resp      *openfgapb.CreateStoreResponse
		errorCode codes.Code
	}

	tests := []struct {
		name   string
		input  *openfgapb.CreateStoreRequest
		output output
	}{
		{
			name:  "empty request",
			input: &openfgapb.CreateStoreRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid 'name' length",
			input: &openfgapb.CreateStoreRequest{
				Name: "a",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid 'name' characters",
			input: &openfgapb.CreateStoreRequest{
				Name: "$openfga",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "success",
			input: &openfgapb.CreateStoreRequest{
				Name: "openfga",
			},
		},
		{
			name: "duplicate store 'name' is allowed",
			input: &openfgapb.CreateStoreRequest{
				Name: "openfga",
			},
		},
	}

	conn, err := grpc.Dial(
		fmt.Sprintf("localhost:%s", tester.GetGRPCPort()),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}...,
	)
	require.NoError(t, err)
	defer conn.Close()

	client := openfgapb.NewOpenFGAServiceClient(conn)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := client.CreateStore(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				require.True(t, response.Name == test.input.Name)
				require.True(t, id.IsValid(response.Id))
			}
		})
	}
}

func GRPCCheckTest(t *testing.T, tester OpenFGATester) {

	type output struct {
		resp      *openfgapb.CheckResponse
		errorCode codes.Code
	}

	tests := []struct {
		name      string
		input     *openfgapb.CheckRequest
		output    output
		bootstrap *bootstrap
	}{
		{
			name:  "empty request",
			input: &openfgapb.CheckRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (too short)",
			input: &openfgapb.CheckRequest{
				StoreId: "1",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (extra chars)",
			input: &openfgapb.CheckRequest{
				StoreId: randomID(t) + "A",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (invalid chars)",
			input: &openfgapb.CheckRequest{
				StoreId: "ABCDEFGHIJKLMNOPQRSTUVWXY@",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
	}

	conn, err := grpc.Dial(
		fmt.Sprintf("localhost:%s", tester.GetGRPCPort()),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}...,
	)
	require.NoError(t, err)
	defer conn.Close()

	client := openfgapb.NewOpenFGAServiceClient(conn)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storeID := randomID(t)
			modelID := randomID(t)

			if test.bootstrap != nil {
				test.bootstrap.storeID = storeID
				test.bootstrap.modelID = modelID
				bootstrapTest(t, test.bootstrap, client)
			}

			response, err := client.Check(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				require.Equal(t, test.output.resp.Allowed, response.Allowed)
				require.Equal(t, test.output.resp.Resolution, response.Resolution)
			}
		})
	}
}

func TestFunctionalHTTP(t *testing.T) {

	require := require.New(t)

	tester, err := newOpenFGATester(t, "presharedkey")
	require.NoError(err)
	defer tester.Cleanup()

	t.Run("TestCreateStore", HTTPCreateStoreTest)
	t.Run("TestListStores", HTTPListStoresTest)
	t.Run("TestDeleteStore", HTTPDeleteStoreTest)
	t.Run("TestWrite", HTTPWriteTest)
	t.Run("TestRead", HTTPReadTest)
	t.Run("TestCheck", HTTPCheckTest)
	t.Run("TestExpand", HTTPExpandTest)
}

func HTTPCreateStoreTest(t *testing.T) {

}

func HTTPListStoresTest(t *testing.T) {

}

func HTTPDeleteStoreTest(t *testing.T) {

}

func HTTPWriteTest(t *testing.T) {

}

func HTTPReadTest(t *testing.T) {

}

func HTTPCheckTest(t *testing.T) {

}

func HTTPExpandTest(t *testing.T) {

}

type bootstrap struct {
	storeID         string
	modelID         string
	tuples          *openfgapb.TupleKeys
	typeDefinitions *openfgapb.TypeDefinitions
}

func bootstrapTest(t *testing.T, b *bootstrap, client openfgapb.OpenFGAServiceClient) {
	_, err := client.WriteAuthorizationModel(context.Background(), &openfgapb.WriteAuthorizationModelRequest{
		StoreId:         b.storeID,
		TypeDefinitions: b.typeDefinitions,
	})
	require.NoError(t, err)

	_, err = client.Write(context.Background(), &openfgapb.WriteRequest{
		StoreId:              b.storeID,
		AuthorizationModelId: b.modelID,
		Writes:               b.tuples,
	})
	require.NoError(t, err)
}
