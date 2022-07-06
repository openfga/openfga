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
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
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
	t.Helper()

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

	timeoutCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	creds := insecure.NewCredentials()
	// if transportCredentials != nil {
	// 	creds = transportCredentials
	// }

	dialOpts := []grpc.DialOption{
		grpc.WithBlock(),
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(grpc.ConnectParams{Backoff: grpcbackoff.DefaultConfig}),
	}

	conn, err := grpc.DialContext(
		timeoutCtx,
		fmt.Sprintf("localhost:%s", grpcPort),
		dialOpts...,
	)
	require.NoError(t, err)
	defer conn.Close()

	client := healthv1pb.NewHealthClient(conn)

	backoffPolicy := backoff.NewExponentialBackOff()
	backoffPolicy.MaxElapsedTime = 30 * time.Second

	err = backoff.Retry(func() error {
		timeoutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		resp, err := client.Check(timeoutCtx, &healthv1pb.HealthCheckRequest{
			Service: openfgapb.OpenFGAService_ServiceDesc.ServiceName,
		})
		if err != nil {
			return err
		}

		if resp.GetStatus() != healthv1pb.HealthCheckResponse_SERVING {
			return fmt.Errorf("not serving")
		}

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

func TestFunctionalGRPC(t *testing.T) {

	require := require.New(t)

	tester, err := newOpenFGATester(t, "presharedkey")
	require.NoError(err)
	defer tester.Cleanup()

	t.Run("TestCreateStore", func(t *testing.T) { GRPCCreateStoreTest(t, tester) })

	t.Run("TestCheck", func(t *testing.T) { GRPCCheckTest(t, tester) })

	t.Run("TestWriteAuthorizationModel", func(t *testing.T) { GRPCWriteAuthorizationModelTest(t, tester) })
	t.Run("TestReadAuthorizationModel", func(t *testing.T) { GRPCReadAuthorizationModelTest(t, tester) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { GRPCReadAuthorizationModelsTest(t, tester) })
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

	type testData struct {
		storeID         string
		modelID         string
		tuples          *openfgapb.TupleKeys
		typeDefinitions *openfgapb.TypeDefinitions
	}

	type output struct {
		resp      *openfgapb.CheckResponse
		errorCode codes.Code
	}

	tests := []struct {
		name     string
		input    *openfgapb.CheckRequest
		output   output
		testData *testData
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
				StoreId:              "1",
				AuthorizationModelId: testutils.RandomID(t),
				TupleKey:             tuple("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (extra chars)",
			input: &openfgapb.CheckRequest{
				StoreId:              testutils.RandomID(t) + "A",
				AuthorizationModelId: testutils.RandomID(t),
				TupleKey:             tuple("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (invalid chars)",
			input: &openfgapb.CheckRequest{
				StoreId:              "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				AuthorizationModelId: testutils.RandomID(t),
				TupleKey:             tuple("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid authorization model ID (extra chars)",
			input: &openfgapb.CheckRequest{
				StoreId:              testutils.RandomID(t),
				AuthorizationModelId: testutils.RandomID(t) + "A",
				TupleKey:             tuple("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid authorization model ID (invalid chars)",
			input: &openfgapb.CheckRequest{
				StoreId:              testutils.RandomID(t),
				AuthorizationModelId: "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				TupleKey:             tuple("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing tuplekey field",
			input: &openfgapb.CheckRequest{
				StoreId:              testutils.RandomID(t),
				AuthorizationModelId: testutils.RandomID(t),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "blerb",
			input: &openfgapb.CheckRequest{
				StoreId: testutils.RandomID(t),
				// AuthorizationModelId is generated automatically during testData bootstrap
				TupleKey: tuple("repo:auth0/express-jwt", "admin", "github|bob@auth0.com"),
				Trace:    true,
			},
			output: output{
				resp: &openfgapb.CheckResponse{
					Allowed:    true,
					Resolution: ".union.0(direct).",
				},
			},
			testData: &testData{
				typeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "repo",
							Relations: map[string]*openfgapb.Userset{
								"admin": {
									Userset: &openfgapb.Userset_Union{
										Union: &openfgapb.Usersets{Child: []*openfgapb.Userset{
											{
												Userset: &openfgapb.Userset_This{
													This: &openfgapb.DirectUserset{},
												},
											},
										}},
									},
								},
							},
						},
					},
				},
				tuples: &openfgapb.TupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple("repo:auth0/express-jwt", "admin", "github|bob@auth0.com"),
					},
				},
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

			storeID := test.input.StoreId
			modelID := test.input.AuthorizationModelId
			if test.testData != nil {
				resp, err := client.WriteAuthorizationModel(context.Background(), &openfgapb.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					TypeDefinitions: test.testData.typeDefinitions,
				})
				require.NoError(t, err)

				modelID = resp.GetAuthorizationModelId()

				_, err = client.Write(context.Background(), &openfgapb.WriteRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					Writes:               test.testData.tuples,
				})
				require.NoError(t, err)
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

func GRPCReadAuthorizationModelTest(t *testing.T, tester OpenFGATester) {

	type output struct {
		resp      *openfgapb.ReadAuthorizationModelResponse
		errorCode codes.Code
	}

	type testData struct {
		typeDefinitions *openfgapb.TypeDefinitions
	}

	tests := []struct {
		name     string
		input    *openfgapb.ReadAuthorizationModelRequest
		output   output
		testData *testData
	}{
		{
			name:  "empty request",
			input: &openfgapb.ReadAuthorizationModelRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (too short)",
			input: &openfgapb.ReadAuthorizationModelRequest{
				StoreId: "1",
				Id:      testutils.RandomID(t),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (extra chars)",
			input: &openfgapb.ReadAuthorizationModelRequest{
				StoreId: testutils.RandomID(t) + "A",
				Id:      testutils.CreateRandomString(26), // ulids aren't required at this time
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid authorization model ID (extra chars)",
			input: &openfgapb.ReadAuthorizationModelRequest{
				StoreId: testutils.RandomID(t),
				Id:      testutils.CreateRandomString(27), // ulids aren't required at this time
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

			response, err := client.ReadAuthorizationModel(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				_ = response // use response for assertions
				//require.Equal(t, test.output.resp.Allowed, response.Allowed)
			}
		})
	}
}

func GRPCReadAuthorizationModelsTest(t *testing.T, tester OpenFGATester) {

}

func GRPCWriteAuthorizationModelTest(t *testing.T, tester OpenFGATester) {

	type output struct {
		resp      *openfgapb.WriteAuthorizationModelResponse
		errorCode codes.Code
	}

	tests := []struct {
		name   string
		input  *openfgapb.WriteAuthorizationModelRequest
		output output
	}{
		{
			name:  "empty request",
			input: &openfgapb.WriteAuthorizationModelRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (too short)",
			input: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: "1",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (extra chars)",
			input: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: testutils.RandomID(t) + "A",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing type definitions",
			input: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: testutils.RandomID(t),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid type definition (empty type name)",
			input: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: testutils.RandomID(t),
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "",
							Relations: map[string]*openfgapb.Userset{
								"viewer": {Userset: &openfgapb.Userset_This{}},
							},
						},
					},
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid type definition (too many chars in name)",
			input: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: testutils.RandomID(t),
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: testutils.CreateRandomString(255),
							Relations: map[string]*openfgapb.Userset{
								"viewer": {Userset: &openfgapb.Userset_This{}},
							},
						},
					},
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid type definition (invalid chars in name)",
			input: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: testutils.RandomID(t),
				TypeDefinitions: &openfgapb.TypeDefinitions{
					TypeDefinitions: []*openfgapb.TypeDefinition{
						{
							Type: "some type",
							Relations: map[string]*openfgapb.Userset{
								"viewer": {Userset: &openfgapb.Userset_This{}},
							},
						},
					},
				},
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
			response, err := client.WriteAuthorizationModel(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				_, err = id.Parse(response.AuthorizationModelId)
				require.NoError(t, err)
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

func tuple(object, relation, user string) *openfgapb.TupleKey {
	return &openfgapb.TupleKey{
		Object:   object,
		Relation: relation,
		User:     user,
	}
}
