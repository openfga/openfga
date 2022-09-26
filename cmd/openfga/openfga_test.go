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
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/stretchr/testify/require"
	openfgapb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
func newOpenFGATester(t *testing.T, args ...string) (OpenFGATester, error) {
	t.Helper()

	dockerClient, err := client.NewClientWithOpts(client.FromEnv)
	require.NoError(t, err)

	cmd := []string{"run"}
	cmd = append(cmd, args...)

	containerCfg := container.Config{
		Env: []string{},
		ExposedPorts: nat.PortSet{
			nat.Port("8080/tcp"): {},
			nat.Port("8081/tcp"): {},
			nat.Port("3000/tcp"): {},
		},
		Image: "openfga/openfga:functionaltest",
		Cmd:   cmd,
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

	ulid := id.Must(id.New()).String()
	name := fmt.Sprintf("openfga-%s", ulid)

	ctx := context.Background()

	cont, err := dockerClient.ContainerCreate(ctx, &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create openfga docker container")

	stopContainer := func() {

		timeout := 5 * time.Second

		err := dockerClient.ContainerStop(ctx, cont.ID, &timeout)
		if err != nil && !client.IsErrNotFound(err) {
			t.Fatalf("failed to stop openfga container: %v", err)
		}
	}

	err = dockerClient.ContainerStart(ctx, cont.ID, types.ContainerStartOptions{})
	require.NoError(t, err)

	t.Cleanup(stopContainer)

	// spin up a goroutine to survive any test panics or terminations to expire/stop the running container
	go func() {
		time.Sleep(2 * time.Minute)

		// swallow the error because by this point we've terminated
		_ = dockerClient.ContainerStop(ctx, cont.ID, nil)
	}()

	containerJSON, err := dockerClient.ContainerInspect(ctx, cont.ID)
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

	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	creds := insecure.NewCredentials()

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

	// tester can be shared across tests that aren't impacted
	// by shared state
	tester, err := newOpenFGATester(t)
	require.NoError(err)
	defer tester.Cleanup()

	t.Run("TestCreateStore", func(t *testing.T) { GRPCCreateStoreTest(t, tester) })
	t.Run("TestGetStore", func(t *testing.T) { GRPCGetStoreTest(t, tester) })
	t.Run("TestListStores", GRPCListStoresTest) // run an isolated tester from the others so bootstrapped stores don't collide
	t.Run("TestDeleteStore", func(t *testing.T) { GRPCDeleteStoreTest(t, tester) })

	t.Run("TestWrite", func(t *testing.T) { GRPCWriteTest(t, tester) })
	t.Run("TestRead", func(t *testing.T) { GRPCReadTest(t, tester) })
	t.Run("TestReadChanges", func(t *testing.T) { GRPCReadChangesTest(t, tester) })

	t.Run("TestCheck", func(t *testing.T) { GRPCCheckTest(t, tester) })
	t.Run("TestExpand", func(t *testing.T) { GRPCExpandTest(t, tester) })

	t.Run("TestWriteAuthorizationModel", func(t *testing.T) { GRPCWriteAuthorizationModelTest(t, tester) })
	t.Run("TestReadAuthorizationModel", func(t *testing.T) { GRPCReadAuthorizationModelTest(t, tester) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { GRPCReadAuthorizationModelsTest(t, tester) })
}

func TestGRPCWithPresharedKey(t *testing.T) {
	tester, err := newOpenFGATester(t, "--authn-method", "preshared", "--authn-preshared-keys", "key1,key2")
	require.NoError(t, err)
	defer tester.Cleanup()

	conn := connect(t, tester)

	openfgaClient := openfgapb.NewOpenFGAServiceClient(conn)
	healthClient := healthv1pb.NewHealthClient(conn)

	resp, err := healthClient.Check(context.Background(), &healthv1pb.HealthCheckRequest{
		Service: openfgapb.OpenFGAService_ServiceDesc.ServiceName,
	})
	require.NoError(t, err)
	require.Equal(t, healthv1pb.HealthCheckResponse_SERVING, resp.Status)

	_, err = openfgaClient.CreateStore(context.Background(), &openfgapb.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.Error(t, err)

	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgapb.AuthErrorCode_bearer_token_missing), s.Code())

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key1")
	_, err = openfgaClient.CreateStore(ctx, &openfgapb.CreateStoreRequest{
		Name: "openfga-demo1",
	})
	require.NoError(t, err)

	ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key2")
	_, err = openfgaClient.CreateStore(ctx, &openfgapb.CreateStoreRequest{
		Name: "openfga-demo2",
	})
	require.NoError(t, err)

	ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key3")
	_, err = openfgaClient.CreateStore(ctx, &openfgapb.CreateStoreRequest{
		Name: "openfga-demo3",
	})
	require.Error(t, err)

	s, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgapb.AuthErrorCode_unauthenticated), s.Code())
}

// connect connects to the underlying grpc server of the OpenFGATester and
// returns the client connection.
func connect(t *testing.T, tester OpenFGATester) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.Dial(
		fmt.Sprintf("localhost:%s", tester.GetGRPCPort()),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		}...,
	)
	require.NoError(t, err)

	return conn
}

func GRPCWriteTest(t *testing.T, tester OpenFGATester) {

}

func GRPCReadTest(t *testing.T, tester OpenFGATester) {

}

func GRPCReadChangesTest(t *testing.T, tester OpenFGATester) {

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

	conn := connect(t, tester)
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

func GRPCGetStoreTest(t *testing.T, tester OpenFGATester) {
	conn := connect(t, tester)
	defer conn.Close()

	client := openfgapb.NewOpenFGAServiceClient(conn)

	resp1, err := client.CreateStore(context.Background(), &openfgapb.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	resp2, err := client.GetStore(context.Background(), &openfgapb.GetStoreRequest{
		StoreId: resp1.Id,
	})
	require.NoError(t, err)

	require.Equal(t, resp1.Name, resp2.Name)
	require.Equal(t, resp1.Id, resp2.Id)

	resp3, err := client.GetStore(context.Background(), &openfgapb.GetStoreRequest{
		StoreId: id.Must(id.New()).String(),
	})
	require.Error(t, err)
	require.Nil(t, resp3)
}

func GRPCListStoresTest(t *testing.T) {
	tester, err := newOpenFGATester(t)
	require.NoError(t, err)
	defer tester.Cleanup()

	conn := connect(t, tester)
	defer conn.Close()

	client := openfgapb.NewOpenFGAServiceClient(conn)

	_, err = client.CreateStore(context.Background(), &openfgapb.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	_, err = client.CreateStore(context.Background(), &openfgapb.CreateStoreRequest{
		Name: "openfga-test",
	})

	response1, err := client.ListStores(context.Background(), &openfgapb.ListStoresRequest{
		PageSize: wrapperspb.Int32(1),
	})
	require.NoError(t, err)

	require.NotEmpty(t, response1.ContinuationToken)

	var received []*openfgapb.Store
	received = append(received, response1.Stores...)

	response2, err := client.ListStores(context.Background(), &openfgapb.ListStoresRequest{
		PageSize:          wrapperspb.Int32(2),
		ContinuationToken: response1.ContinuationToken,
	})
	require.NoError(t, err)

	require.Empty(t, response2.ContinuationToken)

	received = append(received, response2.Stores...)

	require.Len(t, received, 2)
	// todo: add assertions on received Store objects
}

func GRPCDeleteStoreTest(t *testing.T, tester OpenFGATester) {
	conn := connect(t, tester)
	defer conn.Close()

	client := openfgapb.NewOpenFGAServiceClient(conn)

	response1, err := client.CreateStore(context.Background(), &openfgapb.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	response2, err := client.GetStore(context.Background(), &openfgapb.GetStoreRequest{
		StoreId: response1.Id,
	})
	require.NoError(t, err)

	require.Equal(t, response1.Id, response2.Id)

	_, err = client.DeleteStore(context.Background(), &openfgapb.DeleteStoreRequest{
		StoreId: response1.Id,
	})
	require.NoError(t, err)

	response3, err := client.GetStore(context.Background(), &openfgapb.GetStoreRequest{
		StoreId: response1.Id,
	})
	require.Nil(t, response3)

	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgapb.NotFoundErrorCode_store_id_not_found), s.Code())

	// delete is idempotent, so if the store does not exist it's a noop
	_, err = client.DeleteStore(context.Background(), &openfgapb.DeleteStoreRequest{
		StoreId: id.Must(id.New()).String(),
	})
	require.NoError(t, err)
}

func GRPCCheckTest(t *testing.T, tester OpenFGATester) {

	type testData struct {
		storeID         string
		modelID         string
		tuples          *openfgapb.TupleKeys
		typeDefinitions []*openfgapb.TypeDefinition
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
				AuthorizationModelId: id.Must(id.New()).String(),
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (extra chars)",
			input: &openfgapb.CheckRequest{
				StoreId:              id.Must(id.New()).String() + "A",
				AuthorizationModelId: id.Must(id.New()).String(),
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (invalid chars)",
			input: &openfgapb.CheckRequest{
				StoreId:              "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				AuthorizationModelId: id.Must(id.New()).String(),
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid authorization model ID (extra chars)",
			input: &openfgapb.CheckRequest{
				StoreId:              id.Must(id.New()).String(),
				AuthorizationModelId: id.Must(id.New()).String() + "A",
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid authorization model ID (invalid chars)",
			input: &openfgapb.CheckRequest{
				StoreId:              id.Must(id.New()).String(),
				AuthorizationModelId: "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing tuplekey field",
			input: &openfgapb.CheckRequest{
				StoreId:              id.Must(id.New()).String(),
				AuthorizationModelId: id.Must(id.New()).String(),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "blerb",
			input: &openfgapb.CheckRequest{
				StoreId: id.Must(id.New()).String(),
				// AuthorizationModelId is generated automatically during testData bootstrap
				TupleKey: tuple.NewTupleKey("repo:auth0/express-jwt", "admin", "github|bob@auth0.com"),
				Trace:    true,
			},
			output: output{
				resp: &openfgapb.CheckResponse{
					Allowed:    true,
					Resolution: ".union.0(direct).",
				},
			},
			testData: &testData{
				typeDefinitions: []*openfgapb.TypeDefinition{
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
				tuples: &openfgapb.TupleKeys{
					TupleKeys: []*openfgapb.TupleKey{
						tuple.NewTupleKey("repo:auth0/express-jwt", "admin", "github|bob@auth0.com"),
					},
				},
			},
		},
	}

	conn := connect(t, tester)
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

func GRPCExpandTest(t *testing.T, tester OpenFGATester) {

}

func GRPCReadAuthorizationModelTest(t *testing.T, tester OpenFGATester) {

	type output struct {
		resp      *openfgapb.ReadAuthorizationModelResponse
		errorCode codes.Code
	}

	type testData struct {
		typeDefinitions []*openfgapb.TypeDefinition
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
				Id:      id.Must(id.New()).String(),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid storeID (extra chars)",
			input: &openfgapb.ReadAuthorizationModelRequest{
				StoreId: id.Must(id.New()).String() + "A",
				Id:      id.Must(id.New()).String(), // ulids aren't required at this time
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid authorization model ID (extra chars)",
			input: &openfgapb.ReadAuthorizationModelRequest{
				StoreId: id.Must(id.New()).String(),
				Id:      id.Must(id.New()).String() + "A",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
	}

	conn := connect(t, tester)
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

	conn := connect(t, tester)
	defer conn.Close()

	client := openfgapb.NewOpenFGAServiceClient(conn)

	storeID := id.Must(id.New()).String()

	_, err := client.WriteAuthorizationModel(context.Background(), &openfgapb.WriteAuthorizationModelRequest{
		StoreId: storeID,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"viewer": {Userset: &openfgapb.Userset_This{}},
				},
			},
		},
	})
	require.NoError(t, err)

	_, err = client.WriteAuthorizationModel(context.Background(), &openfgapb.WriteAuthorizationModelRequest{
		StoreId: storeID,
		TypeDefinitions: []*openfgapb.TypeDefinition{
			{
				Type: "document",
				Relations: map[string]*openfgapb.Userset{
					"editor": {Userset: &openfgapb.Userset_This{}},
				},
			},
		},
	})
	require.NoError(t, err)

	resp1, err := client.ReadAuthorizationModels(context.Background(), &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:  storeID,
		PageSize: wrapperspb.Int32(1),
	})
	require.NoError(t, err)

	require.Len(t, resp1.AuthorizationModels, 1)
	require.NotEmpty(t, resp1.ContinuationToken)

	resp2, err := client.ReadAuthorizationModels(context.Background(), &openfgapb.ReadAuthorizationModelsRequest{
		StoreId:           storeID,
		ContinuationToken: resp1.ContinuationToken,
	})
	require.NoError(t, err)

	require.Len(t, resp2.AuthorizationModels, 1)
	require.Empty(t, resp2.ContinuationToken)
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
				StoreId: id.Must(id.New()).String() + "A",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing type definitions",
			input: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: id.Must(id.New()).String(),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid type definition (empty type name)",
			input: &openfgapb.WriteAuthorizationModelRequest{
				StoreId: id.Must(id.New()).String(),
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "",
						Relations: map[string]*openfgapb.Userset{
							"viewer": {Userset: &openfgapb.Userset_This{}},
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
				StoreId: id.Must(id.New()).String(),
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: testutils.CreateRandomString(255),
						Relations: map[string]*openfgapb.Userset{
							"viewer": {Userset: &openfgapb.Userset_This{}},
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
				StoreId: id.Must(id.New()).String(),
				TypeDefinitions: []*openfgapb.TypeDefinition{
					{
						Type: "some type",
						Relations: map[string]*openfgapb.Userset{
							"viewer": {Userset: &openfgapb.Userset_This{}},
						},
					},
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
	}

	conn := connect(t, tester)
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

	tester, err := newOpenFGATester(t)
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
