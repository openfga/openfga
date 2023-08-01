//go:build functional
// +build functional

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cenkalti/backoff/v4"
	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/google/go-cmp/cmp"
	"github.com/oklog/ulid/v2"
	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	grpcbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthv1pb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
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

	dockerClient, err := client.NewClientWithOpts(
		client.FromEnv,
		client.WithAPIVersionNegotiation(),
	)
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

	ulid := ulid.Make().String()
	name := fmt.Sprintf("openfga-%s", ulid)

	ctx := context.Background()

	cont, err := dockerClient.ContainerCreate(ctx, &containerCfg, &hostCfg, nil, nil, name)
	require.NoError(t, err, "failed to create openfga docker container")

	stopContainer := func() {

		t.Logf("stopping container %s", name)
		timeoutSec := 5

		err := dockerClient.ContainerStop(ctx, cont.ID, container.StopOptions{Timeout: &timeoutSec})
		if err != nil && !client.IsErrNotFound(err) {
			t.Logf("failed to stop openfga container: %v", err)
		}

		dockerClient.Close()
		t.Logf("stopped container %s", name)
	}

	err = dockerClient.ContainerStart(ctx, cont.ID, types.ContainerStartOptions{})
	require.NoError(t, err)

	t.Cleanup(func() {
		stopContainer()
	})

	// spin up a goroutine to survive any test panics or terminations to expire/stop the running container
	go func() {
		time.Sleep(2 * time.Minute)
		timeoutSec := 0

		t.Logf("expiring container %s", name)
		// swallow the error because by this point we've terminated
		_ = dockerClient.ContainerStop(ctx, cont.ID, container.StopOptions{Timeout: &timeoutSec})

		t.Logf("expired container %s", name)
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
			Service: openfgav1.OpenFGAService_ServiceDesc.ServiceName,
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
	t.Run("TestDeleteStore", func(t *testing.T) { GRPCDeleteStoreTest(t, tester) })

	t.Run("TestWrite", func(t *testing.T) { GRPCWriteTest(t, tester) })
	t.Run("TestRead", func(t *testing.T) { GRPCReadTest(t, tester) })
	t.Run("TestReadChanges", func(t *testing.T) { GRPCReadChangesTest(t, tester) })

	t.Run("TestCheck", func(t *testing.T) { GRPCCheckTest(t, tester) })
	t.Run("TestListObjects", func(t *testing.T) { GRPCListObjectsTest(t, tester) })

	t.Run("TestWriteAuthorizationModel", func(t *testing.T) { GRPCWriteAuthorizationModelTest(t, tester) })
	t.Run("TestReadAuthorizationModel", func(t *testing.T) { GRPCReadAuthorizationModelTest(t, tester) })
	t.Run("TestReadAuthorizationModels", func(t *testing.T) { GRPCReadAuthorizationModelsTest(t, tester) })
}

func TestGRPCWithPresharedKey(t *testing.T) {
	tester, err := newOpenFGATester(t, "--authn-method", "preshared", "--authn-preshared-keys", "key1,key2")
	require.NoError(t, err)
	defer tester.Cleanup()

	conn := connect(t, tester)
	defer conn.Close()

	openfgaClient := openfgav1.NewOpenFGAServiceClient(conn)
	healthClient := healthv1pb.NewHealthClient(conn)

	resp, err := healthClient.Check(context.Background(), &healthv1pb.HealthCheckRequest{
		Service: openfgav1.OpenFGAService_ServiceDesc.ServiceName,
	})
	require.NoError(t, err)
	require.Equal(t, healthv1pb.HealthCheckResponse_SERVING, resp.Status)

	_, err = openfgaClient.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.Error(t, err)

	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.AuthErrorCode_bearer_token_missing), s.Code())

	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key1")
	_, err = openfgaClient.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: "openfga-demo1",
	})
	require.NoError(t, err)

	ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key2")
	_, err = openfgaClient.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: "openfga-demo2",
	})
	require.NoError(t, err)

	ctx = metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer key3")
	_, err = openfgaClient.CreateStore(ctx, &openfgav1.CreateStoreRequest{
		Name: "openfga-demo3",
	})
	require.Error(t, err)

	s, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.AuthErrorCode_unauthenticated), s.Code())
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
		errorCode codes.Code
	}

	tests := []struct {
		name   string
		input  *openfgav1.CreateStoreRequest
		output output
	}{
		{
			name:  "empty_request",
			input: &openfgav1.CreateStoreRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_name_length",
			input: &openfgav1.CreateStoreRequest{
				Name: "a",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_name_characters",
			input: &openfgav1.CreateStoreRequest{
				Name: "$openfga",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "success",
			input: &openfgav1.CreateStoreRequest{
				Name: "openfga",
			},
		},
		{
			name: "duplicate_store_name_is_allowed",
			input: &openfgav1.CreateStoreRequest{
				Name: "openfga",
			},
		},
	}

	conn := connect(t, tester)
	defer conn.Close()

	client := openfgav1.NewOpenFGAServiceClient(conn)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := client.CreateStore(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				require.True(t, response.Name == test.input.Name)
				_, err = ulid.Parse(response.Id)
				require.NoError(t, err)
			}
		})
	}
}

func GRPCGetStoreTest(t *testing.T, tester OpenFGATester) {
	conn := connect(t, tester)
	defer conn.Close()

	client := openfgav1.NewOpenFGAServiceClient(conn)

	resp1, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	resp2, err := client.GetStore(context.Background(), &openfgav1.GetStoreRequest{
		StoreId: resp1.Id,
	})
	require.NoError(t, err)

	require.Equal(t, resp1.Name, resp2.Name)
	require.Equal(t, resp1.Id, resp2.Id)

	resp3, err := client.GetStore(context.Background(), &openfgav1.GetStoreRequest{
		StoreId: ulid.Make().String(),
	})
	require.Error(t, err)
	require.Nil(t, resp3)
}

func TestGRPCListStores(t *testing.T) {
	tester, err := newOpenFGATester(t)
	require.NoError(t, err)
	defer tester.Cleanup()

	conn := connect(t, tester)
	defer conn.Close()

	client := openfgav1.NewOpenFGAServiceClient(conn)

	_, err = client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	_, err = client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-test",
	})
	require.NoError(t, err)

	response1, err := client.ListStores(context.Background(), &openfgav1.ListStoresRequest{
		PageSize: wrapperspb.Int32(1),
	})
	require.NoError(t, err)

	require.NotEmpty(t, response1.ContinuationToken)

	var received []*openfgav1.Store
	received = append(received, response1.Stores...)

	response2, err := client.ListStores(context.Background(), &openfgav1.ListStoresRequest{
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

	client := openfgav1.NewOpenFGAServiceClient(conn)

	response1, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
		Name: "openfga-demo",
	})
	require.NoError(t, err)

	response2, err := client.GetStore(context.Background(), &openfgav1.GetStoreRequest{
		StoreId: response1.Id,
	})
	require.NoError(t, err)

	require.Equal(t, response1.Id, response2.Id)

	_, err = client.DeleteStore(context.Background(), &openfgav1.DeleteStoreRequest{
		StoreId: response1.Id,
	})
	require.NoError(t, err)

	response3, err := client.GetStore(context.Background(), &openfgav1.GetStoreRequest{
		StoreId: response1.Id,
	})
	require.Nil(t, response3)

	s, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Code(openfgav1.NotFoundErrorCode_store_id_not_found), s.Code())

	// delete is idempotent, so if the store does not exist it's a noop
	_, err = client.DeleteStore(context.Background(), &openfgav1.DeleteStoreRequest{
		StoreId: ulid.Make().String(),
	})
	require.NoError(t, err)
}

func GRPCCheckTest(t *testing.T, tester OpenFGATester) {

	type testData struct {
		tuples []*openfgav1.TupleKey
		model  *openfgav1.AuthorizationModel
	}

	type output struct {
		resp      *openfgav1.CheckResponse
		errorCode codes.Code
	}

	tests := []struct {
		name     string
		input    *openfgav1.CheckRequest
		output   output
		testData *testData
	}{
		{
			name:  "empty_request",
			input: &openfgav1.CheckRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_too_short",
			input: &openfgav1.CheckRequest{
				StoreId:              "1",
				AuthorizationModelId: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_extra_chars",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String() + "A",
				AuthorizationModelId: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_invalid_chars",
			input: &openfgav1.CheckRequest{
				StoreId:              "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				AuthorizationModelId: ulid.Make().String(),
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_authorization_model_ID_because_extra_chars",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String() + "A",
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_authorization_model_ID_because_invalid_chars",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: "ABCDEFGHIJKLMNOPQRSTUVWXY@",
				TupleKey:             tuple.NewTupleKey("document:doc1", "viewer", "bob"),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_tuplekey_field",
			input: &openfgav1.CheckRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
	}

	conn := connect(t, tester)
	defer conn.Close()

	client := openfgav1.NewOpenFGAServiceClient(conn)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			storeID := test.input.StoreId
			if test.testData != nil {
				resp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   test.testData.model.SchemaVersion,
					TypeDefinitions: test.testData.model.TypeDefinitions,
				})
				require.NoError(t, err)

				modelID := resp.GetAuthorizationModelId()

				_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
					StoreId:              storeID,
					AuthorizationModelId: modelID,
					Writes:               &openfgav1.TupleKeys{TupleKeys: test.testData.tuples},
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

func GRPCListObjectsTest(t *testing.T, tester OpenFGATester) {
	type testData struct {
		tuples []*openfgav1.TupleKey
		model  string
	}

	type output struct {
		resp      *openfgav1.ListObjectsResponse
		errorCode codes.Code
	}

	tests := []struct {
		name     string
		input    *openfgav1.ListObjectsRequest
		output   output
		testData *testData
	}{
		{
			name: "undefined_model_id_returns_error",
			input: &openfgav1.ListObjectsRequest{
				StoreId:              ulid.Make().String(),
				AuthorizationModelId: ulid.Make().String(), // generate random ulid so it doesn't match
				Type:                 "document",
				Relation:             "viewer",
				User:                 "user:jon",
			},
			output: output{
				errorCode: codes.Code(openfgav1.ErrorCode_authorization_model_not_found),
			},
			testData: &testData{
				model: `
				type user

				type document
				  relations
				    define viewer: [user] as self
				`,
			},
		},
		{
			name: "direct_relationships_with_intersecton_returns_expected_objects",
			input: &openfgav1.ListObjectsRequest{
				StoreId:  ulid.Make().String(),
				Type:     "document",
				Relation: "viewer",
				User:     "user:jon",
			},
			output: output{
				resp: &openfgav1.ListObjectsResponse{
					Objects: []string{"document:1"},
				},
			},
			testData: &testData{
				tuples: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:jon"),
					tuple.NewTupleKey("document:1", "allowed", "user:jon"),
				},
				model: `
				type user

				type document
				  relations
				    define allowed: [user] as self
				    define viewer: [user] as self and allowed
				`,
			},
		},
	}

	conn := connect(t, tester)
	defer conn.Close()

	client := openfgav1.NewOpenFGAServiceClient(conn)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {

			typedefs := parser.MustParse(test.testData.model)

			storeID := test.input.StoreId

			if test.testData != nil {
				resp, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
					StoreId:         storeID,
					SchemaVersion:   typesystem.SchemaVersion1_1,
					TypeDefinitions: typedefs,
				})
				require.NoError(t, err)

				modelID := resp.GetAuthorizationModelId()

				if len(test.testData.tuples) > 0 {
					_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
						StoreId:              storeID,
						AuthorizationModelId: modelID,
						Writes:               &openfgav1.TupleKeys{TupleKeys: test.testData.tuples},
					})
					require.NoError(t, err)
				}
			}

			response, err := client.ListObjects(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				require.Equal(t, test.output.resp.Objects, response.Objects)
			}
		})
	}
}

// TestCheckWorkflows are tests that involve workflows that define assertions for
// Checks against multi-model stores etc..
func TestCheckWorkflows(t *testing.T) {

	tester, err := newOpenFGATester(t)
	require.NoError(t, err)
	defer tester.Cleanup()

	conn := connect(t, tester)
	defer conn.Close()

	client := openfgav1.NewOpenFGAServiceClient(conn)

	/*
	 * TypedWildcardsFromOtherModelsIgnored ensures that a typed wildcard introduced
	 * from a prior model does not impact the Check outcome of a model that should not
	 * involve it. For example,
	 *
	 * type user
	 * type document
	 *   relations
	 *     define viewer: [user:*]
	 *
	 * write(document:1#viewer@user:*)
	 *
	 * type user
	 * type document
	 *	relations
	 *	  define viewer: [user]
	 *
	 * check(document:1#viewer@user:jon) --> {allowed: false}
	 */
	t.Run("TypedWildcardsFromOtherModelsIgnored", func(t *testing.T) {
		resp1, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
			Name: "openfga-demo",
		})
		require.NoError(t, err)

		storeID := resp1.GetId()

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									typesystem.WildcardRelationReference("user"),
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.TupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:*"),
				},
			},
		})
		require.NoError(t, err)

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "user"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		resp, err := client.Check(context.Background(), &openfgav1.CheckRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", "user:jon"),
		})
		require.NoError(t, err)
		require.False(t, resp.Allowed)
	})
}

// TestExpandWorkflows are tests that involve workflows that define assertions for
// Expands against multi-model stores etc..
func TestExpandWorkflows(t *testing.T) {

	tester, err := newOpenFGATester(t)
	require.NoError(t, err)
	defer tester.Cleanup()

	conn := connect(t, tester)
	defer conn.Close()

	client := openfgav1.NewOpenFGAServiceClient(conn)

	/*
	 * TypedWildcardsFromOtherModelsIgnored ensures that a typed wildcard introduced
	 * from a prior model does not impact the Expand outcome of a model that should not
	 * involve it. For example,
	 *
	 * type user
	 * type document
	 *   relations
	 *     define viewer: [user, user:*]
	 *
	 * write(document:1#viewer@user:*)
	 * write(document:1#viewer@user:jon)
	 * Expand(document:1#viewer) --> {tree: {root: {name: document:1#viewer, leaf: {users: [user:*, user:jon]}}}}
	 *
	 * type user
	 * type document
	 *	relations
	 *	  define viewer: [user]
	 *
	 * Expand(document:1#viewer) --> {tree: {root: {name: document:1#viewer, leaf: {users: [user:jon]}}}}
	 *
	 * type employee
	 * type document
	 *   relations
	 *     define viewer: [employee]
	 *
	 * type user
	 * type employee
	 * type document
	 *   relations
	 *     define viewer: [employee]
	 * Expand(document:1#viewer) --> {tree: {root: {name: document:1#viewer, leaf: {users: []}}}}
	 */
	t.Run("TypedWildcardsFromOtherModelsIgnored", func(t *testing.T) {
		resp1, err := client.CreateStore(context.Background(), &openfgav1.CreateStoreRequest{
			Name: "openfga-demo",
		})
		require.NoError(t, err)

		storeID := resp1.GetId()

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									typesystem.DirectRelationReference("user", ""),
									typesystem.WildcardRelationReference("user"),
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		_, err = client.Write(context.Background(), &openfgav1.WriteRequest{
			StoreId: storeID,
			Writes: &openfgav1.TupleKeys{
				TupleKeys: []*openfgav1.TupleKey{
					tuple.NewTupleKey("document:1", "viewer", "user:*"),
					tuple.NewTupleKey("document:1", "viewer", "user:jon"),
				},
			},
		})
		require.NoError(t, err)

		expandResp, err := client.Expand(context.Background(), &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", ""),
		})
		require.NoError(t, err)

		if diff := cmp.Diff(&openfgav1.UsersetTree{
			Root: &openfgav1.UsersetTree_Node{
				Name: "document:1#viewer",
				Value: &openfgav1.UsersetTree_Node_Leaf{
					Leaf: &openfgav1.UsersetTree_Leaf{
						Value: &openfgav1.UsersetTree_Leaf_Users{
							Users: &openfgav1.UsersetTree_Users{
								Users: []string{"user:*", "user:jon"},
							},
						},
					},
				},
			},
		}, expandResp.GetTree(), protocmp.Transform(), protocmp.SortRepeated(func(x, y string) bool {
			return x <= y
		})); diff != "" {
			require.Fail(t, diff)
		}

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									typesystem.DirectRelationReference("user", ""),
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		expandResp, err = client.Expand(context.Background(), &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", ""),
		})
		require.NoError(t, err)

		if diff := cmp.Diff(&openfgav1.UsersetTree{
			Root: &openfgav1.UsersetTree_Node{
				Name: "document:1#viewer",
				Value: &openfgav1.UsersetTree_Node_Leaf{
					Leaf: &openfgav1.UsersetTree_Leaf{
						Value: &openfgav1.UsersetTree_Leaf_Users{
							Users: &openfgav1.UsersetTree_Users{
								Users: []string{"user:jon"},
							},
						},
					},
				},
			},
		}, expandResp.GetTree(), protocmp.Transform()); diff != "" {
			require.Fail(t, diff)
		}

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "employee",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "employee"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		expandResp, err = client.Expand(context.Background(), &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", ""),
		})
		require.NoError(t, err)

		if diff := cmp.Diff(&openfgav1.UsersetTree{
			Root: &openfgav1.UsersetTree_Node{
				Name: "document:1#viewer",
				Value: &openfgav1.UsersetTree_Node_Leaf{
					Leaf: &openfgav1.UsersetTree_Leaf{
						Value: &openfgav1.UsersetTree_Leaf_Users{
							Users: &openfgav1.UsersetTree_Users{
								Users: []string{},
							},
						},
					},
				},
			},
		}, expandResp.GetTree(), protocmp.Transform()); diff != "" {
			require.Fail(t, diff)
		}

		_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
			StoreId:       storeID,
			SchemaVersion: typesystem.SchemaVersion1_1,
			TypeDefinitions: []*openfgav1.TypeDefinition{
				{
					Type: "user",
				},
				{
					Type: "employee",
				},
				{
					Type: "document",
					Relations: map[string]*openfgav1.Userset{
						"viewer": typesystem.This(),
					},
					Metadata: &openfgav1.Metadata{
						Relations: map[string]*openfgav1.RelationMetadata{
							"viewer": {
								DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
									{Type: "employee"},
								},
							},
						},
					},
				},
			},
		})
		require.NoError(t, err)

		expandResp, err = client.Expand(context.Background(), &openfgav1.ExpandRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewTupleKey("document:1", "viewer", ""),
		})
		require.NoError(t, err)

		if diff := cmp.Diff(&openfgav1.UsersetTree{
			Root: &openfgav1.UsersetTree_Node{
				Name: "document:1#viewer",
				Value: &openfgav1.UsersetTree_Node_Leaf{
					Leaf: &openfgav1.UsersetTree_Leaf{
						Value: &openfgav1.UsersetTree_Leaf_Users{
							Users: &openfgav1.UsersetTree_Users{
								Users: []string{},
							},
						},
					},
				},
			},
		}, expandResp.GetTree(), protocmp.Transform()); diff != "" {
			require.Fail(t, diff)
		}
	})
}

func GRPCReadAuthorizationModelTest(t *testing.T, tester OpenFGATester) {

	type output struct {
		errorCode codes.Code
	}

	tests := []struct {
		name   string
		input  *openfgav1.ReadAuthorizationModelRequest
		output output
	}{
		{
			name:  "empty_request",
			input: &openfgav1.ReadAuthorizationModelRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_too_short",
			input: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: "1",
				Id:      ulid.Make().String(),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_extra_chars",
			input: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: ulid.Make().String() + "A",
				Id:      ulid.Make().String(), // ulids aren't required at this time
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_authorization_model_ID_because_extra_chars",
			input: &openfgav1.ReadAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				Id:      ulid.Make().String() + "A",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
	}

	conn := connect(t, tester)
	defer conn.Close()

	client := openfgav1.NewOpenFGAServiceClient(conn)

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

	client := openfgav1.NewOpenFGAServiceClient(conn)

	storeID := ulid.Make().String()

	_, err := client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"viewer": {Userset: &openfgav1.Userset_This{}},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"viewer": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								typesystem.DirectRelationReference("user", ""),
							},
						},
					},
				},
			},
		},

		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	_, err = client.WriteAuthorizationModel(context.Background(), &openfgav1.WriteAuthorizationModelRequest{
		StoreId: storeID,
		TypeDefinitions: []*openfgav1.TypeDefinition{
			{
				Type: "user",
			},
			{
				Type: "document",
				Relations: map[string]*openfgav1.Userset{
					"editor": {Userset: &openfgav1.Userset_This{}},
				},
				Metadata: &openfgav1.Metadata{
					Relations: map[string]*openfgav1.RelationMetadata{
						"editor": {
							DirectlyRelatedUserTypes: []*openfgav1.RelationReference{
								typesystem.DirectRelationReference("user", ""),
							},
						},
					},
				},
			},
		},
		SchemaVersion: typesystem.SchemaVersion1_1,
	})
	require.NoError(t, err)

	resp1, err := client.ReadAuthorizationModels(context.Background(), &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:  storeID,
		PageSize: wrapperspb.Int32(1),
	})
	require.NoError(t, err)

	require.Len(t, resp1.AuthorizationModels, 1)
	require.NotEmpty(t, resp1.ContinuationToken)

	resp2, err := client.ReadAuthorizationModels(context.Background(), &openfgav1.ReadAuthorizationModelsRequest{
		StoreId:           storeID,
		ContinuationToken: resp1.ContinuationToken,
	})
	require.NoError(t, err)

	require.Len(t, resp2.AuthorizationModels, 1)
	require.Empty(t, resp2.ContinuationToken)
}

func GRPCWriteAuthorizationModelTest(t *testing.T, tester OpenFGATester) {

	type output struct {
		errorCode codes.Code
	}

	tests := []struct {
		name   string
		input  *openfgav1.WriteAuthorizationModelRequest
		output output
	}{
		{
			name:  "empty_request",
			input: &openfgav1.WriteAuthorizationModelRequest{},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_too_short",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: "1",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_storeID_because_extra_chars",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: ulid.Make().String() + "A",
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "missing_type_definitions",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_type_definition_because_empty_type_name",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "",
						Relations: map[string]*openfgav1.Userset{
							"viewer": {Userset: &openfgav1.Userset_This{}},
						},
					},
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_type_definition_because_too_many_chars_in_name",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: testutils.CreateRandomString(255),
						Relations: map[string]*openfgav1.Userset{
							"viewer": {Userset: &openfgav1.Userset_This{}},
						},
					},
				},
			},
			output: output{
				errorCode: codes.InvalidArgument,
			},
		},
		{
			name: "invalid_type_definition_because_invalid_chars_in_name",
			input: &openfgav1.WriteAuthorizationModelRequest{
				StoreId: ulid.Make().String(),
				TypeDefinitions: []*openfgav1.TypeDefinition{
					{
						Type: "some type",
						Relations: map[string]*openfgav1.Userset{
							"viewer": {Userset: &openfgav1.Userset_This{}},
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

	client := openfgav1.NewOpenFGAServiceClient(conn)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, err := client.WriteAuthorizationModel(context.Background(), test.input)

			s, ok := status.FromError(err)
			require.True(t, ok)
			require.Equal(t, test.output.errorCode.String(), s.Code().String())

			if test.output.errorCode == codes.OK {
				_, err = ulid.Parse(response.AuthorizationModelId)
				require.NoError(t, err)
			}
		})
	}
}
