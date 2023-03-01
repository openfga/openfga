package check

import (
	"context"
	"fmt"
	"testing"

	parser "github.com/craigpastro/openfga-dsl-parser/v2"
	"github.com/openfga/openfga/cmd/run"
	"github.com/openfga/openfga/pkg/tuple"
	"github.com/openfga/openfga/pkg/typesystem"
	"github.com/openfga/openfga/tests"
	"github.com/stretchr/testify/require"
	pb "go.buf.build/openfga/go/openfga/api/openfga/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

var tuples = []*pb.TupleKey{
	tuple.NewTupleKey("repo:openfga/openfga", "reader", "team:openfga#member"),
	tuple.NewTupleKey("team:openfga", "member", "user:github|iaco@openfga"),
}

func TestCheckMemory(t *testing.T) {
	testRunAll(t, "memory")
}

func TestCheckPostgres(t *testing.T) {
	testRunAll(t, "postgres")
}

func TestCheckMySQL(t *testing.T) {
	testRunAll(t, "mysql")
}

func testRunAll(t *testing.T, engine string) {
	testCheck(t, engine)
	testBadAuthModelID(t, engine)
}

func testCheck(t *testing.T, engine string) {

	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()
	t.Run("testCheck", func(t *testing.T) {

		RunSchema1_1CheckTests(t, pb.NewOpenFGAServiceClient(conn))
		RunSchema1_0CheckTests(t, pb.NewOpenFGAServiceClient(conn))

	})
}

func testBadAuthModelID(t *testing.T, engine string) {

	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine

	cancel := tests.StartServer(t, cfg)
	defer cancel()

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewOpenFGAServiceClient(conn)

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "bad auth id"})
	require.NoError(t, err)

	storeID := resp.GetId()
	model := `
	type user

	type doc
	  relations
	    define viewer: [user] as self
	    define can_view as viewer
	`
	_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(model),
	})
	require.NoError(t, err)
	_, err = client.Check(ctx, &pb.CheckRequest{
		StoreId:              storeID,
		TupleKey:             tuple.NewTupleKey("doc:x", "viewer", "user:y"),
		AuthorizationModelId: "01GS89AJC3R3PFQ9BNY5ZF6Q97",
	})

	require.Error(t, err)
	e, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, int(pb.ErrorCode_authorization_model_not_found), int(e.Code()))
}

func BenchmarkCheckMemory(b *testing.B) {
	benchmarkAll(b, "memory")
}

func BenchmarkCheckPostgres(b *testing.B) {
	benchmarkAll(b, "postgres")
}

func BenchmarkCheckMySQL(b *testing.B) {
	benchmarkAll(b, "mysql")
}

func benchmarkAll(b *testing.B, engine string) {
	b.Run("BenchmarkCheckWithoutTrace", func(b *testing.B) { benchmarkCheckWithoutTrace(b, engine) })
	b.Run("BenchmarkCheckWithTrace", func(b *testing.B) { benchmarkCheckWithTrace(b, engine) })
	b.Run("BenchmarkCheckWithDirectResolution", func(b *testing.B) { benchmarkCheckWithDirectResolution(b, engine) })
	b.Run("BenchmarkCheckWithBypassDirectRead", func(b *testing.B) { benchmarkCheckWithBypassDirectRead(b, engine) })
	b.Run("BenchmarkCheckWithBypassUsersetRead", func(b *testing.B) { benchmarkCheckWithBypassUsersetRead(b, engine) })
}

const githubModel = `
type user
type team
  relations
    define member: [user,team#member] as self
type repo
  relations
    define admin: [user,team#member] as self or repo_admin from owner
    define maintainer: [user,team#member] as self or admin
    define owner: [organization] as self
    define reader: [user,team#member] as self or triager or repo_reader from owner
    define triager: [user,team#member] as self or writer
    define writer: [user,team#member] as self or maintainer or repo_writer from owner
type organization
  relations
    define member: [user] as self or owner
    define owner: [user] as self
    define repo_admin: [user,organization#member] as self
    define repo_reader: [user,organization#member] as self
    define repo_writer: [user,organization#member] as self
`

func setupBenchmarkTest(b *testing.B, engine string) (context.CancelFunc, *grpc.ClientConn, pb.OpenFGAServiceClient) {
	cfg := run.MustDefaultConfigWithRandomPorts()
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = engine

	cancel := tests.StartServer(b, cfg)

	conn, err := grpc.Dial(cfg.GRPC.Addr,
		grpc.WithBlock(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(b, err)

	client := pb.NewOpenFGAServiceClient(conn)
	return cancel, conn, client
}

func benchmarkCheckWithoutTrace(b *testing.B, engine string) {

	cancel, conn, client := setupBenchmarkTest(b, engine)
	defer cancel()
	defer conn.Close()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "check benchmark without trace"})
	require.NoError(b, err)

	storeID := resp.GetId()
	_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(githubModel),
	})
	require.NoError(b, err)
	_, err = client.Write(ctx, &pb.WriteRequest{
		StoreId: storeID,
		Writes:  &pb.TupleKeys{TupleKeys: tuples},
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = client.Check(ctx, &pb.CheckRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "user:github|iaco@openfga"),
		})

		require.NoError(b, err)
	}
}

func benchmarkCheckWithTrace(b *testing.B, engine string) {
	cancel, conn, client := setupBenchmarkTest(b, engine)
	defer cancel()
	defer conn.Close()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "check benchmark with trace"})
	require.NoError(b, err)

	storeID := resp.GetId()
	_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(githubModel),
	})
	require.NoError(b, err)
	_, err = client.Write(ctx, &pb.WriteRequest{
		StoreId: storeID,
		Writes:  &pb.TupleKeys{TupleKeys: tuples},
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = client.Check(ctx, &pb.CheckRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "reader", "user:github|iaco@openfga"),
			Trace:    true,
		})

		require.NoError(b, err)
	}
}

func benchmarkCheckWithDirectResolution(b *testing.B, engine string) {
	cancel, conn, client := setupBenchmarkTest(b, engine)
	defer cancel()
	defer conn.Close()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "check benchmark with direct resolution"})
	require.NoError(b, err)

	storeID := resp.GetId()
	_, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(githubModel),
	})
	require.NoError(b, err)

	// add user to many usersets
	for i := 0; i < 1000; i++ {
		_, err = client.Write(ctx, &pb.WriteRequest{
			StoreId: storeID,
			Writes: &pb.TupleKeys{TupleKeys: []*pb.TupleKey{
				tuple.NewTupleKey(fmt.Sprintf("team:%d", i), "member", "user:anne"),
			}},
		})
		require.NoError(b, err)
	}

	// one of those usersets gives access to the repo
	_, err = client.Write(ctx, &pb.WriteRequest{
		StoreId: storeID,
		Writes: &pb.TupleKeys{TupleKeys: []*pb.TupleKey{
			tuple.NewTupleKey("repo:openfga", "admin", "team:999#member"),
		}},
	})
	require.NoError(b, err)

	// add direct access to the repo
	_, err = client.Write(ctx, &pb.WriteRequest{
		StoreId: storeID,
		Writes: &pb.TupleKeys{TupleKeys: []*pb.TupleKey{
			tuple.NewTupleKey("repo:openfga", "admin", "user:anne"),
		}},
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err = client.Check(ctx, &pb.CheckRequest{
			StoreId:  storeID,
			TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "admin", "user:anne"),
		})

		require.NoError(b, err)
	}
}

func benchmarkCheckWithBypassDirectRead(b *testing.B, engine string) {
	cancel, conn, client := setupBenchmarkTest(b, engine)
	defer cancel()
	defer conn.Close()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "check benchmark with bypass direct read"})
	require.NoError(b, err)

	storeID := resp.GetId()
	writeAuthModelResponse, err := client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		SchemaVersion:   typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(githubModel),
	})
	require.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		check, err := client.Check(ctx, &pb.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.AuthorizationModelId,
			// users can't be direct owners of repos
			TupleKey: tuple.NewTupleKey("repo:openfga/openfga", "owner", "user:anne"),
		})

		require.False(b, check.Allowed)
		require.NoError(b, err)
	}
}

func benchmarkCheckWithBypassUsersetRead(b *testing.B, engine string) {
	cancel, conn, client := setupBenchmarkTest(b, engine)
	defer cancel()
	defer conn.Close()

	ctx := context.Background()
	resp, err := client.CreateStore(ctx, &pb.CreateStoreRequest{Name: "check benchmark with bypass direct read"})
	require.NoError(b, err)

	storeID := resp.GetId()
	// model A
	writeAuthModelResponse, err := client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
		StoreId:       storeID,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(`type user
          type group
            relations
              define member: [user] as self
          type document
            relations
              define viewer: [user:*, group#member] as self`),
	})
	require.NoError(b, err)

	// add user to many usersets according to model A
	for i := 0; i < 1000; i++ {
		_, err = client.Write(ctx, &pb.WriteRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.AuthorizationModelId,
			Writes: &pb.TupleKeys{TupleKeys: []*pb.TupleKey{
				tuple.NewTupleKey(fmt.Sprintf("group:%d", i), "member", "user:anne"),
			}},
		})
		require.NoError(b, err)
	}

	// one of those usersets gives access to document:budget
	_, err = client.Write(ctx, &pb.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: writeAuthModelResponse.AuthorizationModelId,
		Writes: &pb.TupleKeys{TupleKeys: []*pb.TupleKey{
			tuple.NewTupleKey("document:budget", "viewer", "group:999#member"),
		}},
	})
	require.NoError(b, err)

	// model B
	writeAuthModelResponse, err = client.WriteAuthorizationModel(ctx, &pb.WriteAuthorizationModelRequest{
		StoreId:       storeID,
		SchemaVersion: typesystem.SchemaVersion1_1,
		TypeDefinitions: parser.MustParse(`type user
          type user2
          type group
            relations
              define member: [user2] as self
          type document
            relations
              define viewer: [user:*, group#member] as self`),
	})
	require.NoError(b, err)

	// all the usersets added above are now invalid and should be skipped!

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		check, err := client.Check(ctx, &pb.CheckRequest{
			StoreId:              storeID,
			AuthorizationModelId: writeAuthModelResponse.AuthorizationModelId,
			TupleKey:             tuple.NewTupleKey("document:budget", "viewer", "user:anne"),
		})

		require.False(b, check.Allowed)
		require.NoError(b, err)
	}
}
