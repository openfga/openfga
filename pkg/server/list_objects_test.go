package server

import (
	"context"
	"sync"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/structpb"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"
	parser "github.com/openfga/language/pkg/go/transformer"

	serverconfig "github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/storage/memory"
	"github.com/openfga/openfga/pkg/typesystem"
)

// recordingStreamServer records every object the server streams to the client,
// mirroring what a real gRPC client would have already observed by the time the
// RPC returns its trailing error.
type recordingStreamServer struct {
	grpc.ServerStream
	ctx context.Context

	mu      sync.Mutex
	objects []string
}

func (s *recordingStreamServer) Context() context.Context { return s.ctx }

func (s *recordingStreamServer) Send(r *openfgav1.StreamedListObjectsResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects = append(s.objects, r.GetObject())
	return nil
}

func (s *recordingStreamServer) sent() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.objects...)
}

// TestListObjectsUnevaluableExclusion asserts that ListObjects and
// StreamedListObjects both fail closed when the subtract branch of a `but not`
// cannot be evaluated: an object may not be returned if the exclusion that
// applies to it could not be resolved.
//
// user:anne is a viewer of document:1 but is also `restricted` via a
// conditioned tuple. The request context omits the condition parameter `x`, so
// evaluating the subtract branch errors instead of cleanly returning false.
func TestListObjectsUnevaluableExclusion(t *testing.T) {
	ctx := context.Background()

	ds := memory.New()
	t.Cleanup(ds.Close)

	s := MustNewServerWithOpts(
		WithDatastore(ds),
		WithExperimentals(serverconfig.ExperimentalPipelineListObjects),
	)
	t.Cleanup(s.Close)

	storeID := ulid.Make().String()

	model := parser.MustTransformDSLToProto(`
		model
		  schema 1.1
		type user
		type document
		  relations
		    define restricted: [user with cond]
		    define viewer: [user] but not restricted
		condition cond(x: int) {
		  x > 0
		}`)

	writeModelResp, err := s.WriteAuthorizationModel(ctx, &openfgav1.WriteAuthorizationModelRequest{
		StoreId:         storeID,
		TypeDefinitions: model.GetTypeDefinitions(),
		SchemaVersion:   typesystem.SchemaVersion1_1,
		Conditions:      model.GetConditions(),
	})
	require.NoError(t, err)
	modelID := writeModelResp.GetAuthorizationModelId()

	_, err = s.Write(ctx, &openfgav1.WriteRequest{
		StoreId:              storeID,
		AuthorizationModelId: modelID,
		Writes: &openfgav1.WriteRequestWrites{
			TupleKeys: []*openfgav1.TupleKey{
				{Object: "document:1", Relation: "viewer", User: "user:anne"},
				{
					Object:    "document:1",
					Relation:  "restricted",
					User:      "user:anne",
					Condition: &openfgav1.RelationshipCondition{Name: "cond"},
				},
			},
		},
	})
	require.NoError(t, err)

	// Context deliberately omits the required parameter `x`, so evaluating
	// `cond` on the subtract side errors rather than returning false.
	badContext, err := structpb.NewStruct(map[string]any{})
	require.NoError(t, err)

	t.Run("list_objects_returns_no_objects", func(t *testing.T) {
		resp, err := s.ListObjects(ctx, &openfgav1.ListObjectsRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Type:                 "document",
			Relation:             "viewer",
			User:                 "user:anne",
			Context:              badContext,
		})
		require.Error(t, err)
		require.Nil(t, resp)
	})

	t.Run("streamed_list_objects_sends_no_objects", func(t *testing.T) {
		srv := &recordingStreamServer{ctx: ctx}

		err := s.StreamedListObjects(&openfgav1.StreamedListObjectsRequest{
			StoreId:              storeID,
			AuthorizationModelId: modelID,
			Type:                 "document",
			Relation:             "viewer",
			User:                 "user:anne",
			Context:              badContext,
		}, srv)

		require.Error(t, err)
		require.Empty(t, srv.sent(),
			"objects were streamed although their exclusion could not be evaluated: %v", srv.sent())
	})
}
