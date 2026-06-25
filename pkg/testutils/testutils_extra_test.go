package testutils

import (
	"strconv"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/tuple"
)

func TestConvertTuplesAndKeys(t *testing.T) {
	tuples := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:anne")},
		{Key: tuple.NewTupleKey("document:2", "editor", "user:bob")},
	}

	keys := ConvertTuplesToTupleKeys(tuples)
	require.Len(t, keys, 2)
	require.Equal(t, "document:1", keys[0].GetObject())

	back := ConvertTuplesKeysToTuples(keys)
	require.Len(t, back, 2)
	require.Equal(t, "editor", back[1].GetKey().GetRelation())
}

func TestShuffle(t *testing.T) {
	in := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
		tuple.NewTupleKey("document:2", "viewer", "user:bob"),
		tuple.NewTupleKey("document:3", "viewer", "user:charlie"),
	}
	out := Shuffle(in)
	require.Len(t, out, len(in))
	// Shuffle copies; the input slice header is not mutated in place.
	require.Equal(t, "document:1", in[0].GetObject())
}

func TestCreateRandomString(t *testing.T) {
	s := CreateRandomString(16)
	require.Len(t, s, 16)
	for _, r := range s {
		require.Contains(t, AllChars, string(r))
	}
	require.Empty(t, CreateRandomString(0))
}

func TestMustNewStruct(t *testing.T) {
	s := MustNewStruct(t, map[string]interface{}{"ip": "10.0.0.1", "n": 3})
	require.NotNil(t, s)
	require.Equal(t, "10.0.0.1", s.GetFields()["ip"].GetStringValue())
}

func TestMakeSliceWithGenerator(t *testing.T) {
	s := MakeSliceWithGenerator[string](3, NumericalStringGenerator)
	require.Equal(t, []string{"0", "1", "2"}, s)

	require.Equal(t, "5", NumericalStringGenerator(5))
}

func TestMakeStringWithRuneset(t *testing.T) {
	runes := []rune{'a', 'b'}
	s := MakeStringWithRuneset(10, runes)
	require.Len(t, []rune(s), 10)
	for _, r := range s {
		require.Contains(t, "ab", string(r))
	}
}

func TestMustTransformDSLToProtoWithID(t *testing.T) {
	model := MustTransformDSLToProtoWithID(`
model
  schema 1.1
type user
type document
  relations
    define viewer: [user]
`)
	require.NotEmpty(t, model.GetId())
	require.Len(t, model.GetTypeDefinitions(), 2)
}

func TestTupleCmpTransformers(t *testing.T) {
	a := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:2", "viewer", "user:bob")},
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:anne")},
	}
	b := []*openfgav1.Tuple{
		{Key: tuple.NewTupleKey("document:1", "viewer", "user:anne")},
		{Key: tuple.NewTupleKey("document:2", "viewer", "user:bob")},
	}
	// Order-insensitive thanks to the sorting transformer.
	require.Empty(t, cmp.Diff(a, b, TupleCmpTransformer, protocmp.Transform()))

	ka := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:2", "viewer", "user:bob"),
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
	}
	kb := []*openfgav1.TupleKey{
		tuple.NewTupleKey("document:1", "viewer", "user:anne"),
		tuple.NewTupleKey("document:2", "viewer", "user:bob"),
	}
	require.Empty(t, cmp.Diff(ka, kb, TupleKeyCmpTransformer, protocmp.Transform()))
}

func TestMustDefaultConfigVariants(t *testing.T) {
	cfg := MustDefaultConfig()
	require.False(t, cfg.Playground.Enabled)
	require.False(t, cfg.Trace.Enabled)
	require.False(t, cfg.Metrics.Enabled)

	parallel := MustDefaultConfigForParallelTests()
	require.NotZero(t, parallel.Datastore.ConnMaxIdleTime)
	require.NotZero(t, parallel.RequestTimeout)

	randomPorts := MustDefaultConfigWithRandomPorts()
	require.NotEmpty(t, randomPorts.GRPC.Addr)
	require.NotEmpty(t, randomPorts.HTTP.Addr)
	require.NotEqual(t, randomPorts.GRPC.Addr, randomPorts.HTTP.Addr)
}

func TestTCPRandomPort(t *testing.T) {
	port, release := TCPRandomPort()
	require.Positive(t, port)
	// The returned port should be a parseable port number and releasable.
	require.NotEmpty(t, strconv.Itoa(port))
	release()
}

func TestCreateGrpcConnection(t *testing.T) {
	// grpc.Dial is lazy without WithBlock, so this returns a usable handle without a live server.
	conn := CreateGrpcConnection(t, "localhost:53999")
	require.NotNil(t, conn)
}
