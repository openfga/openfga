package validator

import (
	"testing"

	"github.com/openfga/openfga/pkg/testutils"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

var contextProtoSize int

func BenchmarkContextSize(b *testing.B) {

	context, err := structpb.NewStruct(map[string]interface{}{
		"s": testutils.CreateRandomString(256 * 1_024),
	})
	require.NoError(b, err)

	bytes, err := proto.Marshal(context)
	require.NoError(b, err)

	require.Equal(b, proto.Size(context), len(bytes))

	var protoSizeIterations int
	var protoMarshalIterations int

	b.Run("Proto.Size", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			contextProtoSize = proto.Size(context)
		}

		protoSizeIterations = b.N
	})

	b.Run("Proto.Marshal", func(b *testing.B) {
		b.ResetTimer()
		for n := 0; n < b.N; n++ {
			bytes, _ := proto.Marshal(context)
			contextProtoSize = len(bytes)
		}

		protoMarshalIterations = b.N
	})

	require.Greater(b, protoSizeIterations, protoMarshalIterations)
}
