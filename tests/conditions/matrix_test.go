package conditions

import (
	"testing"

	openfgav1 "github.com/openfga/api/proto/openfga/v1"

	"github.com/openfga/openfga/pkg/server/config"
	"github.com/openfga/openfga/pkg/testutils"
	"github.com/openfga/openfga/tests"
)

// Engine is one server configuration the matrix runs against.
type Engine struct {
	Name          string
	Experimentals []string
	Caches        bool
}

// Engines lists the server configurations. Every one enables dynamic conditions.
var Engines = []Engine{
	{Name: "default"},
	{Name: "check-optimizations", Experimentals: []string{config.ExperimentalCheckOptimizations, config.ExperimentalListObjectsOptimizations}},
	{Name: "weighted-graph", Experimentals: []string{config.ExperimentalWeightedGraphCheck}},
	{Name: "pipeline", Experimentals: []string{config.ExperimentalPipelineListObjects, config.ExperimentalPipelineOptimizations}},
	{Name: "caches", Caches: true},
}

// startEngine starts a server with the in-memory datastore.
func startEngine(t *testing.T, e Engine) Client {
	t.Helper()
	cfg := testutils.MustDefaultConfigForParallelTests()
	cfg.Experimentals = append(cfg.Experimentals, config.ExperimentalInlineExpressions)
	cfg.Experimentals = append(cfg.Experimentals, e.Experimentals...)
	cfg.Log.Level = "none"
	cfg.Datastore.Engine = "memory"
	cfg.ListObjectsDeadline = 0
	cfg.ListUsersDeadline = 0
	if e.Caches {
		cfg.CheckQueryCache.Enabled = true
		cfg.CheckIteratorCache.Enabled = true
		cfg.ListObjectsIteratorCache.Enabled = true
		cfg.SharedIterator.Enabled = true
	}
	tests.StartServer(t, cfg)
	return openfgav1.NewOpenFGAServiceClient(testutils.CreateGrpcConnection(t, cfg.GRPC.Addr))
}

func TestLayer1OneTupleMemory(t *testing.T) {
	for _, e := range Engines {
		t.Run(e.Name, func(t *testing.T) {
			client := startEngine(t, e)
			opts := RunOptions{}
			if e.Name != "default" {
				opts.Sample = 7 // the conversion rules do not depend on the engine
			}
			for _, kind := range []Kind{KindExpression, KindNamed} {
				out := newOutcome()
				for _, g := range Layer1(kind) {
					out.merge(RunGroup(t, client, e.Name, g, opts))
				}
				out.Report(t, "layer 1 "+string(kind))
			}
		})
	}
}

func TestLayer2CompositionMemory(t *testing.T) {
	for _, e := range Engines {
		t.Run(e.Name, func(t *testing.T) {
			client := startEngine(t, e)
			out := newOutcome()
			out.merge(RunGroup(t, client, e.Name, Layer2(KindExpression, secondNone), RunOptions{}))
			out.merge(RunGroup(t, client, e.Name, Layer2(KindNamed, secondNone), RunOptions{}))
			out.merge(RunGroup(t, client, e.Name, Layer2(KindExpression, secondBefore), RunOptions{}))
			out.merge(RunGroup(t, client, e.Name, Layer2(KindExpression, secondAfter), RunOptions{}))
			out.Report(t, "layer 2")
		})
	}
}

func TestLayer0WriteMemory(t *testing.T) {
	client := startEngine(t, Engines[0])
	RunLayer0(t, client, Engines[0].Name).Report(t, "layer 0")
}

func TestLayer3LifecycleMemory(t *testing.T) {
	for _, e := range Engines {
		t.Run(e.Name, func(t *testing.T) {
			RunLayer3(t, startEngine(t, e))
		})
	}
}
