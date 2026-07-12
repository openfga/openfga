package logger

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/contrib/bridges/otelzap"
	"go.opentelemetry.io/otel/log/logtest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestWithoutContext(t *testing.T) {
	for _, tc := range []struct {
		name          string
		expectedLevel zapcore.Level
	}{
		{
			name:          "Info",
			expectedLevel: zapcore.InfoLevel,
		},
		{
			name:          "Debug",
			expectedLevel: zapcore.DebugLevel,
		},
		{
			name:          "Warn",
			expectedLevel: zapcore.WarnLevel,
		},
		{
			name:          "Error",
			expectedLevel: zapcore.ErrorLevel,
		},
	} {
		observerLogger, logs := observer.New(zap.DebugLevel)
		dut := ZapLogger{Logger: zap.New(observerLogger)}
		const testMessage = "ABC"
		switch tc.name {
		case "Info":
			dut.Info(testMessage)
		case "Debug":
			dut.Debug(testMessage)
		case "Warn":
			dut.Warn(testMessage)
		case "Error":
			dut.Error(testMessage)
		default:
			t.Errorf("%s: Unknown name", tc.name)
		}
		require.Equal(t, 1, logs.Len())

		actualMessage := logs.All()[0]
		require.Equal(t, testMessage, actualMessage.Message)

		expectedZapFields := map[string]interface{}{}
		require.Equal(t, expectedZapFields, actualMessage.ContextMap())
		require.Equal(t, tc.expectedLevel, actualMessage.Level)
	}
}

func TestWithContext(t *testing.T) {
	for _, tc := range []struct {
		name          string
		expectedLevel zapcore.Level
	}{
		{
			name:          "InfoWithContext",
			expectedLevel: zapcore.InfoLevel,
		},
		{
			name:          "DebugWithContext",
			expectedLevel: zapcore.DebugLevel,
		},
		{
			name:          "WarnWithContext",
			expectedLevel: zapcore.WarnLevel,
		},
		{
			name:          "ErrorWithContext",
			expectedLevel: zapcore.ErrorLevel,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observerLogger, logs := observer.New(zap.DebugLevel)
			dut := ZapLogger{Logger: zap.New(observerLogger)}
			const testMessage = "ABC"
			switch tc.name {
			case "InfoWithContext":
				dut.InfoWithContext(context.Background(), testMessage)
			case "DebugWithContext":
				dut.DebugWithContext(context.Background(), testMessage)
			case "WarnWithContext":
				dut.WarnWithContext(context.Background(), testMessage)
			case "ErrorWithContext":
				dut.ErrorWithContext(context.Background(), testMessage)
			default:
				t.Errorf("%s: Unknown name", tc.name)
			}
			require.Equal(t, 1, logs.Len())

			actualMessage := logs.All()[0]
			require.Equal(t, testMessage, actualMessage.Message)
			require.Equal(t, tc.expectedLevel, actualMessage.Level)

			// Without an OTEL core the context must not be attached as a field.
			expectedZapFields := map[string]interface{}{}
			require.Equal(t, expectedZapFields, actualMessage.ContextMap())
		})
	}
}

func TestContextFilterCore(t *testing.T) {
	observerCore, logs := observer.New(zap.DebugLevel)
	filtered := &contextFilterCore{Core: observerCore}

	log := zap.New(filtered)
	log.Info("test",
		zap.String("keep", "value"),
		zap.Any("ctx", context.Background()),
		zap.Any("otherCtx", context.TODO()),
	)

	require.Equal(t, 1, logs.Len())

	entry := logs.All()[0]
	require.Equal(t, "test", entry.Message)

	// Context fields are stripped regardless of key; other fields remain.
	contextMap := entry.ContextMap()
	require.Contains(t, contextMap, "keep")
	require.NotContains(t, contextMap, "ctx")
	require.NotContains(t, contextMap, "otherCtx")

	// A non-context field is kept even when keyed "ctx".
	log.Info("test2", zap.String("ctx", "not-a-context"))
	require.Equal(t, 2, logs.Len())
	require.Contains(t, logs.All()[1].ContextMap(), "ctx")
}

func TestFilterContextFieldsNoCopy(t *testing.T) {
	fields := []zapcore.Field{
		zap.String("a", "1"),
		zap.String("b", "2"),
	}

	// With no context field present, the input slice is returned as-is.
	filtered := filterContextFields(fields)
	require.Equal(t, &fields[0], &filtered[0])
	require.Len(t, filtered, 2)
}

func TestContextFilterCoreWith(t *testing.T) {
	observerCore, logs := observer.New(zap.DebugLevel)
	filtered := &contextFilterCore{Core: observerCore}

	// With should also strip "ctx" from persistent fields.
	log := zap.New(filtered).With(
		zap.String("persistent", "yes"),
		zap.Any("ctx", context.Background()),
	)
	log.Info("test")

	require.Equal(t, 1, logs.Len())

	contextMap := logs.All()[0].ContextMap()
	require.Contains(t, contextMap, "persistent")
	require.NotContains(t, contextMap, "ctx")
}

// newOTELTestLogger builds a logger via NewLogger with an otelzap core backed
// by a logtest recorder, writing stdout output to a temp file to keep test
// output clean.
func newOTELTestLogger(t *testing.T, level string) (*ZapLogger, *logtest.Recorder) {
	t.Helper()

	recorder := logtest.NewRecorder()
	core := otelzap.NewCore("openfga", otelzap.WithLoggerProvider(recorder))

	l, err := NewLogger(
		WithLevel(level),
		WithOTELCore(core),
		WithOutputPaths(filepath.Join(t.TempDir(), "out.log")),
	)
	require.NoError(t, err)

	return l, recorder
}

func allRecords(recording logtest.Recording) []logtest.Record {
	var records []logtest.Record
	for _, recs := range recording {
		records = append(records, recs...)
	}
	return records
}

func TestOTELCoreReceivesSpanContext(t *testing.T) {
	l, recorder := newOTELTestLogger(t, "info")

	spanCtx := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    trace.TraceID{0x01},
		SpanID:     trace.SpanID{0x02},
		TraceFlags: trace.FlagsSampled,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), spanCtx)

	l.InfoWithContext(ctx, "hello")

	records := allRecords(recorder.Result())
	require.Len(t, records, 1)

	// The bridge must emit the record with the request context so the SDK can
	// attach the trace/span IDs for log-trace correlation.
	gotSpanCtx := trace.SpanContextFromContext(records[0].Context)
	require.Equal(t, spanCtx.TraceID(), gotSpanCtx.TraceID())
	require.Equal(t, spanCtx.SpanID(), gotSpanCtx.SpanID())
}

func TestOTELCoreRespectsLogLevel(t *testing.T) {
	l, recorder := newOTELTestLogger(t, "warn")

	l.InfoWithContext(context.Background(), "below the configured level")
	require.Empty(t, allRecords(recorder.Result()))

	l.WarnWithContext(context.Background(), "at the configured level")
	require.Len(t, allRecords(recorder.Result()), 1)
}

// countLines returns the number of log lines written to the given file.
func countLines(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return strings.Count(string(data), "\n")
}

func TestSamplingDropsRepeatedEntries(t *testing.T) {
	out := filepath.Join(t.TempDir(), "out.log")
	l, err := NewLogger(WithLevel("info"), WithOutputPaths(out))
	require.NoError(t, err)

	const total = 500
	for i := 0; i < total; i++ {
		l.Info("repeated message")
	}

	// Repeated identical messages beyond the sampler's budget must be dropped,
	// matching zap's production behavior.
	lines := countLines(t, out)
	require.GreaterOrEqual(t, lines, samplingInitial)
	require.Less(t, lines, total)
}

func TestSamplingAppliesToOTELCore(t *testing.T) {
	recorder := logtest.NewRecorder()
	core := otelzap.NewCore("openfga", otelzap.WithLoggerProvider(recorder))

	out := filepath.Join(t.TempDir(), "out.log")
	l, err := NewLogger(WithLevel("info"), WithOTELCore(core), WithOutputPaths(out))
	require.NoError(t, err)

	const total = 500
	for i := 0; i < total; i++ {
		l.InfoWithContext(context.Background(), "repeated message")
	}

	// The sampler wraps the tee, so the keep/drop decision is shared: the OTEL
	// core must be sampled too, and both streams must be identical.
	records := len(allRecords(recorder.Result()))
	require.Less(t, records, total)
	require.Equal(t, countLines(t, out), records)
}

func TestWithFields(t *testing.T) {
	observerLogger, logs := observer.New(zap.DebugLevel)
	logger := ZapLogger{Logger: zap.New(observerLogger)}

	const testMessage = "ABC"

	newLogger := logger.With(
		zap.String("TestOption", "Message"),
	)

	newLogger.Info(testMessage)

	// Check that child message carries the context fields
	expectedZapFields := map[string]interface{}{
		"TestOption": "Message",
	}
	childMessage := logs.All()[0]
	require.Equal(t, expectedZapFields, childMessage.ContextMap())

	// Check that parent message does not carry the context fields
	logger.Info(testMessage)
	parentMessage := logs.All()[1]
	require.Empty(t, parentMessage.ContextMap())
}
