package logger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
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
		dut := ZapLogger{zap.New(observerLogger)}
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
			dut := ZapLogger{zap.New(observerLogger)}
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

			// The "ctx" field is present (for otelzap bridge consumption).
			require.Contains(t, actualMessage.ContextMap(), "ctx")
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
	)

	require.Equal(t, 1, logs.Len())

	entry := logs.All()[0]
	require.Equal(t, "test", entry.Message)

	// "ctx" should be stripped, "keep" should remain.
	contextMap := entry.ContextMap()
	require.Contains(t, contextMap, "keep")
	require.NotContains(t, contextMap, "ctx")
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

func TestWithFields(t *testing.T) {
	observerLogger, logs := observer.New(zap.DebugLevel)
	logger := ZapLogger{zap.New(observerLogger)}

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
