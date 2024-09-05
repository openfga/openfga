package logger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestNewLogger(t *testing.T) {
	t.Run("no options", func(t *testing.T) {
		logger, err := NewLogger()
		require.NoError(t, err)
		require.NotNil(t, logger)
	})

	t.Run("with JSON format", func(t *testing.T) {
		logger, err := NewLogger(WithFormat("json"))
		require.NoError(t, err)
		require.NotNil(t, logger)
	})

	t.Run("with Text format", func(t *testing.T) {
		logger, err := NewLogger(WithFormat("text"))
		require.NoError(t, err)
		require.NotNil(t, logger)
	})

	t.Run("with unknown logging level", func(t *testing.T) {
		logger, err := NewLogger(WithLevel("unknown"))
		require.Error(t, err)
		require.Nil(t, logger)
	})

	t.Run("with unknown logging format", func(t *testing.T) {
		logger, err := NewLogger(WithFormat("unknown"))
		require.Error(t, err)
		require.Nil(t, logger)
	})
}

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
		dut, logs := NewObserverLogger("debug")
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
			dut, logs := NewObserverLogger("debug")
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

			expectedZapFields := map[string]interface{}{}
			require.Equal(t, expectedZapFields, actualMessage.ContextMap())
			require.Equal(t, tc.expectedLevel, actualMessage.Level)
		})
	}
}

func TestWithFields(t *testing.T) {
	logger, logs := NewObserverLogger("debug")

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
