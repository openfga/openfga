package logger

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestNewNoopLogger(t *testing.T) {
	l := NewNoopLogger()
	require.NotNil(t, l)
	// Should not panic when used.
	l.Info("noop")
}

func TestNewLogger_Formats(t *testing.T) {
	t.Run("text_format", func(t *testing.T) {
		l, err := NewLogger(WithFormat("text"), WithLevel("info"))
		require.NoError(t, err)
		require.NotNil(t, l)
	})

	t.Run("json_format_with_iso8601", func(t *testing.T) {
		l, err := NewLogger(WithFormat("json"), WithLevel("debug"), WithTimestampFormat("ISO8601"))
		require.NoError(t, err)
		require.NotNil(t, l)
	})

	t.Run("json_format_with_unix_timestamp", func(t *testing.T) {
		l, err := NewLogger(WithFormat("json"), WithTimestampFormat("Unix"))
		require.NoError(t, err)
		require.NotNil(t, l)
	})

	t.Run("level_none_returns_noop", func(t *testing.T) {
		l, err := NewLogger(WithLevel("none"))
		require.NoError(t, err)
		require.NotNil(t, l)
	})

	t.Run("invalid_level_errors", func(t *testing.T) {
		_, err := NewLogger(WithLevel("not-a-level"))
		require.Error(t, err)
	})

	t.Run("with_output_paths", func(t *testing.T) {
		logFile := filepath.Join(t.TempDir(), "out.log")
		l, err := NewLogger(WithFormat("json"), WithOutputPaths(logFile))
		require.NoError(t, err)
		require.NotNil(t, l)
		l.Info("written to file")
	})
}

func TestMustNewLogger(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		l := MustNewLogger("text", "info", "ISO8601")
		require.NotNil(t, l)
	})

	t.Run("panics_on_invalid_level", func(t *testing.T) {
		require.Panics(t, func() {
			MustNewLogger("text", "invalid-level", "ISO8601")
		})
	})
}

func TestZapLogger_PanicAndFatal(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)

	t.Run("panic_logs_and_panics", func(t *testing.T) {
		l := &ZapLogger{zap.New(core)}
		require.Panics(t, func() {
			l.Panic("panic message")
		})
		require.Equal(t, 1, logs.FilterMessage("panic message").Len())
	})

	t.Run("panic_with_context", func(t *testing.T) {
		l := &ZapLogger{zap.New(core)}
		require.Panics(t, func() {
			l.PanicWithContext(context.Background(), "panic ctx message")
		})
		require.Equal(t, 1, logs.FilterMessage("panic ctx message").Len())
	})

	t.Run("fatal_logs_and_aborts", func(t *testing.T) {
		// WriteThenPanic makes Fatal panic instead of calling os.Exit, so it is testable.
		l := &ZapLogger{zap.New(core, zap.WithFatalHook(zapcore.WriteThenPanic))}
		require.Panics(t, func() {
			l.Fatal("fatal message")
		})
		require.Equal(t, 1, logs.FilterMessage("fatal message").Len())
	})

	t.Run("fatal_with_context", func(t *testing.T) {
		l := &ZapLogger{zap.New(core, zap.WithFatalHook(zapcore.WriteThenPanic))}
		require.Panics(t, func() {
			l.FatalWithContext(context.Background(), "fatal ctx message")
		})
		require.Equal(t, 1, logs.FilterMessage("fatal ctx message").Len())
	})
}

func TestZapLogger_ErrorWithContext_AppendsCtxTags(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	l := &ZapLogger{zap.New(core)}

	// A plain context has no grpc tags, so TagsToFields contributes nothing,
	// but the field is still logged and the ctx-tag append path is exercised.
	l.ErrorWithContext(context.Background(), "error with tags", zap.String("explicit", "field"))

	entries := logs.FilterMessage("error with tags").All()
	require.Len(t, entries, 1)
	require.Equal(t, zapcore.ErrorLevel, entries[0].Level)
	require.Equal(t, map[string]interface{}{"explicit": "field"}, entries[0].ContextMap())
}

func TestZapLogger_WarnAndPanicLevelMethods(t *testing.T) {
	core, logs := observer.New(zap.DebugLevel)
	l := &ZapLogger{zap.New(core)}

	l.WarnWithContext(context.Background(), "warn ctx")
	require.Equal(t, zapcore.WarnLevel, logs.All()[0].Level)
}
