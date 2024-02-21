package logger

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/openfga/openfga/internal/build"
)

type Logger interface {
	// These are ops that call directly to the actual zap implementation
	Debug(string, ...zap.Field)
	Info(string, ...zap.Field)
	Warn(string, ...zap.Field)
	Error(string, ...zap.Field)
	Panic(string, ...zap.Field)
	Fatal(string, ...zap.Field)

	// These are the equivalent logger function but with context provided
	DebugWithContext(context.Context, string, ...zap.Field)
	InfoWithContext(context.Context, string, ...zap.Field)
	WarnWithContext(context.Context, string, ...zap.Field)
	ErrorWithContext(context.Context, string, ...zap.Field)
	PanicWithContext(context.Context, string, ...zap.Field)
	FatalWithContext(context.Context, string, ...zap.Field)
}

// NewNoopLogger provides a noop logger.
func NewNoopLogger() *ZapLogger {
	return &ZapLogger{
		zap.NewNop(),
	}
}

// ZapLogger is an implementation of Logger that uses the uber/zap logger underneath.
// It provides additional methods such as ones that logs based on context.
type ZapLogger struct {
	*zap.Logger
}

var _ Logger = (*ZapLogger)(nil)

func (l *ZapLogger) With(fields ...zap.Field) {
	l.Logger = l.Logger.With(fields...)
}

func (l *ZapLogger) Debug(msg string, fields ...zap.Field) {
	l.Logger.Debug(msg, fields...)
}

func (l *ZapLogger) Info(msg string, fields ...zap.Field) {
	l.Logger.Info(msg, fields...)
}

func (l *ZapLogger) Warn(msg string, fields ...zap.Field) {
	l.Logger.Warn(msg, fields...)
}

func (l *ZapLogger) Error(msg string, fields ...zap.Field) {
	l.Logger.Error(msg, fields...)
}

func (l *ZapLogger) Panic(msg string, fields ...zap.Field) {
	l.Logger.Panic(msg, fields...)
}

func (l *ZapLogger) Fatal(msg string, fields ...zap.Field) {
	l.Logger.Fatal(msg, fields...)
}

func (l *ZapLogger) DebugWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Debug(msg, fields...)
}

func (l *ZapLogger) InfoWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Info(msg, fields...)
}

func (l *ZapLogger) WarnWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Warn(msg, fields...)
}

func (l *ZapLogger) ErrorWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Error(msg, fields...)
}

func (l *ZapLogger) PanicWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Panic(msg, fields...)
}

func (l *ZapLogger) FatalWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Fatal(msg, fields...)
}

func NewLogger(logFormat, logLevel, logTimestampFormat string) (*ZapLogger, error) {
	if logLevel == "none" {
		return NewNoopLogger(), nil
	}

	level, err := zap.ParseAtomicLevel(logLevel)
	if err != nil {
		return nil, fmt.Errorf("unknown log level: %s, error: %w", logLevel, err)
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = level
	cfg.EncoderConfig.TimeKey = "timestamp"
	cfg.EncoderConfig.CallerKey = "" // remove the "caller" field
	cfg.DisableStacktrace = true

	if logFormat == "text" {
		cfg.Encoding = "console"
		cfg.DisableCaller = true
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else { // Json
		cfg.EncoderConfig.EncodeTime = zapcore.EpochTimeEncoder // default in json for backward compatibility
		if logTimestampFormat == "ISO8601" {
			cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		}
	}

	log, err := cfg.Build()
	if err != nil {
		return nil, err
	}

	if logFormat == "json" {
		log = log.With(zap.String("build.version", build.Version), zap.String("build.commit", build.Commit))
	}

	return &ZapLogger{log}, nil
}

func MustNewLogger(logFormat, logLevel, logTimestampFormat string) *ZapLogger {
	logger, err := NewLogger(logFormat, logLevel, logTimestampFormat)
	if err != nil {
		panic(err)
	}

	return logger
}
