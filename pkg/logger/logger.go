package logger

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
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
	With(...zap.Field) Logger

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

// With creates a child logger and adds structured context to it. Fields added
// to the child don't affect the parent, and vice versa. Any fields that
// require evaluation (such as Objects) are evaluated upon invocation of With.
func (l *ZapLogger) With(fields ...zap.Field) Logger {
	return &ZapLogger{l.Logger.With(fields...)}
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
	fields = append(fields, zap.Any("ctx", ctx))
	l.Logger.Debug(msg, fields...)
}

func (l *ZapLogger) InfoWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.Any("ctx", ctx))
	l.Logger.Info(msg, fields...)
}

func (l *ZapLogger) WarnWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.Any("ctx", ctx))
	l.Logger.Warn(msg, fields...)
}

func (l *ZapLogger) ErrorWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, ctxzap.TagsToFields(ctx)...)
	fields = append(fields, zap.Any("ctx", ctx))
	l.Logger.Error(msg, fields...)
}

func (l *ZapLogger) PanicWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.Any("ctx", ctx))
	l.Logger.Panic(msg, fields...)
}

func (l *ZapLogger) FatalWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, zap.Any("ctx", ctx))
	l.Logger.Fatal(msg, fields...)
}

// OptionsLogger Implements options for logger.
type OptionsLogger struct {
	format          string
	level           string
	timestampFormat string
	outputPaths     []string
	otelCore        zapcore.Core
}

type OptionLogger func(ol *OptionsLogger)

func WithFormat(format string) OptionLogger {
	return func(ol *OptionsLogger) {
		ol.format = format
	}
}

func WithLevel(level string) OptionLogger {
	return func(ol *OptionsLogger) {
		ol.level = level
	}
}

func WithTimestampFormat(timestampFormat string) OptionLogger {
	return func(ol *OptionsLogger) {
		ol.timestampFormat = timestampFormat
	}
}

// WithOutputPaths sets a list of URLs or file paths to write logging output to.
//
// URLs with the "file" scheme must use absolute paths on the local filesystem.
// No user, password, port, fragments, or query parameters are allowed, and the
// hostname must be empty or "localhost".
//
// Since it's common to write logs to the local filesystem, URLs without a scheme
// (e.g., "/var/log/foo.log") are treated as local file paths. Without a scheme,
// the special paths "stdout" and "stderr" are interpreted as os.Stdout and os.Stderr.
// When specified without a scheme, relative file paths also work.
//
// Defaults to "stdout".
func WithOutputPaths(paths ...string) OptionLogger {
	return func(ol *OptionsLogger) {
		ol.outputPaths = paths
	}
}

// WithOTELCore adds an additional zapcore.Core (typically an otelzap bridge)
// that receives a copy of every log entry via zapcore.NewTee. The stdout core
// is wrapped with a contextFilterCore that strips "ctx" fields which are only
// meaningful to the OTEL bridge.
func WithOTELCore(core zapcore.Core) OptionLogger {
	return func(ol *OptionsLogger) {
		ol.otelCore = core
	}
}

func NewLogger(options ...OptionLogger) (*ZapLogger, error) {
	logOptions := &OptionsLogger{
		level:           "info",
		format:          "text",
		timestampFormat: "ISO8601",
		outputPaths:     []string{"stdout"},
	}

	for _, opt := range options {
		opt(logOptions)
	}

	if logOptions.level == "none" {
		return NewNoopLogger(), nil
	}

	level, err := zap.ParseAtomicLevel(logOptions.level)
	if err != nil {
		return nil, fmt.Errorf("unknown log level: %s, error: %w", logOptions.level, err)
	}

	cfg := zap.NewProductionConfig()
	cfg.Level = level
	cfg.OutputPaths = logOptions.outputPaths
	cfg.EncoderConfig.TimeKey = "timestamp"
	cfg.EncoderConfig.CallerKey = "" // remove the "caller" field
	cfg.DisableStacktrace = true

	if logOptions.format == "text" {
		cfg.Encoding = "console"
		cfg.DisableCaller = true
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else { // Json
		cfg.EncoderConfig.EncodeTime = zapcore.EpochTimeEncoder // default in json for backward compatibility
		if logOptions.timestampFormat == "ISO8601" {
			cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		}
	}

	log, err := cfg.Build()
	if err != nil {
		return nil, err
	}

	// Always wrap the stdout core with contextFilterCore to strip "ctx" fields
	// that are added by *WithContext methods. Without this, context.Context
	// objects would be serialized to stdout.
	if logOptions.otelCore != nil {
		stdoutCore := &contextFilterCore{Core: log.Core()}
		log = log.WithOptions(zap.WrapCore(func(_ zapcore.Core) zapcore.Core {
			return zapcore.NewTee(stdoutCore, logOptions.otelCore)
		}))
	} else {
		log = log.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
			return &contextFilterCore{Core: c}
		}))
	}

	if logOptions.format == "json" {
		log = log.With(zap.String("build.version", build.Version), zap.String("build.commit", build.Commit))
	}

	return &ZapLogger{log}, nil
}

// contextFilterCore wraps a zapcore.Core and strips fields keyed "ctx" before
// passing entries to the underlying core. The "ctx" field carries a
// context.Context for the otelzap bridge to extract trace/span IDs; the stdout
// core does not need it and would otherwise serialize a meaningless object.
type contextFilterCore struct {
	zapcore.Core
}

func (c *contextFilterCore) With(fields []zapcore.Field) zapcore.Core {
	return &contextFilterCore{Core: c.Core.With(filterContextFields(fields))}
}

func (c *contextFilterCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Core.Enabled(entry.Level) {
		return ce.AddCore(entry, c)
	}
	return ce
}

func (c *contextFilterCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return c.Core.Write(entry, filterContextFields(fields))
}

func filterContextFields(fields []zapcore.Field) []zapcore.Field {
	filtered := fields[:0:0]
	for _, f := range fields {
		if f.Key == "ctx" {
			continue
		}
		filtered = append(filtered, f)
	}
	return filtered
}

func MustNewLogger(logFormat, logLevel, logTimestampFormat string) *ZapLogger {
	logger, err := NewLogger(
		WithFormat(logFormat),
		WithLevel(logLevel),
		WithTimestampFormat(logTimestampFormat))
	if err != nil {
		panic(err)
	}

	return logger
}
