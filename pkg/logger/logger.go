package logger

import (
	"context"
	"fmt"
	"time"

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

// ctxFieldKey names the field that carries a context.Context through zap to
// the otelzap bridge, which uses it to extract trace/span IDs. The bridge
// detects the field by its value's type, not by this key; contextFilterCore
// strips it from stdout output the same way.
const ctxFieldKey = "ctx"

// Sampling parameters mirroring zap.NewProductionConfig: per message string
// and per samplingTick, the first samplingInitial entries pass and then only
// every samplingThereafter-th does.
const (
	samplingTick       = time.Second
	samplingInitial    = 100
	samplingThereafter = 100
)

// NewNoopLogger provides a noop logger.
func NewNoopLogger() *ZapLogger {
	return &ZapLogger{
		Logger: zap.NewNop(),
	}
}

// ZapLogger is an implementation of Logger that uses the uber/zap logger underneath.
// It provides additional methods such as ones that logs based on context.
type ZapLogger struct {
	*zap.Logger

	// hasOTELCore is true when an otelzap bridge core is teed into the logger.
	// Only then do the *WithContext methods attach the context as a field, so
	// that the default (stdout-only) path pays no extra cost.
	hasOTELCore bool
}

var _ Logger = (*ZapLogger)(nil)

// With creates a child logger and adds structured context to it. Fields added
// to the child don't affect the parent, and vice versa. Any fields that
// require evaluation (such as Objects) are evaluated upon invocation of With.
func (l *ZapLogger) With(fields ...zap.Field) Logger {
	return &ZapLogger{Logger: l.Logger.With(fields...), hasOTELCore: l.hasOTELCore}
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

// withContextField attaches ctx as a field for the otelzap bridge to consume.
// It is a no-op unless an OTEL core is configured.
func (l *ZapLogger) withContextField(ctx context.Context, fields []zap.Field) []zap.Field {
	if !l.hasOTELCore {
		return fields
	}
	return append(fields, zap.Any(ctxFieldKey, ctx))
}

func (l *ZapLogger) DebugWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Debug(msg, l.withContextField(ctx, fields)...)
}

func (l *ZapLogger) InfoWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Info(msg, l.withContextField(ctx, fields)...)
}

func (l *ZapLogger) WarnWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Warn(msg, l.withContextField(ctx, fields)...)
}

func (l *ZapLogger) ErrorWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	fields = append(fields, ctxzap.TagsToFields(ctx)...)
	l.Logger.Error(msg, l.withContextField(ctx, fields)...)
}

func (l *ZapLogger) PanicWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Panic(msg, l.withContextField(ctx, fields)...)
}

func (l *ZapLogger) FatalWithContext(ctx context.Context, msg string, fields ...zap.Field) {
	l.Logger.Fatal(msg, l.withContextField(ctx, fields)...)
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
// is wrapped with a contextFilterCore that strips context.Context fields
// which are only meaningful to the OTEL bridge.
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
	// Disable the built-in sampler wrap: it would cover only the stdout core.
	// Sampling is re-applied below, around the final core, so that it also
	// covers the OTEL core when one is teed in.
	cfg.Sampling = nil
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

	if logOptions.otelCore != nil {
		// The otelzap core enables all levels by default (filtering is deferred
		// to the OTEL SDK), so raise it to the configured level to keep OTLP
		// export consistent with stdout.
		otelCore, err := zapcore.NewIncreaseLevelCore(logOptions.otelCore, level)
		if err != nil {
			return nil, fmt.Errorf("failed to apply log level to the OTEL core: %w", err)
		}
		// The stdout core is wrapped with contextFilterCore to strip the
		// context fields added by the *WithContext methods for the otelzap
		// bridge; without this, context.Context objects would be serialized
		// to stdout.
		log = log.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
			return zapcore.NewTee(&contextFilterCore{Core: c}, otelCore)
		}))
	}

	// The sampler wraps the outermost core — the tee, when OTLP export is on —
	// so the keep/drop decision is made once, before fan-out, and stdout and
	// OTLP receive the same sampled stream.
	log = log.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
		return zapcore.NewSamplerWithOptions(c, samplingTick, samplingInitial, samplingThereafter)
	}))

	if logOptions.format == "json" {
		log = log.With(zap.String("build.version", build.Version), zap.String("build.commit", build.Commit))
	}

	return &ZapLogger{Logger: log, hasOTELCore: logOptions.otelCore != nil}, nil
}

// contextFilterCore wraps a zapcore.Core and strips context.Context fields
// before passing entries to the underlying core. Such fields carry the request
// context for the otelzap bridge (which detects them by type) to extract
// trace/span IDs; the stdout core does not need them and would otherwise
// serialize a meaningless object.
type contextFilterCore struct {
	zapcore.Core
}

func (c *contextFilterCore) With(fields []zapcore.Field) zapcore.Core {
	return &contextFilterCore{Core: c.Core.With(filterContextFields(fields))}
}

// Check gates on the level alone rather than delegating to the wrapped core's
// Check. That is only sound while the wrapped core's own Check has no
// additional drop logic: NewLogger wraps the plain stdout core here and
// applies the sampler outside the tee.
func (c *contextFilterCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return ce.AddCore(entry, c)
	}
	return ce
}

func (c *contextFilterCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	return c.Core.Write(entry, filterContextFields(fields))
}

// isContextField mirrors the otelzap bridge's detection of context fields:
// any field whose value is a context.Context, regardless of key.
func isContextField(f zapcore.Field) bool {
	_, ok := f.Interface.(context.Context)
	return ok
}

func filterContextFields(fields []zapcore.Field) []zapcore.Field {
	needsFilter := false
	for _, f := range fields {
		if isContextField(f) {
			needsFilter = true
			break
		}
	}
	// Most entries carry no context field; avoid copying in that case.
	if !needsFilter {
		return fields
	}

	filtered := make([]zapcore.Field, 0, len(fields)-1)
	for _, f := range fields {
		if !isContextField(f) {
			filtered = append(filtered, f)
		}
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
