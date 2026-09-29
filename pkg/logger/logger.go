package logger

//go:generate mockgen -source logger.go -destination ../../internal/mocks/mock_logger.go -package mocks

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/openfga/openfga/internal/build"
)

type Logger interface {
	// Level reports the minimum enabled log level for this logger. Callers can
	// use it to skip expensive work that only feeds a log line which would be
	// dropped anyway (e.g. l.Level() <= zapcore.InfoLevel).
	Level() zapcore.Level

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

// ctxFieldKey is the key under which the *WithContext methods attach the
// request context. The otelzap bridge and contextFilterCore both detect the
// field by its value's type (context.Context), not by this key.
const ctxFieldKey = "ctx"

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
	fatalHook       func()
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

// WithFatalHook installs a function that runs after a Fatal entry has been
// written, before the process exits. It is used to flush buffered log records
// (e.g. the OTLP provider) that os.Exit would otherwise discard.
func WithFatalHook(hook func()) OptionLogger {
	return func(ol *OptionsLogger) {
		ol.fatalHook = hook
	}
}

type fatalHookFunc func()

func (f fatalHookFunc) OnWrite(*zapcore.CheckedEntry, []zapcore.Field) {
	f()
	os.Exit(1)
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

	// Capture the production sampling policy before disabling the config's
	// built-in sampler. When an OTEL core is attached the sampler must wrap the
	// combined tee (not just stdout), so it is applied below instead.
	sampling := cfg.Sampling
	if logOptions.otelCore != nil {
		cfg.Sampling = nil
	}

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

	var buildOpts []zap.Option
	if logOptions.fatalHook != nil {
		buildOpts = append(buildOpts, zap.WithFatalHook(fatalHookFunc(logOptions.fatalHook)))
	}

	log, err := cfg.Build(buildOpts...)
	if err != nil {
		return nil, err
	}

	var otelCore zapcore.Core
	if logOptions.otelCore != nil {
		// The otelzap core enables all levels by default (filtering is deferred
		// to the OTEL SDK), so raise it to the configured level to keep OTLP
		// export consistent with stdout.
		otelCore, err = zapcore.NewIncreaseLevelCore(logOptions.otelCore, level)
		if err != nil {
			return nil, fmt.Errorf("failed to apply log level to the OTEL core: %w", err)
		}

		// Tee stdout (wrapped to strip the bridge-only context field) and the
		// OTEL core, then wrap the combined core in the production sampler so
		// both sinks receive the identical sampled stream.
		log = log.WithOptions(zap.WrapCore(func(c zapcore.Core) zapcore.Core {
			tee := zapcore.NewTee(&contextFilterCore{Core: c}, otelCore)
			var samplerOpts []zapcore.SamplerOption
			if sampling.Hook != nil {
				samplerOpts = append(samplerOpts, zapcore.SamplerHook(sampling.Hook))
			}
			return zapcore.NewSamplerWithOptions(tee, time.Second, sampling.Initial, sampling.Thereafter, samplerOpts...)
		}))
	}

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

// Check asks the wrapped core whether the entry is admitted, so that its
// level and sampling logic still apply, then registers this core rather than
// the wrapped one so Write can strip context fields before stdout sees them.
func (c *contextFilterCore) Check(entry zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Core.Check(entry, nil) == nil {
		return ce
	}
	return ce.AddCore(entry, c)
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
