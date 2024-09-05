package logger

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/openfga/openfga/internal/build"
)

type Field = zap.Field

var (
	Any         = zap.Any
	Skip        = zap.Skip
	Binary      = zap.Binary
	Bool        = zap.Bool
	Boolp       = zap.Boolp
	ByteString  = zap.ByteString
	Complex128  = zap.Complex128
	Complex128p = zap.Complex128p
	Complex64   = zap.Complex64
	Complex64p  = zap.Complex64p
	Float64     = zap.Float64
	Float64p    = zap.Float64p
	Float32     = zap.Float32
	Int         = zap.Int
	Intp        = zap.Intp
	Int64       = zap.Int64
	Int64p      = zap.Int64p
	Int32       = zap.Int32
	Int32p      = zap.Int32p
	Int16       = zap.Int16
	Int16p      = zap.Int16p
	Int8        = zap.Int8
	Int8p       = zap.Int8p
	String      = zap.String
	Stringp     = zap.Stringp
	Uint        = zap.Uint
	Uintp       = zap.Uintp
	Uint64      = zap.Uint64
	Uint64p     = zap.Uint64p
	Uint32      = zap.Uint32
	Uint32p     = zap.Uint32p
	Uint16      = zap.Uint16
	Uint16p     = zap.Uint16p
	Uint8       = zap.Uint8
	Uint8p      = zap.Uint8p
	Uintptr     = zap.Uintptr
	Reflect     = zap.Reflect
	Namespace   = zap.Namespace
	Stringer    = zap.Stringer
	Time        = zap.Time
	Timep       = zap.Timep
	Stack       = zap.Stack
	StackSkip   = zap.StackSkip
	Duration    = zap.Duration
	Durationp   = zap.Durationp
	Object      = zap.Object
	Inline      = zap.Inline
	Dict        = zap.Dict
	Error       = zap.Error
	NamedError  = zap.NamedError
)

type Logger interface {
	// These are ops that call directly to the actual zap implementation
	Debug(string, ...Field)
	Info(string, ...Field)
	Warn(string, ...Field)
	Error(string, ...Field)
	Panic(string, ...Field)
	Fatal(string, ...Field)
	With(...Field) Logger

	// These are the equivalent logger function but with context provided
	DebugWithContext(context.Context, string, ...Field)
	InfoWithContext(context.Context, string, ...Field)
	WarnWithContext(context.Context, string, ...Field)
	ErrorWithContext(context.Context, string, ...Field)
	PanicWithContext(context.Context, string, ...Field)
	FatalWithContext(context.Context, string, ...Field)
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
	delegate *zap.Logger
}

var _ Logger = (*ZapLogger)(nil)

// With creates a child logger and adds structured context to it. Fields added
// to the child don't affect the parent, and vice versa. Any fields that
// require evaluation (such as Objects) are evaluated upon invocation of With.
func (l *ZapLogger) With(fields ...Field) Logger {
	return &ZapLogger{l.delegate.With(fields...)}
}

func (l *ZapLogger) Debug(msg string, fields ...Field) {
	l.delegate.Debug(msg, fields...)
}

func (l *ZapLogger) Info(msg string, fields ...Field) {
	l.delegate.Info(msg, fields...)
}

func (l *ZapLogger) Warn(msg string, fields ...Field) {
	l.delegate.Warn(msg, fields...)
}

func (l *ZapLogger) Error(msg string, fields ...Field) {
	l.delegate.Error(msg, fields...)
}

func (l *ZapLogger) Panic(msg string, fields ...Field) {
	l.delegate.Panic(msg, fields...)
}

func (l *ZapLogger) Fatal(msg string, fields ...Field) {
	l.delegate.Fatal(msg, fields...)
}

func (l *ZapLogger) DebugWithContext(ctx context.Context, msg string, fields ...Field) {
	l.delegate.Debug(msg, fields...)
}

func (l *ZapLogger) InfoWithContext(ctx context.Context, msg string, fields ...Field) {
	l.delegate.Info(msg, fields...)
}

func (l *ZapLogger) WarnWithContext(ctx context.Context, msg string, fields ...Field) {
	l.delegate.Warn(msg, fields...)
}

func (l *ZapLogger) ErrorWithContext(ctx context.Context, msg string, fields ...Field) {
	l.delegate.Error(msg, fields...)
}

func (l *ZapLogger) PanicWithContext(ctx context.Context, msg string, fields ...Field) {
	l.delegate.Panic(msg, fields...)
}

func (l *ZapLogger) FatalWithContext(ctx context.Context, msg string, fields ...Field) {
	l.delegate.Fatal(msg, fields...)
}

// OptionsLogger Implements options for logger.
type OptionsLogger struct {
	format          string
	level           string
	timestampFormat string
	outputPaths     []string
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

// NewLogger creates a new logger with the given options.
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

	switch logOptions.format {
	case "text":
		cfg.Encoding = "console"
		cfg.DisableCaller = true
		cfg.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	case "json":
		cfg.EncoderConfig.EncodeTime = zapcore.EpochTimeEncoder // default in json for backward compatibility
		if logOptions.timestampFormat == "ISO8601" {
			cfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
		}
	default:
		return nil, fmt.Errorf("unknown log format: %s", logOptions.format)
	}

	log, err := cfg.Build()
	if err != nil {
		return nil, fmt.Errorf("building config: %w", err)
	}

	if logOptions.format == "json" {
		log = log.With(zap.String("build.version", build.Version), zap.String("build.commit", build.Commit))
	}

	return &ZapLogger{log}, nil
}

// MustNewLogger creates a new logger with the given options and panics if an error occurs.
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
