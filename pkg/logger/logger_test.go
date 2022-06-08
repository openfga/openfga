package logger

import (
	"context"
	"reflect"
	"testing"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestMessage(t *testing.T) {
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
		if logs.Len() != 1 {
			t.Errorf("%s: Expected log length to be 1, actual %d", tc.name, logs.Len())
		}
		actualMessage := logs.All()[0]
		if actualMessage.Message != testMessage {
			t.Errorf("%s: Expected message to be %s, actual %s", tc.name, testMessage, actualMessage.LoggerName)
		}
		expectedZapFields := map[string]interface{}{}
		if !reflect.DeepEqual(actualMessage.ContextMap(), expectedZapFields) {
			t.Errorf("%s: Expected zap fields %v actual %v", tc.name, expectedZapFields, actualMessage.ContextMap())
		}
		if actualMessage.Level != tc.expectedLevel {
			t.Errorf("%s: Expected message level to be %d, actual %d", tc.name, tc.expectedLevel, actualMessage.Level)

		}

	}
}

func TestMessageWithEmptyId(t *testing.T) {
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
		if logs.Len() != 1 {
			t.Errorf("%s: Expected log length to be 1, actual %d", tc.name, logs.Len())
		}
		actualMessage := logs.All()[0]
		if actualMessage.Message != testMessage {
			t.Errorf("%s: Expected message to be %s, actual %s", tc.name, testMessage, actualMessage.LoggerName)
		}
		expectedZapFields := map[string]interface{}{}
		if !reflect.DeepEqual(actualMessage.ContextMap(), expectedZapFields) {
			t.Errorf("%s: Expected zap fields %v actual %v", tc.name, expectedZapFields, actualMessage.ContextMap())
		}
		if actualMessage.Level != tc.expectedLevel {
			t.Errorf("%s: Expected message level to be %d, actual %d", tc.name, tc.expectedLevel, actualMessage.Level)

		}

	}
}

func TestWithFields(t *testing.T) {
	observerLogger, logs := observer.New(zap.DebugLevel)
	logger := ZapLogger{zap.New(observerLogger)}
	logger.With(
		zap.String("TestOption", "Message"),
	)
	testMessage := "ABC"
	logger.Info(testMessage)
	actualMessage := logs.All()[0]
	if actualMessage.Message != testMessage {
		t.Errorf("Expected message to be %s, actual %s", testMessage, actualMessage.LoggerName)
	}
	expectedZapFields := map[string]interface{}{
		"TestOption": "Message",
	}
	if !reflect.DeepEqual(actualMessage.ContextMap(), expectedZapFields) {
		t.Errorf("Expected zap fields %v actual %v", expectedZapFields, actualMessage.ContextMap())
	}
}
