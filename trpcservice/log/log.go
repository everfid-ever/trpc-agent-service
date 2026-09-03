// Package log provides the service's structured, redacting log boundary.
package log

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"regexp"
	"strings"

	"go.opentelemetry.io/otel/trace"
)

type Level string

const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

type MaskingLevel string

const (
	MaskNone   MaskingLevel = "none"
	MaskBasic  MaskingLevel = "basic"
	MaskStrict MaskingLevel = "strict"
)

// Attribute is a structured field. Values are normalized before reaching the
// output handler, so a custom Stringer or error cannot bypass redaction.
type Attribute struct {
	Key   string
	Value any
}

func String(key, value string) Attribute    { return Attribute{Key: key, Value: value} }
func Int(key string, value int) Attribute   { return Attribute{Key: key, Value: value} }
func Bool(key string, value bool) Attribute { return Attribute{Key: key, Value: value} }
func Error(err error) Attribute             { return Attribute{Key: "error", Value: err} }

type Config struct {
	Writer       io.Writer
	Level        Level
	MaskingLevel MaskingLevel
	Role         string
}

// Logger retains Print and Printf for the existing role call sites while
// exposing context-aware structured methods for new code.
type Logger struct {
	logger  *slog.Logger
	masking MaskingLevel
}

func New(config Config) (*Logger, error) {
	if config.Writer == nil || !validRole(config.Role) {
		return nil, errors.New("invalid log configuration")
	}
	level, ok := parseLevel(config.Level)
	if !ok {
		return nil, errors.New("invalid log level")
	}
	if config.MaskingLevel == "" {
		config.MaskingLevel = MaskBasic
	}
	if !validMaskingLevel(config.MaskingLevel) {
		return nil, errors.New("invalid log masking level")
	}
	handler := slog.NewJSONHandler(config.Writer, &slog.HandlerOptions{Level: level})
	return &Logger{logger: slog.New(handler).With(slog.String("role", config.Role)), masking: config.MaskingLevel}, nil
}

func NewFromEnv(getenv func(string) string, writer io.Writer, role string) (*Logger, error) {
	if getenv == nil {
		return nil, errors.New("invalid log environment")
	}
	level := Level(strings.ToLower(strings.TrimSpace(valueOr(getenv("TRPC_LOG_LEVEL"), string(LevelInfo)))))
	masking := MaskingLevel(strings.ToLower(strings.TrimSpace(valueOr(getenv("TRPC_LOG_MASKING_LEVEL"), string(MaskBasic)))))
	return New(Config{Writer: writer, Level: level, MaskingLevel: masking, Role: role})
}

func (l *Logger) Debug(ctx context.Context, message string, attributes ...Attribute) {
	l.log(ctx, slog.LevelDebug, message, attributes)
}

func (l *Logger) Info(ctx context.Context, message string, attributes ...Attribute) {
	l.log(ctx, slog.LevelInfo, message, attributes)
}

func (l *Logger) Warn(ctx context.Context, message string, attributes ...Attribute) {
	l.log(ctx, slog.LevelWarn, message, attributes)
}

func (l *Logger) Error(ctx context.Context, message string, attributes ...Attribute) {
	l.log(ctx, slog.LevelError, message, attributes)
}

func (l *Logger) Print(values ...any) {
	if l != nil {
		l.Info(context.Background(), fmt.Sprint(values...))
	}
}

func (l *Logger) Printf(format string, values ...any) {
	if l != nil {
		l.Info(context.Background(), fmt.Sprintf(format, values...))
	}
}

func (l *Logger) log(ctx context.Context, level slog.Level, message string, attributes []Attribute) {
	if l == nil || l.logger == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fields := make([]slog.Attr, 0, len(attributes)+2)
	if span := trace.SpanContextFromContext(ctx); span.IsValid() {
		fields = append(fields, slog.String("trace_id", span.TraceID().String()), slog.String("span_id", span.SpanID().String()))
	}
	for _, attribute := range attributes {
		if key := strings.TrimSpace(attribute.Key); validKey(key) {
			fields = append(fields, slog.Any(key, normalizeValue(key, attribute.Value, l.masking)))
		}
	}
	l.logger.LogAttrs(ctx, level, redactText(message), fields...)
}

func parseLevel(value Level) (slog.Level, bool) {
	switch value {
	case LevelDebug:
		return slog.LevelDebug, true
	case LevelInfo:
		return slog.LevelInfo, true
	case LevelWarn:
		return slog.LevelWarn, true
	case LevelError:
		return slog.LevelError, true
	default:
		return 0, false
	}
}

func validMaskingLevel(value MaskingLevel) bool {
	return value == MaskNone || value == MaskBasic || value == MaskStrict
}

func validRole(value string) bool {
	value = strings.TrimSpace(value)
	return value != "" && len(value) <= 64 && !strings.ContainsAny(value, "\x00\r\n")
}

func validKey(value string) bool {
	if value == "" || len(value) > 96 {
		return false
	}
	for _, runeValue := range value {
		if !(runeValue >= 'a' && runeValue <= 'z' || runeValue >= 'A' && runeValue <= 'Z' || runeValue >= '0' && runeValue <= '9' || runeValue == '_' || runeValue == '.' || runeValue == '-') {
			return false
		}
	}
	return true
}

func normalizeValue(key string, value any, masking MaskingLevel) any {
	if sensitiveKey(key) || (masking == MaskStrict && piiKey(key)) {
		return "[REDACTED]"
	}
	switch typed := value.(type) {
	case nil:
		return nil
	case string:
		return redactText(typed)
	case error:
		return redactText(typed.Error())
	case bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return typed
	default:
		return redactText(fmt.Sprint(typed))
	}
}

func sensitiveKey(key string) bool {
	key = strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", "_"), ".", "_"))
	for _, part := range []string{"secret", "token", "password", "authorization", "credential", "api_key", "apikey", "dsn", "cookie", "payload", "prompt", "content", "body", "private_key"} {
		if strings.Contains(key, part) {
			return true
		}
	}
	return false
}

func piiKey(key string) bool {
	key = strings.ToLower(key)
	for _, part := range []string{"user", "email", "phone", "message", "external", "session"} {
		if strings.Contains(key, part) {
			return true
		}
	}
	return false
}

var (
	bearerPattern        = regexp.MustCompile(`(?i)\bbearer\s+[a-z0-9._~+/-]+`)
	assignmentPattern    = regexp.MustCompile(`(?i)\b(secret|token|password|authorization|api[_-]?key|credential|dsn)\s*[:=]\s*[^\s,;]+`)
	urlCredentialPattern = regexp.MustCompile(`(?i)([a-z][a-z0-9+.-]*://[^\s:/@]+:)[^\s@/]+@`)
)

func redactText(value string) string {
	value = urlCredentialPattern.ReplaceAllString(value, "${1}[REDACTED]@")
	value = bearerPattern.ReplaceAllString(value, "Bearer [REDACTED]")
	return assignmentPattern.ReplaceAllStringFunc(value, func(match string) string {
		separator := strings.IndexAny(match, ":=")
		if separator < 0 {
			return "[REDACTED]"
		}
		return match[:separator+1] + "[REDACTED]"
	})
}

func valueOr(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}
