package shared

import (
	"context"
	"log/slog"
	"os"
)

type ctxKey string

const traceIDKey ctxKey = "trace_surf_id"

func NewLogger(service string) *slog.Logger {
	h := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	})
	return slog.New(h).With("service", service)
}

func ContextWithTrace(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, traceIDKey, id)
}

func WithTrace(ctx context.Context, logger *slog.Logger) *slog.Logger {
	if id, ok := ctx.Value(traceIDKey).(string); ok && id != "" {
		return logger.With("trace_id", id)
	}
	return logger
}
