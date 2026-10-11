package shared

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
)

func TestWithTrace(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, nil))

	ctx := ContextWithTrace(context.Background(), "abc-123")
	l := WithTrace(ctx, logger)
	l.Info("test")

	out := buf.String()
	if len(out) == 0 {
		t.Fatal("expected log output")
	}
	if !bytes.Contains(buf.Bytes(), []byte("abc-123")) {
		t.Errorf("trace_id missing from log: %s", out)
	}
}
