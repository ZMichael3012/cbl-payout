package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWithTraceparentValidation(t *testing.T) {
	t.Parallel()

	valid := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	ctx := WithTraceparent(context.Background(), valid)
	assert.Equal(t, valid, traceparentFromContext(ctx), "валидный traceparent сохраняется")

	for name, value := range map[string]string{
		"empty":     "",
		"garbage":   "not-a-traceparent",
		"uppercase": "00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01",
		"short":     "00-4bf92f3577b34da6a3ce929d0e0e4736-01",
		"injection": valid + "\nX-Injected: 1",
		"very_long": string(make([]byte, 4096)),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			invalid := WithTraceparent(context.Background(), value)
			assert.Empty(t, traceparentFromContext(invalid), "невалидный traceparent игнорируется")
		})
	}

	assert.Empty(t, traceparentFromContext(context.Background()), "контекст без traceparent пуст")
}
