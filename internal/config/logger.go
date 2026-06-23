package config

import (
	"log/slog"
	"os"
)

// InitLogger configures the process-wide JSON slog logger writing to stdout.
func InitLogger(level slog.Level) *slog.Logger {
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: level,
	})
	logger := slog.New(handler).With("service", ServiceName)
	slog.SetDefault(logger)
	return logger
}
