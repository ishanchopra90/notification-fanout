package config_test

import (
	"log/slog"
	"testing"

	"github.com/notification-fanout/service/internal/config"
)

func TestInitLoggerSetsDefault(t *testing.T) {
	logger := config.InitLogger(slog.LevelInfo)
	if logger == nil {
		t.Fatal("InitLogger returned nil")
	}
	if slog.Default() != logger {
		t.Fatal("InitLogger did not set slog default")
	}
}
