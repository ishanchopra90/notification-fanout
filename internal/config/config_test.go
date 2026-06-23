package config_test

import (
	"testing"
	"time"

	"github.com/notification-fanout/service/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	t.Setenv("PORT", "")
	t.Setenv("WORKER_COUNT", "")
	t.Setenv("HTTP_TIMEOUT", "")
	t.Setenv("REQUEST_TIMEOUT", "")
	t.Setenv("SHUTDOWN_TIMEOUT", "")
	t.Setenv("LOG_LEVEL", "")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.DatabaseURL == "" {
		t.Fatal("DatabaseURL must have a default")
	}
	if cfg.Port != 8080 {
		t.Fatalf("Port = %d, want 8080", cfg.Port)
	}
	if cfg.WorkerCount != 4 {
		t.Fatalf("WorkerCount = %d, want 4", cfg.WorkerCount)
	}
	if cfg.HTTPTimeout != 10*time.Second {
		t.Fatalf("HTTPTimeout = %s, want 10s", cfg.HTTPTimeout)
	}
	if cfg.RequestTimeout != 30*time.Second {
		t.Fatalf("RequestTimeout = %s, want 30s", cfg.RequestTimeout)
	}
	if cfg.ShutdownTimeout != 30*time.Second {
		t.Fatalf("ShutdownTimeout = %s, want 30s", cfg.ShutdownTimeout)
	}
	if cfg.LogLevel != "info" {
		t.Fatalf("LogLevel = %q, want info", cfg.LogLevel)
	}
}

func TestLoadFromEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://example/db")
	t.Setenv("PORT", "9090")
	t.Setenv("WORKER_COUNT", "8")
	t.Setenv("HTTP_TIMEOUT", "5s")
	t.Setenv("REQUEST_TIMEOUT", "45s")
	t.Setenv("SHUTDOWN_TIMEOUT", "15s")
	t.Setenv("LOG_LEVEL", "debug")

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	if cfg.DatabaseURL != "postgres://example/db" {
		t.Fatalf("DatabaseURL = %q", cfg.DatabaseURL)
	}
	if cfg.Port != 9090 {
		t.Fatalf("Port = %d, want 9090", cfg.Port)
	}
	if cfg.WorkerCount != 8 {
		t.Fatalf("WorkerCount = %d, want 8", cfg.WorkerCount)
	}
	if cfg.HTTPTimeout != 5*time.Second {
		t.Fatalf("HTTPTimeout = %s, want 5s", cfg.HTTPTimeout)
	}
	if cfg.RequestTimeout != 45*time.Second {
		t.Fatalf("RequestTimeout = %s, want 45s", cfg.RequestTimeout)
	}
	if cfg.ShutdownTimeout != 15*time.Second {
		t.Fatalf("ShutdownTimeout = %s, want 15s", cfg.ShutdownTimeout)
	}
	if cfg.LogLevel != "debug" {
		t.Fatalf("LogLevel = %q, want debug", cfg.LogLevel)
	}
}

func TestLoadInvalidPort(t *testing.T) {
	t.Setenv("PORT", "not-a-number")
	if _, err := config.Load(); err == nil {
		t.Fatal("Load() expected error for invalid PORT")
	}
}

func TestLoadInvalidWorkerCount(t *testing.T) {
	t.Setenv("WORKER_COUNT", "0")
	if _, err := config.Load(); err == nil {
		t.Fatal("Load() expected error for WORKER_COUNT=0")
	}
}

func TestLoadInvalidDuration(t *testing.T) {
	t.Setenv("HTTP_TIMEOUT", "fast")
	if _, err := config.Load(); err == nil {
		t.Fatal("Load() expected error for invalid HTTP_TIMEOUT")
	}
}

func TestLoadInvalidLogLevel(t *testing.T) {
	t.Setenv("LOG_LEVEL", "verbose")
	if _, err := config.Load(); err == nil {
		t.Fatal("Load() expected error for invalid LOG_LEVEL")
	}
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"debug", "DEBUG"},
		{"INFO", "INFO"},
		{"warn", "WARN"},
		{"warning", "WARN"},
		{"error", "ERROR"},
	}

	for _, tt := range tests {
		level, err := config.ParseLogLevel(tt.in)
		if err != nil {
			t.Fatalf("ParseLogLevel(%q) error = %v", tt.in, err)
		}
		if level.String() != tt.want {
			t.Fatalf("ParseLogLevel(%q) = %s, want %s", tt.in, level, tt.want)
		}
	}
}
