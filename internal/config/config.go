package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const ServiceName = "notification-fanout"

const (
	defaultDatabaseURL   = "postgres://postgres:postgres@localhost:5432/notification_fanout?sslmode=disable"
	defaultPort          = 8080
	defaultWorkerCount   = 4
	defaultHTTPTimeout   = 10 * time.Second
	defaultRequestTimeout = 30 * time.Second
	defaultShutdownTimeout = 30 * time.Second
	defaultLogLevel      = "info"
)

// Config holds runtime settings loaded from the environment.
type Config struct {
	DatabaseURL     string
	Port            int
	WorkerCount     int
	HTTPTimeout     time.Duration
	RequestTimeout  time.Duration
	ShutdownTimeout time.Duration
	LogLevel        string
}

// Load reads configuration from environment variables, applying defaults where unset.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:     envOrDefault("DATABASE_URL", defaultDatabaseURL),
		Port:            defaultPort,
		WorkerCount:     defaultWorkerCount,
		HTTPTimeout:     defaultHTTPTimeout,
		RequestTimeout:  defaultRequestTimeout,
		ShutdownTimeout: defaultShutdownTimeout,
		LogLevel:        envOrDefault("LOG_LEVEL", defaultLogLevel),
	}

	if v := os.Getenv("PORT"); v != "" {
		port, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("PORT: invalid integer %q: %w", v, err)
		}
		cfg.Port = port
	}

	if v := os.Getenv("WORKER_COUNT"); v != "" {
		count, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("WORKER_COUNT: invalid integer %q: %w", v, err)
		}
		cfg.WorkerCount = count
	}

	for _, spec := range []struct {
		name   string
		value  *time.Duration
		def    time.Duration
	}{
		{"HTTP_TIMEOUT", &cfg.HTTPTimeout, defaultHTTPTimeout},
		{"REQUEST_TIMEOUT", &cfg.RequestTimeout, defaultRequestTimeout},
		{"SHUTDOWN_TIMEOUT", &cfg.ShutdownTimeout, defaultShutdownTimeout},
	} {
		if v := os.Getenv(spec.name); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return Config{}, fmt.Errorf("%s: invalid duration %q: %w", spec.name, v, err)
			}
			*spec.value = d
		}
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}

	return cfg, nil
}

func (c Config) validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("DATABASE_URL must not be empty")
	}
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("PORT must be between 1 and 65535, got %d", c.Port)
	}
	if c.WorkerCount < 1 {
		return fmt.Errorf("WORKER_COUNT must be at least 1, got %d", c.WorkerCount)
	}
	for _, spec := range []struct {
		name string
		d    time.Duration
	}{
		{"HTTP_TIMEOUT", c.HTTPTimeout},
		{"REQUEST_TIMEOUT", c.RequestTimeout},
		{"SHUTDOWN_TIMEOUT", c.ShutdownTimeout},
	} {
		if spec.d <= 0 {
			return fmt.Errorf("%s must be positive, got %s", spec.name, spec.d)
		}
	}
	if _, err := ParseLogLevel(c.LogLevel); err != nil {
		return err
	}
	return nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
