package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/notification-fanout/service/internal/api"
	"github.com/notification-fanout/service/internal/config"
	"github.com/notification-fanout/service/internal/matcher"
	"github.com/notification-fanout/service/internal/store"
	"github.com/notification-fanout/service/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	level, err := config.ParseLogLevel(cfg.LogLevel)
	if err != nil {
		slog.Error("failed to parse log level", "error", err)
		os.Exit(1)
	}

	logger := config.InitLogger(level)
	migrateCtx, cancel := context.WithTimeout(context.Background(), cfg.RequestTimeout)
	defer cancel()

	if err := store.ApplyMigrations(migrateCtx, cfg.DatabaseURL); err != nil {
		logger.Error("failed to apply migrations", "error", err)
		os.Exit(1)
	}

	dbPool, err := store.NewPool(context.Background(), cfg.DatabaseURL)
	if err != nil {
		logger.Error("failed to create db pool", "error", err)
		os.Exit(1)
	}
	defer dbPool.Close()

	st := store.New(dbPool)
	if err := st.Ready(context.Background()); err != nil {
		logger.Error("service is not ready", "error", err)
		os.Exit(1)
	}

	router := api.NewRouter(st, matcher.New())
	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      router,
		ReadTimeout:  cfg.RequestTimeout,
		WriteTimeout: cfg.RequestTimeout,
		IdleTimeout:  60 * time.Second,
	}

	wp := worker.NewPool(st, &http.Client{}, worker.Config{
		WorkerCount:    cfg.WorkerCount,
		ClaimBatchSize: cfg.WorkerCount,
		MaxAttempts:    5,
		PollInterval:   250 * time.Millisecond,
		WebhookTimeout: cfg.HTTPTimeout,
		BaseBackoff:    1 * time.Second,
		MaxBackoff:     30 * time.Second,
	})
	workerCtx, workerCancel := context.WithCancel(context.Background())
	var workerWG sync.WaitGroup
	workerWG.Add(1)
	go func() {
		defer workerWG.Done()
		wp.Start(workerCtx)
	}()

	logger.Info("starting",
		"port", cfg.Port,
		"worker_count", cfg.WorkerCount,
		"http_timeout", cfg.HTTPTimeout,
		"request_timeout", cfg.RequestTimeout,
		"shutdown_timeout", cfg.ShutdownTimeout,
	)

	serverErrCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErrCh <- err
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)

	select {
	case sig := <-sigCh:
		logger.Info("received shutdown signal", "signal", sig.String())
	case err := <-serverErrCh:
		logger.Error("http server failed", "error", err)
		workerCancel()
		workerWG.Wait()
		os.Exit(1)
	}

	workerCancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}

	workerWG.Wait()
	logger.Info("shutdown complete")
}
