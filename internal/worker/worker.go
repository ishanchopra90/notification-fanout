package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"sync"
	"time"

	"github.com/notification-fanout/service/internal/store"
)

type DeliveryStore interface {
	ClaimDueDeliveries(ctx context.Context, limit int) ([]store.ClaimedDelivery, error)
	CreateDeliveryAttempt(ctx context.Context, deliveryID int64, attemptNo int, httpStatus *int, errorMessage, responseBody *string) (store.DeliveryAttempt, error)
	UpdateDeliveryStatus(ctx context.Context, id int64, status string, attempts int, nextAttemptAt time.Time, lastError *string, httpStatus *int) error
}

type Sender interface {
	Do(req *http.Request) (*http.Response, error)
}

type Config struct {
	WorkerCount    int
	ClaimBatchSize int
	MaxAttempts    int
	PollInterval   time.Duration
	WebhookTimeout time.Duration
	BaseBackoff    time.Duration
	MaxBackoff     time.Duration
}

const (
	StatusPending   = "pending"
	StatusRetrying  = "retrying"
	StatusDelivered = "delivered"
	StatusFailed    = "failed"
)

// Pool runs delivery workers that claim and POST pending deliveries.
type Pool struct {
	store  DeliveryStore
	sender Sender
	cfg    Config
}

// NewPool creates a worker pool with the given number of workers.
func NewPool(store DeliveryStore, sender Sender, cfg Config) *Pool {
	if cfg.WorkerCount <= 0 {
		cfg.WorkerCount = 1
	}
	if cfg.ClaimBatchSize <= 0 {
		cfg.ClaimBatchSize = 10
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	if cfg.WebhookTimeout <= 0 {
		cfg.WebhookTimeout = 5 * time.Second
	}
	if cfg.BaseBackoff <= 0 {
		cfg.BaseBackoff = 1 * time.Second
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	return &Pool{
		store:  store,
		sender: sender,
		cfg:    cfg,
	}
}

// Size returns the configured worker count.
func (p *Pool) Size() int {
	return p.cfg.WorkerCount
}

// Start runs the worker pool until the context is cancelled.
func (p *Pool) Start(ctx context.Context) {
	var wg sync.WaitGroup
	for i := 0; i < p.cfg.WorkerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(p.cfg.PollInterval)
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					_, _ = p.ProcessOnce(ctx)
				}
			}
		}()
	}
	wg.Wait()
}

// ProcessOnce claims due deliveries and processes a single batch.
func (p *Pool) ProcessOnce(ctx context.Context) (int, error) {
	deliveries, err := p.store.ClaimDueDeliveries(ctx, p.cfg.ClaimBatchSize)
	if err != nil {
		return 0, err
	}

	for _, d := range deliveries {
		if err := p.processDelivery(ctx, d); err != nil {
			// Continue processing remaining claimed rows for robustness.
			continue
		}
	}
	return len(deliveries), nil
}

func (p *Pool) processDelivery(ctx context.Context, d store.ClaimedDelivery) error {
	statusCode, responseBody, sendErr := p.sendWebhook(ctx, d.WebhookURL, d.Payload)
	attemptNo := d.Attempts + 1

	var httpStatusPtr *int
	if statusCode > 0 {
		httpStatusPtr = &statusCode
	}

	var errMessagePtr *string
	if sendErr != nil {
		msg := sendErr.Error()
		errMessagePtr = &msg
	}

	var responseBodyPtr *string
	if responseBody != "" {
		responseBodyPtr = &responseBody
	}

	if _, err := p.store.CreateDeliveryAttempt(ctx, d.DeliveryID, attemptNo, httpStatusPtr, errMessagePtr, responseBodyPtr); err != nil {
		return err
	}

	now := time.Now().UTC()
	if sendErr == nil && statusCode >= 200 && statusCode < 300 {
		return p.store.UpdateDeliveryStatus(ctx, d.DeliveryID, StatusDelivered, attemptNo, now, nil, httpStatusPtr)
	}

	if attemptNo >= p.cfg.MaxAttempts {
		return p.store.UpdateDeliveryStatus(ctx, d.DeliveryID, StatusFailed, attemptNo, now, errMessagePtr, httpStatusPtr)
	}

	next := now.Add(ComputeBackoff(attemptNo, p.cfg.BaseBackoff, p.cfg.MaxBackoff))
	return p.store.UpdateDeliveryStatus(ctx, d.DeliveryID, StatusRetrying, attemptNo, next, errMessagePtr, httpStatusPtr)
}

func (p *Pool) sendWebhook(ctx context.Context, webhookURL string, payload json.RawMessage) (int, string, error) {
	reqCtx, cancel := context.WithTimeout(ctx, p.cfg.WebhookTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, webhookURL, bytes.NewReader(payload))
	if err != nil {
		return 0, "", fmt.Errorf("build webhook request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.sender.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("send webhook request: %w", err)
	}
	defer resp.Body.Close()

	b, readErr := io.ReadAll(io.LimitReader(resp.Body, 64*1024))
	if readErr != nil {
		return resp.StatusCode, "", fmt.Errorf("read webhook response: %w", readErr)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return resp.StatusCode, string(b), fmt.Errorf("webhook responded with status %d", resp.StatusCode)
	}
	return resp.StatusCode, string(b), nil
}

// ComputeBackoff returns exponential backoff with max cap.
func ComputeBackoff(attemptNo int, base, max time.Duration) time.Duration {
	if attemptNo < 1 {
		attemptNo = 1
	}
	multiplier := math.Pow(2, float64(attemptNo-1))
	backoff := time.Duration(float64(base) * multiplier)
	if backoff > max {
		return max
	}
	return backoff
}
