package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/notification-fanout/service/internal/store"

	"github.com/notification-fanout/service/internal/worker"
)

type fakeStore struct {
	claimed   []store.ClaimedDelivery
	attempts  []store.DeliveryAttempt
	updated   []updateCall
	claimErr  error
	createErr error
	updateErr error
}

type updateCall struct {
	id            int64
	status        string
	attempts      int
	nextAttemptAt time.Time
	lastError     *string
	httpStatus    *int
}

func (f *fakeStore) ClaimDueDeliveries(context.Context, int) ([]store.ClaimedDelivery, error) {
	if f.claimErr != nil {
		return nil, f.claimErr
	}
	return f.claimed, nil
}

func (f *fakeStore) CreateDeliveryAttempt(_ context.Context, deliveryID int64, attemptNo int, httpStatus *int, errorMessage, responseBody *string) (store.DeliveryAttempt, error) {
	if f.createErr != nil {
		return store.DeliveryAttempt{}, f.createErr
	}
	a := store.DeliveryAttempt{
		ID:           int64(len(f.attempts) + 1),
		DeliveryID:   deliveryID,
		AttemptNo:    attemptNo,
		HTTPStatus:   httpStatus,
		ErrorMessage: errorMessage,
		ResponseBody: responseBody,
	}
	f.attempts = append(f.attempts, a)
	return a, nil
}

func (f *fakeStore) UpdateDeliveryStatus(_ context.Context, id int64, status string, attempts int, nextAttemptAt time.Time, lastError *string, httpStatus *int) error {
	if f.updateErr != nil {
		return f.updateErr
	}
	f.updated = append(f.updated, updateCall{
		id:            id,
		status:        status,
		attempts:      attempts,
		nextAttemptAt: nextAttemptAt,
		lastError:     lastError,
		httpStatus:    httpStatus,
	})
	return nil
}

type fakeSender struct {
	resp *http.Response
	err  error
}

func (f fakeSender) Do(*http.Request) (*http.Response, error) {
	if f.err != nil {
		return nil, f.err
	}
	return f.resp, nil
}

func TestNewPool(t *testing.T) {
	p := worker.NewPool(&fakeStore{}, fakeSender{}, worker.Config{WorkerCount: 4})
	if p == nil {
		t.Fatal("NewPool returned nil")
	}
	if got := p.Size(); got != 4 {
		t.Fatalf("Size() = %d, want 4", got)
	}
}

func TestComputeBackoff(t *testing.T) {
	base := 1 * time.Second
	max := 10 * time.Second
	if got := worker.ComputeBackoff(1, base, max); got != 1*time.Second {
		t.Fatalf("attempt1 = %s, want 1s", got)
	}
	if got := worker.ComputeBackoff(3, base, max); got != 4*time.Second {
		t.Fatalf("attempt3 = %s, want 4s", got)
	}
	if got := worker.ComputeBackoff(10, base, max); got != 10*time.Second {
		t.Fatalf("attempt10 = %s, want capped 10s", got)
	}
}

func TestProcessOnceDelivered(t *testing.T) {
	fs := &fakeStore{
		claimed: []store.ClaimedDelivery{
			{
				DeliveryID: 1,
				Attempts:   0,
				WebhookURL: "https://example.com",
				Payload:    json.RawMessage(`{"ok":true}`),
			},
		},
	}
	sender := fakeSender{
		resp: &http.Response{
			StatusCode: 200,
			Body:       io.NopCloser(strings.NewReader("ok")),
		},
	}
	p := worker.NewPool(fs, sender, worker.Config{
		WorkerCount:    1,
		ClaimBatchSize: 1,
		MaxAttempts:    3,
		BaseBackoff:    time.Second,
		MaxBackoff:     10 * time.Second,
		WebhookTimeout: time.Second,
	})

	n, err := p.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}
	if n != 1 {
		t.Fatalf("processed = %d, want 1", n)
	}
	if len(fs.attempts) != 1 || fs.attempts[0].AttemptNo != 1 {
		t.Fatalf("attempts = %+v", fs.attempts)
	}
	if len(fs.updated) != 1 || fs.updated[0].status != worker.StatusDelivered {
		t.Fatalf("updated = %+v", fs.updated)
	}
}

func TestProcessOnceRetryAndFailed(t *testing.T) {
	fs := &fakeStore{
		claimed: []store.ClaimedDelivery{
			{DeliveryID: 1, Attempts: 0, WebhookURL: "https://example.com", Payload: json.RawMessage(`{"a":1}`)},
			{DeliveryID: 2, Attempts: 2, WebhookURL: "https://example.com", Payload: json.RawMessage(`{"a":2}`)},
		},
	}
	sender := fakeSender{
		resp: &http.Response{
			StatusCode: 500,
			Body:       io.NopCloser(strings.NewReader("nope")),
		},
	}
	p := worker.NewPool(fs, sender, worker.Config{
		WorkerCount:    1,
		ClaimBatchSize: 2,
		MaxAttempts:    3,
		BaseBackoff:    time.Second,
		MaxBackoff:     10 * time.Second,
		WebhookTimeout: time.Second,
	})

	_, err := p.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}
	if len(fs.updated) != 2 {
		t.Fatalf("updated = %+v", fs.updated)
	}
	if fs.updated[0].status != worker.StatusRetrying {
		t.Fatalf("first status = %s, want retrying", fs.updated[0].status)
	}
	if fs.updated[1].status != worker.StatusFailed {
		t.Fatalf("second status = %s, want failed", fs.updated[1].status)
	}
}

func TestProcessOnceClaimError(t *testing.T) {
	p := worker.NewPool(&fakeStore{claimErr: errors.New("db down")}, fakeSender{}, worker.Config{WorkerCount: 1})
	if _, err := p.ProcessOnce(context.Background()); err == nil {
		t.Fatal("ProcessOnce() expected error")
	}
}
