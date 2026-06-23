package store_test

import (
	"context"
	"encoding/json"
	"regexp"
	"testing"
	"time"

	"github.com/notification-fanout/service/internal/store"
	"github.com/pashagolub/pgxmock/v4"
)

func TestNewRequiresPool(t *testing.T) {
	s := store.New(nil)
	if s == nil {
		t.Fatal("New returned nil")
	}
}

func TestReadySuccess(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool() error = %v", err)
	}
	defer mock.Close()

	mock.ExpectPing()
	mock.ExpectQuery(regexp.QuoteMeta("SELECT 1")).
		WillReturnRows(pgxmock.NewRows([]string{"?column?"}).AddRow(1))

	s := store.NewFromDB(mock)
	if err := s.Ready(context.Background()); err != nil {
		t.Fatalf("Ready() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestCreateEvent(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool() error = %v", err)
	}
	defer mock.Close()

	now := time.Now().UTC()
	key := "idem-123"
	payload := json.RawMessage(`{"amount":120}`)
	query := `
INSERT INTO events (idempotency_key, type, source, payload)
VALUES ($1, $2, $3, $4)
RETURNING id, idempotency_key, type, source, payload, created_at`

	mock.ExpectQuery(regexp.QuoteMeta(query)).
		WithArgs(&key, "order.created", "checkout-svc", payload).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "idempotency_key", "type", "source", "payload", "created_at"}).
				AddRow(int64(1), key, "order.created", "checkout-svc", []byte(payload), now),
		)

	s := store.NewFromDB(mock)
	event, err := s.CreateEvent(context.Background(), &key, "order.created", "checkout-svc", payload)
	if err != nil {
		t.Fatalf("CreateEvent() error = %v", err)
	}
	if event.ID != 1 || event.Type != "order.created" || event.Source != "checkout-svc" {
		t.Fatalf("unexpected event: %+v", event)
	}
	if event.IdempotencyKey == nil || *event.IdempotencyKey != key {
		t.Fatalf("unexpected idempotency key: %+v", event.IdempotencyKey)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSubscriptionCRUDHelpers(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool() error = %v", err)
	}
	defer mock.Close()

	now := time.Now().UTC()
	filter := json.RawMessage(`{"type":"order.created"}`)
	eventType := "order.created"
	source := "checkout-svc"

	createQ := `
INSERT INTO subscriptions (webhook_url, filter, type, source, active)
VALUES ($1, $2, $3, $4, TRUE)
RETURNING id, webhook_url, filter, type, source, active, created_at, updated_at, deleted_at`
	mock.ExpectQuery(regexp.QuoteMeta(createQ)).
		WithArgs("https://example.com/hook", filter, &eventType, &source).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "webhook_url", "filter", "type", "source", "active", "created_at", "updated_at", "deleted_at"}).
				AddRow(int64(7), "https://example.com/hook", []byte(filter), eventType, source, true, now, now, nil),
		)

	listQ := `
SELECT id, webhook_url, filter, type, source, active, created_at, updated_at, deleted_at
FROM subscriptions
WHERE active = TRUE
ORDER BY created_at ASC`
	mock.ExpectQuery(regexp.QuoteMeta(listQ)).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "webhook_url", "filter", "type", "source", "active", "created_at", "updated_at", "deleted_at"}).
				AddRow(int64(7), "https://example.com/hook", []byte(filter), eventType, source, true, now, now, nil),
		)

	deleteQ := `
UPDATE subscriptions
SET active = FALSE, updated_at = NOW(), deleted_at = NOW()
WHERE id = $1`
	mock.ExpectExec(regexp.QuoteMeta(deleteQ)).WithArgs(int64(7)).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	s := store.NewFromDB(mock)
	sub, err := s.CreateSubscription(context.Background(), "https://example.com/hook", filter, &eventType, &source)
	if err != nil {
		t.Fatalf("CreateSubscription() error = %v", err)
	}
	if !sub.Active || sub.ID != 7 {
		t.Fatalf("unexpected subscription: %+v", sub)
	}

	subs, err := s.ListActiveSubscriptions(context.Background())
	if err != nil {
		t.Fatalf("ListActiveSubscriptions() error = %v", err)
	}
	if len(subs) != 1 || subs[0].ID != 7 {
		t.Fatalf("unexpected subscriptions: %+v", subs)
	}

	if err := s.SoftDeleteSubscription(context.Background(), 7); err != nil {
		t.Fatalf("SoftDeleteSubscription() error = %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDeliveryAndAttemptHelpers(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool() error = %v", err)
	}
	defer mock.Close()

	now := time.Now().UTC()
	next := now.Add(30 * time.Second)
	status := "pending"
	createDeliveryQ := `
INSERT INTO deliveries (event_id, subscription_id, status, attempts, next_attempt_at)
VALUES ($1, $2, $3, 0, $4)
RETURNING id, event_id, subscription_id, status, attempts, next_attempt_at, last_error, http_status, created_at, updated_at`
	mock.ExpectQuery(regexp.QuoteMeta(createDeliveryQ)).
		WithArgs(int64(10), int64(20), status, next).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "event_id", "subscription_id", "status", "attempts", "next_attempt_at", "last_error", "http_status", "created_at", "updated_at"}).
				AddRow(int64(99), int64(10), int64(20), "pending", 0, next, nil, nil, now, now),
		)

	updateQ := `
UPDATE deliveries
SET status = $2,
    attempts = $3,
    next_attempt_at = $4,
    last_error = $5,
    http_status = $6,
    updated_at = NOW()
WHERE id = $1`
	errMsg := "timeout"
	httpStatus := 500
	mock.ExpectExec(regexp.QuoteMeta(updateQ)).
		WithArgs(int64(99), "retrying", 1, next, &errMsg, &httpStatus).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	createAttemptQ := `
INSERT INTO delivery_attempts (delivery_id, attempt_no, http_status, error_message, response_body)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, delivery_id, attempt_no, attempted_at, http_status, error_message, response_body`
	body := "oops"
	mock.ExpectQuery(regexp.QuoteMeta(createAttemptQ)).
		WithArgs(int64(99), 1, &httpStatus, &errMsg, &body).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "delivery_id", "attempt_no", "attempted_at", "http_status", "error_message", "response_body"}).
				AddRow(int64(300), int64(99), 1, now, 500, "timeout", "oops"),
		)

	listAttemptQ := `
SELECT id, delivery_id, attempt_no, attempted_at, http_status, error_message, response_body
FROM delivery_attempts
WHERE delivery_id = $1
ORDER BY attempt_no ASC`
	mock.ExpectQuery(regexp.QuoteMeta(listAttemptQ)).
		WithArgs(int64(99)).
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "delivery_id", "attempt_no", "attempted_at", "http_status", "error_message", "response_body"}).
				AddRow(int64(300), int64(99), 1, now, 500, "timeout", "oops"),
		)

	s := store.NewFromDB(mock)
	d, err := s.CreateDelivery(context.Background(), 10, 20, "pending", next)
	if err != nil {
		t.Fatalf("CreateDelivery() error = %v", err)
	}
	if d.ID != 99 {
		t.Fatalf("unexpected delivery: %+v", d)
	}

	if err := s.UpdateDeliveryStatus(context.Background(), 99, "retrying", 1, next, &errMsg, &httpStatus); err != nil {
		t.Fatalf("UpdateDeliveryStatus() error = %v", err)
	}

	attempt, err := s.CreateDeliveryAttempt(context.Background(), 99, 1, &httpStatus, &errMsg, &body)
	if err != nil {
		t.Fatalf("CreateDeliveryAttempt() error = %v", err)
	}
	if attempt.ID != 300 {
		t.Fatalf("unexpected delivery attempt: %+v", attempt)
	}

	attempts, err := s.ListDeliveryAttempts(context.Background(), 99)
	if err != nil {
		t.Fatalf("ListDeliveryAttempts() error = %v", err)
	}
	if len(attempts) != 1 || attempts[0].ID != 300 {
		t.Fatalf("unexpected attempts: %+v", attempts)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestIngestEventWithFanout(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool() error = %v", err)
	}
	defer mock.Close()

	idem := "ik-123"
	payload := json.RawMessage(`{"amount":120,"region":"us"}`)

	insertEventQ := `
INSERT INTO events (idempotency_key, type, source, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id`
	candidateSubsQ := `
SELECT id, filter
FROM subscriptions
WHERE active = TRUE
  AND (type IS NULL OR type = $1)
  AND (source IS NULL OR source = $2)`
	insertDeliveryQ := `
INSERT INTO deliveries (event_id, subscription_id, status, attempts, next_attempt_at)
VALUES ($1, $2, $3, 0, NOW())
ON CONFLICT (event_id, subscription_id) DO NOTHING`

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(insertEventQ)).
		WithArgs(&idem, "order.created", "checkout-svc", payload).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(500)))
	mock.ExpectQuery(regexp.QuoteMeta(candidateSubsQ)).
		WithArgs("order.created", "checkout-svc").
		WillReturnRows(
			pgxmock.NewRows([]string{"id", "filter"}).
				AddRow(int64(11), []byte(`{"type":"order.created","source":"checkout-svc","payload":{"amount":{"gt":100}}}`)).
				AddRow(int64(12), []byte(`{"type":"order.created","source":"checkout-svc","payload":{"amount":{"lt":100}}}`)),
		)
	mock.ExpectExec(regexp.QuoteMeta(insertDeliveryQ)).
		WithArgs(int64(500), int64(11), "pending").
		WillReturnResult(pgxmock.NewResult("INSERT", 1))
	mock.ExpectCommit()

	s := store.NewFromDB(mock)
	result, err := s.IngestEventWithFanout(context.Background(), &idem, "order.created", "checkout-svc", payload)
	if err != nil {
		t.Fatalf("IngestEventWithFanout() error = %v", err)
	}
	if result.EventID != 500 {
		t.Fatalf("EventID = %d, want 500", result.EventID)
	}
	if result.FanoutCount != 1 {
		t.Fatalf("FanoutCount = %d, want 1", result.FanoutCount)
	}
	if result.IdempotentReplay {
		t.Fatal("IdempotentReplay = true, want false")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestIngestEventWithFanoutIdempotentReplay(t *testing.T) {
	mock, err := pgxmock.NewPool()
	if err != nil {
		t.Fatalf("pgxmock.NewPool() error = %v", err)
	}
	defer mock.Close()

	idem := "ik-replay"
	payload := json.RawMessage(`{"amount":120}`)

	insertEventQ := `
INSERT INTO events (idempotency_key, type, source, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id`
	existingEventQ := `SELECT id FROM events WHERE idempotency_key = $1`

	mock.ExpectBegin()
	mock.ExpectQuery(regexp.QuoteMeta(insertEventQ)).
		WithArgs(&idem, "order.created", "checkout-svc", payload).
		WillReturnRows(pgxmock.NewRows([]string{"id"}))
	mock.ExpectQuery(regexp.QuoteMeta(existingEventQ)).
		WithArgs(idem).
		WillReturnRows(pgxmock.NewRows([]string{"id"}).AddRow(int64(777)))
	mock.ExpectCommit()

	s := store.NewFromDB(mock)
	result, err := s.IngestEventWithFanout(context.Background(), &idem, "order.created", "checkout-svc", payload)
	if err != nil {
		t.Fatalf("IngestEventWithFanout() error = %v", err)
	}
	if result.EventID != 777 {
		t.Fatalf("EventID = %d, want 777", result.EventID)
	}
	if result.FanoutCount != 0 {
		t.Fatalf("FanoutCount = %d, want 0", result.FanoutCount)
	}
	if !result.IdempotentReplay {
		t.Fatal("IdempotentReplay = false, want true")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}
