package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func (s *Store) CreateEvent(ctx context.Context, idempotencyKey *string, eventType, source string, payload json.RawMessage) (Event, error) {
	const q = `
INSERT INTO events (idempotency_key, type, source, payload)
VALUES ($1, $2, $3, $4)
RETURNING id, idempotency_key, type, source, payload, created_at`

	var event Event
	var idempotencyKeyValue sql.NullString
	if err := s.db.QueryRow(ctx, q, idempotencyKey, eventType, source, payload).Scan(
		&event.ID,
		&idempotencyKeyValue,
		&event.Type,
		&event.Source,
		&event.Payload,
		&event.CreatedAt,
	); err != nil {
		return Event{}, fmt.Errorf("create event: %w", err)
	}
	event.IdempotencyKey = nullableStringPtr(idempotencyKeyValue)

	return event, nil
}

func (s *Store) CreateSubscription(ctx context.Context, webhookURL string, filter json.RawMessage, eventType, source *string) (Subscription, error) {
	const q = `
INSERT INTO subscriptions (webhook_url, filter, type, source, active)
VALUES ($1, $2, $3, $4, TRUE)
RETURNING id, webhook_url, filter, type, source, active, created_at, updated_at, deleted_at`

	var sub Subscription
	var typeValue sql.NullString
	var sourceValue sql.NullString
	var deletedAtValue sql.NullTime
	if err := s.db.QueryRow(ctx, q, webhookURL, filter, eventType, source).Scan(
		&sub.ID,
		&sub.WebhookURL,
		&sub.Filter,
		&typeValue,
		&sourceValue,
		&sub.Active,
		&sub.CreatedAt,
		&sub.UpdatedAt,
		&deletedAtValue,
	); err != nil {
		return Subscription{}, fmt.Errorf("create subscription: %w", err)
	}
	sub.Type = nullableStringPtr(typeValue)
	sub.Source = nullableStringPtr(sourceValue)
	sub.DeletedAt = nullableTimePtr(deletedAtValue)

	return sub, nil
}

func (s *Store) ListActiveSubscriptions(ctx context.Context) ([]Subscription, error) {
	const q = `
SELECT id, webhook_url, filter, type, source, active, created_at, updated_at, deleted_at
FROM subscriptions
WHERE active = TRUE
ORDER BY created_at ASC`

	rows, err := s.db.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("list subscriptions: %w", err)
	}
	defer rows.Close()

	var result []Subscription
	for rows.Next() {
		var sub Subscription
		var typeValue sql.NullString
		var sourceValue sql.NullString
		var deletedAtValue sql.NullTime
		if scanErr := rows.Scan(
			&sub.ID,
			&sub.WebhookURL,
			&sub.Filter,
			&typeValue,
			&sourceValue,
			&sub.Active,
			&sub.CreatedAt,
			&sub.UpdatedAt,
			&deletedAtValue,
		); scanErr != nil {
			return nil, fmt.Errorf("scan subscription: %w", scanErr)
		}
		sub.Type = nullableStringPtr(typeValue)
		sub.Source = nullableStringPtr(sourceValue)
		sub.DeletedAt = nullableTimePtr(deletedAtValue)
		result = append(result, sub)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate subscriptions: %w", err)
	}

	return result, nil
}

func (s *Store) SoftDeleteSubscription(ctx context.Context, id int64) error {
	const q = `
UPDATE subscriptions
SET active = FALSE, updated_at = NOW(), deleted_at = NOW()
WHERE id = $1`

	if _, err := s.db.Exec(ctx, q, id); err != nil {
		return fmt.Errorf("soft delete subscription %d: %w", id, err)
	}
	return nil
}

func (s *Store) CreateDelivery(ctx context.Context, eventID, subscriptionID int64, status string, nextAttemptAt time.Time) (Delivery, error) {
	const q = `
INSERT INTO deliveries (event_id, subscription_id, status, attempts, next_attempt_at)
VALUES ($1, $2, $3, 0, $4)
RETURNING id, event_id, subscription_id, status, attempts, next_attempt_at, last_error, http_status, created_at, updated_at`

	var d Delivery
	var lastErrorValue sql.NullString
	var httpStatusValue sql.NullInt32
	if err := s.db.QueryRow(ctx, q, eventID, subscriptionID, status, nextAttemptAt).Scan(
		&d.ID,
		&d.EventID,
		&d.SubscriptionID,
		&d.Status,
		&d.Attempts,
		&d.NextAttemptAt,
		&lastErrorValue,
		&httpStatusValue,
		&d.CreatedAt,
		&d.UpdatedAt,
	); err != nil {
		return Delivery{}, fmt.Errorf("create delivery: %w", err)
	}
	d.LastError = nullableStringPtr(lastErrorValue)
	d.HTTPStatus = nullableInt32Ptr(httpStatusValue)

	return d, nil
}

func (s *Store) ClaimDueDeliveries(ctx context.Context, limit int) ([]ClaimedDelivery, error) {
	const q = `
SELECT d.id, d.event_id, d.subscription_id, d.attempts, s.webhook_url, e.payload
FROM deliveries d
JOIN subscriptions s ON s.id = d.subscription_id
JOIN events e ON e.id = d.event_id
WHERE d.status IN ('pending', 'retrying')
  AND d.next_attempt_at <= NOW()
ORDER BY d.next_attempt_at ASC
FOR UPDATE SKIP LOCKED
LIMIT $1`

	rows, err := s.db.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("claim due deliveries: %w", err)
	}
	defer rows.Close()

	out := make([]ClaimedDelivery, 0, limit)
	for rows.Next() {
		var d ClaimedDelivery
		if scanErr := rows.Scan(
			&d.DeliveryID,
			&d.EventID,
			&d.SubscriptionID,
			&d.Attempts,
			&d.WebhookURL,
			&d.Payload,
		); scanErr != nil {
			return nil, fmt.Errorf("scan claimed delivery: %w", scanErr)
		}
		out = append(out, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claimed deliveries: %w", err)
	}

	return out, nil
}

func (s *Store) ListDeliveriesByEventID(ctx context.Context, eventID int64) ([]Delivery, error) {
	const q = `
SELECT id, event_id, subscription_id, status, attempts, next_attempt_at, last_error, http_status, created_at, updated_at
FROM deliveries
WHERE event_id = $1
ORDER BY created_at ASC`
	return s.listDeliveries(ctx, q, eventID)
}

func (s *Store) ListDeliveriesBySubscriptionID(ctx context.Context, subscriptionID int64) ([]Delivery, error) {
	const q = `
SELECT id, event_id, subscription_id, status, attempts, next_attempt_at, last_error, http_status, created_at, updated_at
FROM deliveries
WHERE subscription_id = $1
ORDER BY created_at ASC`
	return s.listDeliveries(ctx, q, subscriptionID)
}

func (s *Store) listDeliveries(ctx context.Context, q string, arg int64) ([]Delivery, error) {
	rows, err := s.db.Query(ctx, q, arg)
	if err != nil {
		return nil, fmt.Errorf("list deliveries: %w", err)
	}
	defer rows.Close()

	var result []Delivery
	for rows.Next() {
		var d Delivery
		var lastErrorValue sql.NullString
		var httpStatusValue sql.NullInt32
		if scanErr := rows.Scan(
			&d.ID,
			&d.EventID,
			&d.SubscriptionID,
			&d.Status,
			&d.Attempts,
			&d.NextAttemptAt,
			&lastErrorValue,
			&httpStatusValue,
			&d.CreatedAt,
			&d.UpdatedAt,
		); scanErr != nil {
			return nil, fmt.Errorf("scan delivery: %w", scanErr)
		}
		d.LastError = nullableStringPtr(lastErrorValue)
		d.HTTPStatus = nullableInt32Ptr(httpStatusValue)
		result = append(result, d)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate deliveries: %w", err)
	}

	return result, nil
}

func (s *Store) UpdateDeliveryStatus(ctx context.Context, id int64, status string, attempts int, nextAttemptAt time.Time, lastError *string, httpStatus *int) error {
	const q = `
UPDATE deliveries
SET status = $2,
    attempts = $3,
    next_attempt_at = $4,
    last_error = $5,
    http_status = $6,
    updated_at = NOW()
WHERE id = $1`

	if _, err := s.db.Exec(ctx, q, id, status, attempts, nextAttemptAt, lastError, httpStatus); err != nil {
		return fmt.Errorf("update delivery status %d: %w", id, err)
	}
	return nil
}

func (s *Store) CreateDeliveryAttempt(ctx context.Context, deliveryID int64, attemptNo int, httpStatus *int, errorMessage, responseBody *string) (DeliveryAttempt, error) {
	const q = `
INSERT INTO delivery_attempts (delivery_id, attempt_no, http_status, error_message, response_body)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, delivery_id, attempt_no, attempted_at, http_status, error_message, response_body`

	var attempt DeliveryAttempt
	var httpStatusValue sql.NullInt32
	var errorMessageValue sql.NullString
	var responseBodyValue sql.NullString
	if err := s.db.QueryRow(ctx, q, deliveryID, attemptNo, httpStatus, errorMessage, responseBody).Scan(
		&attempt.ID,
		&attempt.DeliveryID,
		&attempt.AttemptNo,
		&attempt.AttemptedAt,
		&httpStatusValue,
		&errorMessageValue,
		&responseBodyValue,
	); err != nil {
		return DeliveryAttempt{}, fmt.Errorf("create delivery attempt: %w", err)
	}
	attempt.HTTPStatus = nullableInt32Ptr(httpStatusValue)
	attempt.ErrorMessage = nullableStringPtr(errorMessageValue)
	attempt.ResponseBody = nullableStringPtr(responseBodyValue)

	return attempt, nil
}

func (s *Store) ListDeliveryAttempts(ctx context.Context, deliveryID int64) ([]DeliveryAttempt, error) {
	const q = `
SELECT id, delivery_id, attempt_no, attempted_at, http_status, error_message, response_body
FROM delivery_attempts
WHERE delivery_id = $1
ORDER BY attempt_no ASC`

	rows, err := s.db.Query(ctx, q, deliveryID)
	if err != nil {
		return nil, fmt.Errorf("list delivery attempts: %w", err)
	}
	defer rows.Close()

	var result []DeliveryAttempt
	for rows.Next() {
		var a DeliveryAttempt
		var httpStatusValue sql.NullInt32
		var errorMessageValue sql.NullString
		var responseBodyValue sql.NullString
		if scanErr := rows.Scan(
			&a.ID,
			&a.DeliveryID,
			&a.AttemptNo,
			&a.AttemptedAt,
			&httpStatusValue,
			&errorMessageValue,
			&responseBodyValue,
		); scanErr != nil {
			return nil, fmt.Errorf("scan delivery attempt: %w", scanErr)
		}
		a.HTTPStatus = nullableInt32Ptr(httpStatusValue)
		a.ErrorMessage = nullableStringPtr(errorMessageValue)
		a.ResponseBody = nullableStringPtr(responseBodyValue)
		result = append(result, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate delivery attempts: %w", err)
	}

	return result, nil
}

func nullableStringPtr(v sql.NullString) *string {
	if !v.Valid {
		return nil
	}
	s := v.String
	return &s
}

func nullableInt32Ptr(v sql.NullInt32) *int {
	if !v.Valid {
		return nil
	}
	i := int(v.Int32)
	return &i
}

func nullableTimePtr(v sql.NullTime) *time.Time {
	if !v.Valid {
		return nil
	}
	t := v.Time
	return &t
}
