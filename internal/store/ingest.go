package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/notification-fanout/service/internal/matcher"
)

// IngestEventWithFanout inserts the event and all matching pending deliveries in one transaction.
func (s *Store) IngestEventWithFanout(ctx context.Context, idempotencyKey *string, eventType, source string, payload json.RawMessage) (IngestResult, error) {
	var payloadMap map[string]any
	if err := json.Unmarshal(payload, &payloadMap); err != nil {
		return IngestResult{}, fmt.Errorf("invalid payload JSON: %w", err)
	}

	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return IngestResult{}, fmt.Errorf("begin ingest tx: %w", err)
	}
	defer tx.Rollback(ctx)

	result, err := ingestInTx(ctx, tx, idempotencyKey, eventType, source, payload, payloadMap)
	if err != nil {
		return IngestResult{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return IngestResult{}, fmt.Errorf("commit ingest tx: %w", err)
	}
	return result, nil
}

func ingestInTx(ctx context.Context, tx pgx.Tx, idempotencyKey *string, eventType, source string, payload json.RawMessage, payloadMap map[string]any) (IngestResult, error) {
	const insertEventQ = `
INSERT INTO events (idempotency_key, type, source, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (idempotency_key) DO NOTHING
RETURNING id`

	var eventID int64
	insertErr := tx.QueryRow(ctx, insertEventQ, idempotencyKey, eventType, source, payload).Scan(&eventID)
	replay := false

	if insertErr == pgx.ErrNoRows {
		replay = true
		if idempotencyKey == nil {
			return IngestResult{}, fmt.Errorf("idempotency replay without idempotency key")
		}
		const existingEventQ = `SELECT id FROM events WHERE idempotency_key = $1`
		if err := tx.QueryRow(ctx, existingEventQ, *idempotencyKey).Scan(&eventID); err != nil {
			return IngestResult{}, fmt.Errorf("lookup existing event by idempotency key: %w", err)
		}
	} else if insertErr != nil {
		return IngestResult{}, fmt.Errorf("insert event: %w", insertErr)
	}

	if replay {
		return IngestResult{
			EventID:          eventID,
			FanoutCount:      0,
			IdempotentReplay: true,
		}, nil
	}

	const candidateSubsQ = `
SELECT id, filter
FROM subscriptions
WHERE active = TRUE
  AND (type IS NULL OR type = $1)
  AND (source IS NULL OR source = $2)`

	rows, err := tx.Query(ctx, candidateSubsQ, eventType, source)
	if err != nil {
		return IngestResult{}, fmt.Errorf("query candidate subscriptions: %w", err)
	}
	defer rows.Close()

	m := matcher.New()
	event := matcher.Event{
		Type:    eventType,
		Source:  source,
		Payload: payloadMap,
	}

	fanoutCount := 0
	const insertDeliveryQ = `
INSERT INTO deliveries (event_id, subscription_id, status, attempts, next_attempt_at)
VALUES ($1, $2, $3, 0, NOW())
ON CONFLICT (event_id, subscription_id) DO NOTHING`

	for rows.Next() {
		var subscriptionID int64
		var filterJSON json.RawMessage
		if err := rows.Scan(&subscriptionID, &filterJSON); err != nil {
			return IngestResult{}, fmt.Errorf("scan candidate subscription: %w", err)
		}

		filter, err := m.ParseFilter(filterJSON)
		if err != nil {
			return IngestResult{}, fmt.Errorf("parse candidate filter for subscription %d: %w", subscriptionID, err)
		}
		if !m.Match(filter, event) {
			continue
		}

		tag, err := tx.Exec(ctx, insertDeliveryQ, eventID, subscriptionID, "pending")
		if err != nil {
			return IngestResult{}, fmt.Errorf("insert delivery for subscription %d: %w", subscriptionID, err)
		}
		fanoutCount += int(tag.RowsAffected())
	}
	if err := rows.Err(); err != nil {
		return IngestResult{}, fmt.Errorf("iterate candidate subscriptions: %w", err)
	}

	return IngestResult{
		EventID:          eventID,
		FanoutCount:      fanoutCount,
		IdempotentReplay: false,
	}, nil
}
