//go:build localdb

package e2edb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/notification-fanout/service/internal/api"
	"github.com/notification-fanout/service/internal/matcher"
	"github.com/notification-fanout/service/internal/store"
	"github.com/notification-fanout/service/internal/worker"
)

func TestDBBackedE2EIngestDeliverAudit(t *testing.T) {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		t.Fatal("DATABASE_URL is required for localdb e2e tests")
	}
	t.Logf("step=setup database_url=%s", dbURL)

	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		t.Fatalf("pgxpool.New: %v", err)
	}
	defer pool.Close()

	if err := store.ApplyMigrations(context.Background(), dbURL); err != nil {
		t.Fatalf("ApplyMigrations: %v", err)
	}
	t.Logf("step=setup apply_migrations=ok")

	if err := resetDB(context.Background(), pool); err != nil {
		t.Fatalf("resetDB: %v", err)
	}
	t.Logf("step=setup reset_db=ok")

	st := store.New(pool)
	router := api.NewRouter(st, matcher.New())
	apiServer := httptest.NewServer(router)
	defer apiServer.Close()
	t.Logf("step=setup api_server=%s", apiServer.URL)

	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer webhook.Close()
	t.Logf("step=setup webhook_server=%s expected_status=200", webhook.URL)

	createSubscription(t, apiServer.URL, webhook.URL, `{"type":"order.created","source":"checkout-svc","payload":{"amount":{"gt":100}}}`)
	eventID := ingestEvent(t, apiServer.URL, "ik-db-success", `{"type":"order.created","source":"checkout-svc","payload":{"amount":120}}`)

	wp := worker.NewPool(st, &http.Client{}, worker.Config{
		WorkerCount:    1,
		ClaimBatchSize: 10,
		MaxAttempts:    3,
		PollInterval:   2 * time.Millisecond,
		WebhookTimeout: 200 * time.Millisecond,
		BaseBackoff:    2 * time.Millisecond,
		MaxBackoff:     10 * time.Millisecond,
	})
	t.Logf("step=worker action=process expected_status=delivered")
	for i := 0; i < 5; i++ {
		n, runErr := wp.ProcessOnce(context.Background())
		t.Logf("step=worker iteration=%d claimed=%d err=%v", i+1, n, runErr)
	}

	items := fetchAuditByEventID(t, apiServer.URL, eventID)
	t.Logf("step=audit observed_deliveries=%d expected_deliveries=1", len(items))
	if len(items) != 1 {
		t.Fatalf("deliveries len = %d, want 1", len(items))
	}
	t.Logf("step=audit observed_status=%s observed_attempts=%d expected_status=delivered expected_attempts=1", items[0].Delivery.Status, len(items[0].Attempts))
	if items[0].Delivery.Status != "delivered" {
		t.Fatalf("status = %s, want delivered", items[0].Delivery.Status)
	}
	if len(items[0].Attempts) != 1 {
		t.Fatalf("attempts len = %d, want 1", len(items[0].Attempts))
	}
}

func resetDB(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
TRUNCATE TABLE delivery_attempts, deliveries, subscriptions, events
RESTART IDENTITY CASCADE`)
	return err
}

func createSubscription(t *testing.T, apiURL, webhookURL, filter string) {
	t.Helper()
	body := fmt.Sprintf(`{"webhook_url":%q,"filter":%s}`, webhookURL, filter)
	t.Logf("action=http_request method=POST path=/subscriptions body=%s", body)
	resp, err := http.Post(apiURL+"/subscriptions", "application/json", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("create subscription request: %v", err)
	}
	defer resp.Body.Close()
	t.Logf("action=http_response method=POST path=/subscriptions status=%d expected=201", resp.StatusCode)
	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("create subscription status=%d body=%s", resp.StatusCode, string(b))
	}
}

func ingestEvent(t *testing.T, apiURL, idempotencyKey, body string) int64 {
	t.Helper()
	t.Logf("action=http_request method=POST path=/events idempotency_key=%s body=%s", idempotencyKey, body)
	req, err := http.NewRequest(http.MethodPost, apiURL+"/events", bytes.NewBufferString(body))
	if err != nil {
		t.Fatalf("new ingest request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", idempotencyKey)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("ingest request: %v", err)
	}
	defer resp.Body.Close()
	t.Logf("action=http_response method=POST path=/events status=%d expected=202", resp.StatusCode)
	if resp.StatusCode != http.StatusAccepted {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("ingest status=%d body=%s", resp.StatusCode, string(b))
	}
	var payload map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode ingest response: %v", err)
	}
	eventID, ok := payload["event_id"].(float64)
	if !ok {
		t.Fatalf("missing event_id in response: %#v", payload)
	}
	t.Logf("action=parse_response path=/events event_id=%d fanout_count=%v", int64(eventID), payload["fanout_count"])
	return int64(eventID)
}

type auditItem struct {
	Delivery store.Delivery          `json:"delivery"`
	Attempts []store.DeliveryAttempt `json:"attempts"`
}

func fetchAuditByEventID(t *testing.T, apiURL string, eventID int64) []auditItem {
	t.Helper()
	t.Logf("action=http_request method=GET path=/deliveries?event_id=%d", eventID)
	resp, err := http.Get(fmt.Sprintf("%s/deliveries?event_id=%d", apiURL, eventID))
	if err != nil {
		t.Fatalf("audit request: %v", err)
	}
	defer resp.Body.Close()
	t.Logf("action=http_response method=GET path=/deliveries status=%d expected=200", resp.StatusCode)
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("audit status=%d body=%s", resp.StatusCode, string(b))
	}
	var payload struct {
		Deliveries []auditItem `json:"deliveries"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		t.Fatalf("decode audit response: %v", err)
	}
	t.Logf("action=parse_response path=/deliveries deliveries=%d", len(payload.Deliveries))
	return payload.Deliveries
}
