package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/notification-fanout/service/internal/api"
	"github.com/notification-fanout/service/internal/matcher"
	"github.com/notification-fanout/service/internal/store"
	"github.com/notification-fanout/service/internal/worker"
)

type memoryStore struct {
	mu sync.Mutex

	nextEventID        int64
	nextSubscriptionID int64
	nextDeliveryID     int64
	nextAttemptID      int64

	eventsByID           map[int64]store.Event
	eventIDByKey         map[string]int64
	subscriptionsByID    map[int64]store.Subscription
	deliveriesByID       map[int64]store.Delivery
	attemptsByDeliveryID map[int64][]store.DeliveryAttempt
}

func newMemoryStore() *memoryStore {
	return &memoryStore{
		nextEventID:          1,
		nextSubscriptionID:   1,
		nextDeliveryID:       1,
		nextAttemptID:        1,
		eventsByID:           map[int64]store.Event{},
		eventIDByKey:         map[string]int64{},
		subscriptionsByID:    map[int64]store.Subscription{},
		deliveriesByID:       map[int64]store.Delivery{},
		attemptsByDeliveryID: map[int64][]store.DeliveryAttempt{},
	}
}

func (m *memoryStore) Ready(context.Context) error { return nil }

func (m *memoryStore) CreateSubscription(_ context.Context, webhookURL string, filter json.RawMessage, eventType, source *string) (store.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now().UTC()
	sub := store.Subscription{
		ID:         m.nextSubscriptionID,
		WebhookURL: webhookURL,
		Filter:     filter,
		Type:       eventType,
		Source:     source,
		Active:     true,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	m.nextSubscriptionID++
	m.subscriptionsByID[sub.ID] = sub
	return sub, nil
}

func (m *memoryStore) ListActiveSubscriptions(context.Context) ([]store.Subscription, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := make([]store.Subscription, 0, len(m.subscriptionsByID))
	for _, s := range m.subscriptionsByID {
		if s.Active {
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *memoryStore) SoftDeleteSubscription(_ context.Context, id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.subscriptionsByID[id]
	if !ok {
		return nil
	}
	now := time.Now().UTC()
	s.Active = false
	s.DeletedAt = &now
	s.UpdatedAt = now
	m.subscriptionsByID[id] = s
	return nil
}

func (m *memoryStore) IngestEventWithFanout(_ context.Context, idempotencyKey *string, eventType, source string, payload json.RawMessage) (store.IngestResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if idempotencyKey != nil {
		if existingID, ok := m.eventIDByKey[*idempotencyKey]; ok {
			return store.IngestResult{
				EventID:          existingID,
				FanoutCount:      0,
				IdempotentReplay: true,
			}, nil
		}
	}

	var payloadMap map[string]any
	if err := json.Unmarshal(payload, &payloadMap); err != nil {
		return store.IngestResult{}, err
	}

	eventID := m.nextEventID
	m.nextEventID++
	ev := store.Event{
		ID:             eventID,
		IdempotencyKey: idempotencyKey,
		Type:           eventType,
		Source:         source,
		Payload:        payload,
		CreatedAt:      time.Now().UTC(),
	}
	m.eventsByID[eventID] = ev
	if idempotencyKey != nil {
		m.eventIDByKey[*idempotencyKey] = eventID
	}

	matcherEngine := matcher.New()
	fanoutCount := 0
	for _, sub := range m.subscriptionsByID {
		if !sub.Active {
			continue
		}
		filter, err := matcherEngine.ParseFilter(sub.Filter)
		if err != nil {
			continue
		}
		if !matcherEngine.Match(filter, matcher.Event{
			Type:    eventType,
			Source:  source,
			Payload: payloadMap,
		}) {
			continue
		}

		deliveryID := m.nextDeliveryID
		m.nextDeliveryID++
		m.deliveriesByID[deliveryID] = store.Delivery{
			ID:             deliveryID,
			EventID:        eventID,
			SubscriptionID: sub.ID,
			Status:         "pending",
			Attempts:       0,
			NextAttemptAt:  time.Now().UTC(),
			CreatedAt:      time.Now().UTC(),
			UpdatedAt:      time.Now().UTC(),
		}
		fanoutCount++
	}

	return store.IngestResult{
		EventID:          eventID,
		FanoutCount:      fanoutCount,
		IdempotentReplay: false,
	}, nil
}

func (m *memoryStore) ListDeliveriesByEventID(_ context.Context, eventID int64) ([]store.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.Delivery
	for _, d := range m.deliveriesByID {
		if d.EventID == eventID {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *memoryStore) ListDeliveriesBySubscriptionID(_ context.Context, subscriptionID int64) ([]store.Delivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.Delivery
	for _, d := range m.deliveriesByID {
		if d.SubscriptionID == subscriptionID {
			out = append(out, d)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (m *memoryStore) ListDeliveryAttempts(_ context.Context, deliveryID int64) ([]store.DeliveryAttempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := append([]store.DeliveryAttempt(nil), m.attemptsByDeliveryID[deliveryID]...)
	sort.Slice(out, func(i, j int) bool { return out[i].AttemptNo < out[j].AttemptNo })
	return out, nil
}

func (m *memoryStore) ClaimDueDeliveries(_ context.Context, limit int) ([]store.ClaimedDelivery, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().UTC()
	out := make([]store.ClaimedDelivery, 0, limit)
	for _, d := range m.deliveriesByID {
		if len(out) >= limit {
			break
		}
		if d.Status != "pending" && d.Status != "retrying" {
			continue
		}
		if d.NextAttemptAt.After(now) {
			continue
		}
		sub := m.subscriptionsByID[d.SubscriptionID]
		ev := m.eventsByID[d.EventID]
		out = append(out, store.ClaimedDelivery{
			DeliveryID:     d.ID,
			EventID:        d.EventID,
			SubscriptionID: d.SubscriptionID,
			Attempts:       d.Attempts,
			WebhookURL:     sub.WebhookURL,
			Payload:        ev.Payload,
		})
	}
	return out, nil
}

func (m *memoryStore) CreateDeliveryAttempt(_ context.Context, deliveryID int64, attemptNo int, httpStatus *int, errorMessage, responseBody *string) (store.DeliveryAttempt, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	a := store.DeliveryAttempt{
		ID:           m.nextAttemptID,
		DeliveryID:   deliveryID,
		AttemptNo:    attemptNo,
		AttemptedAt:  time.Now().UTC(),
		HTTPStatus:   httpStatus,
		ErrorMessage: errorMessage,
		ResponseBody: responseBody,
	}
	m.nextAttemptID++
	m.attemptsByDeliveryID[deliveryID] = append(m.attemptsByDeliveryID[deliveryID], a)
	return a, nil
}

func (m *memoryStore) UpdateDeliveryStatus(_ context.Context, id int64, status string, attempts int, nextAttemptAt time.Time, lastError *string, httpStatus *int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	d := m.deliveriesByID[id]
	d.Status = status
	d.Attempts = attempts
	d.NextAttemptAt = nextAttemptAt
	d.LastError = lastError
	d.HTTPStatus = httpStatus
	d.UpdatedAt = time.Now().UTC()
	m.deliveriesByID[id] = d
	return nil
}

func TestE2EIngestFanoutDeliveredAudit(t *testing.T) {
	t.Logf("step=setup action=create in-memory store and API test server")
	mem := newMemoryStore()
	router := api.NewRouter(mem, matcher.New())
	apiServer := httptest.NewServer(router)
	defer apiServer.Close()

	t.Logf("step=setup action=create webhook stub expected_http_status=200")
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`ok`))
	}))
	defer webhook.Close()

	t.Logf("step=subscription action=create filter=type+source+amount_gt_100 expected=201")
	createSubscription(t, apiServer.URL, webhook.URL, `{"type":"order.created","source":"checkout-svc","payload":{"amount":{"gt":100}}}`)
	t.Logf("step=event action=ingest expected=202+fanout_to_single_subscription")
	eventID := ingestEvent(t, apiServer.URL, "ik-success", `{"type":"order.created","source":"checkout-svc","payload":{"amount":120}}`)
	t.Logf("step=event result=accepted event_id=%d expected_fanout_count=1", eventID)

	wp := worker.NewPool(mem, &http.Client{}, worker.Config{
		WorkerCount:    1,
		ClaimBatchSize: 10,
		MaxAttempts:    3,
		PollInterval:   1 * time.Millisecond,
		WebhookTimeout: 200 * time.Millisecond,
		BaseBackoff:    1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})
	t.Logf("step=worker action=process delivery batch runs=5 expected=delivery transitions to delivered")
	for i := 0; i < 5; i++ {
		n, err := wp.ProcessOnce(context.Background())
		t.Logf("step=worker iteration=%d claimed=%d err=%v", i+1, n, err)
	}

	items := fetchAuditByEventID(t, apiServer.URL, eventID)
	t.Logf("step=audit observed_deliveries=%d expected_deliveries=1", len(items))
	if len(items) != 1 {
		t.Fatalf("deliveries len = %d, want 1", len(items))
	}
	t.Logf("step=audit observed_status=%s expected_status=delivered observed_attempts=%d expected_attempts=1", items[0].Delivery.Status, len(items[0].Attempts))
	if items[0].Delivery.Status != "delivered" {
		t.Fatalf("status = %s, want delivered", items[0].Delivery.Status)
	}
	if len(items[0].Attempts) != 1 {
		t.Fatalf("attempts len = %d, want 1", len(items[0].Attempts))
	}
}

func TestE2EFailingWebhookRetriesThenFailed(t *testing.T) {
	t.Logf("step=setup action=create in-memory store and API test server")
	mem := newMemoryStore()
	router := api.NewRouter(mem, matcher.New())
	apiServer := httptest.NewServer(router)
	defer apiServer.Close()

	t.Logf("step=setup action=create webhook stub expected_http_status=500")
	webhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`fail`))
	}))
	defer webhook.Close()

	t.Logf("step=subscription action=create filter=type+source expected=201")
	createSubscription(t, apiServer.URL, webhook.URL, `{"type":"order.created","source":"checkout-svc"}`)
	t.Logf("step=event action=ingest expected=202")
	eventID := ingestEvent(t, apiServer.URL, "ik-fail", `{"type":"order.created","source":"checkout-svc","payload":{"amount":80}}`)
	t.Logf("step=event result=accepted event_id=%d expected_final_status=failed after max attempts", eventID)

	wp := worker.NewPool(mem, &http.Client{}, worker.Config{
		WorkerCount:    1,
		ClaimBatchSize: 10,
		MaxAttempts:    3,
		PollInterval:   1 * time.Millisecond,
		WebhookTimeout: 200 * time.Millisecond,
		BaseBackoff:    1 * time.Millisecond,
		MaxBackoff:     5 * time.Millisecond,
	})

	deadline := time.Now().Add(2 * time.Second)
	var items []auditItem
	for time.Now().Before(deadline) {
		n, err := wp.ProcessOnce(context.Background())
		items = fetchAuditByEventID(t, apiServer.URL, eventID)
		if len(items) > 0 {
			t.Logf(
				"step=retry-loop claimed=%d err=%v observed_status=%s observed_attempts=%d expected_terminal_status=failed",
				n,
				err,
				items[0].Delivery.Status,
				len(items[0].Attempts),
			)
		} else {
			t.Logf("step=retry-loop claimed=%d err=%v observed_deliveries=0", n, err)
		}
		if len(items) == 1 && items[0].Delivery.Status == "failed" {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	if len(items) != 1 {
		t.Fatalf("deliveries len = %d, want 1", len(items))
	}
	if items[0].Delivery.Status != "failed" {
		t.Fatalf("status = %s, want failed", items[0].Delivery.Status)
	}
	if len(items[0].Attempts) != 3 {
		t.Fatalf("attempts len = %d, want 3", len(items[0].Attempts))
	}
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
	t.Logf("action=parse_response path=/events event_id=%d fanout_count=%v idempotent_replay=%v", int64(eventID), payload["fanout_count"], payload["idempotent_replay"])
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
