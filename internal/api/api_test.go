package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/notification-fanout/service/internal/api"
	"github.com/notification-fanout/service/internal/matcher"
	"github.com/notification-fanout/service/internal/store"
)

type fakeSubscriptionStore struct {
	readyFn  func(ctx context.Context) error
	createFn func(ctx context.Context, webhookURL string, filter json.RawMessage, eventType, source *string) (store.Subscription, error)
	listFn   func(ctx context.Context) ([]store.Subscription, error)
	deleteFn func(ctx context.Context, id int64) error
	ingestFn func(ctx context.Context, idempotencyKey *string, eventType, source string, payload json.RawMessage) (store.IngestResult, error)
	listByEventFn        func(ctx context.Context, eventID int64) ([]store.Delivery, error)
	listBySubscriptionFn func(ctx context.Context, subscriptionID int64) ([]store.Delivery, error)
	listAttemptsFn       func(ctx context.Context, deliveryID int64) ([]store.DeliveryAttempt, error)
}

func (f *fakeSubscriptionStore) Ready(ctx context.Context) error {
	if f.readyFn == nil {
		return nil
	}
	return f.readyFn(ctx)
}

func (f *fakeSubscriptionStore) CreateSubscription(ctx context.Context, webhookURL string, filter json.RawMessage, eventType, source *string) (store.Subscription, error) {
	if f.createFn == nil {
		return store.Subscription{}, nil
	}
	return f.createFn(ctx, webhookURL, filter, eventType, source)
}

func (f *fakeSubscriptionStore) ListActiveSubscriptions(ctx context.Context) ([]store.Subscription, error) {
	if f.listFn == nil {
		return nil, nil
	}
	return f.listFn(ctx)
}

func (f *fakeSubscriptionStore) SoftDeleteSubscription(ctx context.Context, id int64) error {
	if f.deleteFn == nil {
		return nil
	}
	return f.deleteFn(ctx, id)
}

func (f *fakeSubscriptionStore) IngestEventWithFanout(ctx context.Context, idempotencyKey *string, eventType, source string, payload json.RawMessage) (store.IngestResult, error) {
	if f.ingestFn == nil {
		return store.IngestResult{}, nil
	}
	return f.ingestFn(ctx, idempotencyKey, eventType, source, payload)
}

func (f *fakeSubscriptionStore) ListDeliveriesByEventID(ctx context.Context, eventID int64) ([]store.Delivery, error) {
	if f.listByEventFn == nil {
		return nil, nil
	}
	return f.listByEventFn(ctx, eventID)
}

func (f *fakeSubscriptionStore) ListDeliveriesBySubscriptionID(ctx context.Context, subscriptionID int64) ([]store.Delivery, error) {
	if f.listBySubscriptionFn == nil {
		return nil, nil
	}
	return f.listBySubscriptionFn(ctx, subscriptionID)
}

func (f *fakeSubscriptionStore) ListDeliveryAttempts(ctx context.Context, deliveryID int64) ([]store.DeliveryAttempt, error) {
	if f.listAttemptsFn == nil {
		return nil, nil
	}
	return f.listAttemptsFn(ctx, deliveryID)
}

func TestNewRouter(t *testing.T) {
	r := api.NewRouter(&fakeSubscriptionStore{
		createFn: func(context.Context, string, json.RawMessage, *string, *string) (store.Subscription, error) {
			return store.Subscription{}, nil
		},
		listFn: func(context.Context) ([]store.Subscription, error) { return nil, nil },
		deleteFn: func(context.Context, int64) error { return nil },
		ingestFn: func(context.Context, *string, string, string, json.RawMessage) (store.IngestResult, error) {
			return store.IngestResult{}, nil
		},
	}, matcher.New())
	if r == nil {
		t.Fatal("NewRouter returned nil")
	}
}

func TestCreateSubscriptionSuccess(t *testing.T) {
	now := time.Now().UTC()
	var gotType, gotSource *string
	s := &fakeSubscriptionStore{
		createFn: func(_ context.Context, webhookURL string, filter json.RawMessage, eventType, source *string) (store.Subscription, error) {
			if webhookURL != "https://example.com/hook" {
				t.Fatalf("webhookURL = %q", webhookURL)
			}
			gotType = eventType
			gotSource = source
			return store.Subscription{
				ID:         10,
				WebhookURL: webhookURL,
				Filter:     filter,
				Type:       eventType,
				Source:     source,
				Active:     true,
				CreatedAt:  now,
				UpdatedAt:  now,
			}, nil
		},
		listFn:   func(context.Context) ([]store.Subscription, error) { return nil, nil },
		deleteFn: func(context.Context, int64) error { return nil },
		ingestFn: func(context.Context, *string, string, string, json.RawMessage) (store.IngestResult, error) {
			return store.IngestResult{}, nil
		},
	}

	r := api.NewRouter(s, matcher.New())
	req := httptest.NewRequest(http.MethodPost, "/subscriptions", strings.NewReader(`{
		"webhook_url":"https://example.com/hook",
		"filter":{"type":"order.created","source":"checkout-svc","payload":{"amount":{"gt":100}}}
	}`))
	w := httptest.NewRecorder()

	r.ServeHTTP(w, req)
	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusCreated, w.Body.String())
	}
	if gotType == nil || *gotType != "order.created" {
		t.Fatalf("denormalized type = %#v, want order.created", gotType)
	}
	if gotSource == nil || *gotSource != "checkout-svc" {
		t.Fatalf("denormalized source = %#v, want checkout-svc", gotSource)
	}
}

func TestCreateSubscriptionValidation(t *testing.T) {
	s := &fakeSubscriptionStore{
		createFn: func(_ context.Context, _ string, _ json.RawMessage, _, _ *string) (store.Subscription, error) {
			t.Fatal("CreateSubscription should not be called on validation errors")
			return store.Subscription{}, nil
		},
		listFn:   func(context.Context) ([]store.Subscription, error) { return nil, nil },
		deleteFn: func(context.Context, int64) error { return nil },
		ingestFn: func(context.Context, *string, string, string, json.RawMessage) (store.IngestResult, error) {
			return store.IngestResult{}, nil
		},
	}
	r := api.NewRouter(s, matcher.New())

	tests := []struct {
		name string
		body string
	}{
		{
			name: "invalid webhook url",
			body: `{"webhook_url":"ftp://example.com/hook","filter":{"type":"order.created"}}`,
		},
		{
			name: "invalid filter",
			body: `{"webhook_url":"https://example.com/hook","filter":{"payload":{"amount":{"between":[1,2]}}}}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/subscriptions", strings.NewReader(tt.body))
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusBadRequest, w.Body.String())
			}
		})
	}
}

func TestListSubscriptionsSuccess(t *testing.T) {
	s := &fakeSubscriptionStore{
		createFn: func(context.Context, string, json.RawMessage, *string, *string) (store.Subscription, error) {
			return store.Subscription{}, nil
		},
		listFn: func(context.Context) ([]store.Subscription, error) {
			return []store.Subscription{
				{ID: 1, WebhookURL: "https://a.example", Active: true},
				{ID: 2, WebhookURL: "https://b.example", Active: true},
			}, nil
		},
		deleteFn: func(context.Context, int64) error { return nil },
		ingestFn: func(context.Context, *string, string, string, json.RawMessage) (store.IngestResult, error) {
			return store.IngestResult{}, nil
		},
	}
	r := api.NewRouter(s, matcher.New())

	req := httptest.NewRequest(http.MethodGet, "/subscriptions", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var subs []store.Subscription
	if err := json.Unmarshal(w.Body.Bytes(), &subs); err != nil {
		t.Fatalf("unmarshal list response: %v", err)
	}
	if len(subs) != 2 || subs[0].ID != 1 || subs[1].ID != 2 {
		t.Fatalf("unexpected subscriptions: %+v", subs)
	}
}

func TestDeleteSubscription(t *testing.T) {
	var gotID int64
	s := &fakeSubscriptionStore{
		createFn: func(context.Context, string, json.RawMessage, *string, *string) (store.Subscription, error) {
			return store.Subscription{}, nil
		},
		listFn: func(context.Context) ([]store.Subscription, error) { return nil, nil },
		deleteFn: func(_ context.Context, id int64) error {
			gotID = id
			return nil
		},
		ingestFn: func(context.Context, *string, string, string, json.RawMessage) (store.IngestResult, error) {
			return store.IngestResult{}, nil
		},
	}
	r := api.NewRouter(s, matcher.New())

	req := httptest.NewRequest(http.MethodDelete, "/subscriptions/42", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNoContent)
	}
	if gotID != 42 {
		t.Fatalf("deleted id = %d, want 42", gotID)
	}
}

func TestDeleteSubscriptionInvalidID(t *testing.T) {
	s := &fakeSubscriptionStore{
		createFn: func(context.Context, string, json.RawMessage, *string, *string) (store.Subscription, error) {
			return store.Subscription{}, nil
		},
		listFn:   func(context.Context) ([]store.Subscription, error) { return nil, nil },
		deleteFn: func(context.Context, int64) error { return nil },
		ingestFn: func(context.Context, *string, string, string, json.RawMessage) (store.IngestResult, error) {
			return store.IngestResult{}, nil
		},
	}
	r := api.NewRouter(s, matcher.New())

	req := httptest.NewRequest(http.MethodDelete, "/subscriptions/not-a-number", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestHandlersStoreError(t *testing.T) {
	s := &fakeSubscriptionStore{
		createFn: func(context.Context, string, json.RawMessage, *string, *string) (store.Subscription, error) {
			return store.Subscription{}, errors.New("boom")
		},
		listFn: func(context.Context) ([]store.Subscription, error) { return nil, errors.New("boom") },
		deleteFn: func(context.Context, int64) error { return errors.New("boom") },
		ingestFn: func(context.Context, *string, string, string, json.RawMessage) (store.IngestResult, error) {
			return store.IngestResult{}, errors.New("boom")
		},
	}
	r := api.NewRouter(s, matcher.New())

	postReq := httptest.NewRequest(http.MethodPost, "/subscriptions", strings.NewReader(`{
		"webhook_url":"https://example.com/hook",
		"filter":{"type":"order.created"}
	}`))
	postW := httptest.NewRecorder()
	r.ServeHTTP(postW, postReq)
	if postW.Code != http.StatusInternalServerError {
		t.Fatalf("POST status = %d, want %d", postW.Code, http.StatusInternalServerError)
	}

	getReq := httptest.NewRequest(http.MethodGet, "/subscriptions", nil)
	getW := httptest.NewRecorder()
	r.ServeHTTP(getW, getReq)
	if getW.Code != http.StatusInternalServerError {
		t.Fatalf("GET status = %d, want %d", getW.Code, http.StatusInternalServerError)
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/subscriptions/1", nil)
	deleteW := httptest.NewRecorder()
	r.ServeHTTP(deleteW, deleteReq)
	if deleteW.Code != http.StatusInternalServerError {
		t.Fatalf("DELETE status = %d, want %d", deleteW.Code, http.StatusInternalServerError)
	}
}

func TestIngestEventAcceptedAndFanoutCount(t *testing.T) {
	var gotIdem *string
	s := &fakeSubscriptionStore{
		createFn: func(context.Context, string, json.RawMessage, *string, *string) (store.Subscription, error) {
			return store.Subscription{}, nil
		},
		listFn:   func(context.Context) ([]store.Subscription, error) { return nil, nil },
		deleteFn: func(context.Context, int64) error { return nil },
		ingestFn: func(_ context.Context, idempotencyKey *string, eventType, source string, payload json.RawMessage) (store.IngestResult, error) {
			gotIdem = idempotencyKey
			if eventType != "order.created" || source != "checkout-svc" {
				t.Fatalf("unexpected type/source: %s %s", eventType, source)
			}
			return store.IngestResult{EventID: 55, FanoutCount: 2, IdempotentReplay: false}, nil
		},
	}
	r := api.NewRouter(s, matcher.New())

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(`{
		"type":"order.created",
		"source":"checkout-svc",
		"payload":{"amount":120}
	}`))
	req.Header.Set("Idempotency-Key", "ik-abc")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusAccepted, w.Body.String())
	}
	if gotIdem == nil || *gotIdem != "ik-abc" {
		t.Fatalf("idempotency key = %#v, want ik-abc", gotIdem)
	}

	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if body["fanout_count"] != float64(2) {
		t.Fatalf("fanout_count = %v, want 2", body["fanout_count"])
	}
}

func TestIngestEventIdempotentReplay(t *testing.T) {
	s := &fakeSubscriptionStore{
		createFn: func(context.Context, string, json.RawMessage, *string, *string) (store.Subscription, error) {
			return store.Subscription{}, nil
		},
		listFn:   func(context.Context) ([]store.Subscription, error) { return nil, nil },
		deleteFn: func(context.Context, int64) error { return nil },
		ingestFn: func(context.Context, *string, string, string, json.RawMessage) (store.IngestResult, error) {
			return store.IngestResult{EventID: 88, FanoutCount: 0, IdempotentReplay: true}, nil
		},
	}
	r := api.NewRouter(s, matcher.New())

	req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(`{
		"type":"order.created",
		"source":"checkout-svc",
		"payload":{"amount":120}
	}`))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusAccepted)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if body["idempotent_replay"] != true {
		t.Fatalf("idempotent_replay = %v, want true", body["idempotent_replay"])
	}
}

func TestIngestEventValidation(t *testing.T) {
	s := &fakeSubscriptionStore{
		createFn: func(context.Context, string, json.RawMessage, *string, *string) (store.Subscription, error) {
			return store.Subscription{}, nil
		},
		listFn:   func(context.Context) ([]store.Subscription, error) { return nil, nil },
		deleteFn: func(context.Context, int64) error { return nil },
		ingestFn: func(context.Context, *string, string, string, json.RawMessage) (store.IngestResult, error) {
			t.Fatal("IngestEventWithFanout should not be called for invalid payload")
			return store.IngestResult{}, nil
		},
	}
	r := api.NewRouter(s, matcher.New())

	tests := []string{
		`{"source":"checkout-svc","payload":{"amount":120}}`,
		`{"type":"order.created","payload":{"amount":120}}`,
		`{"type":"order.created","source":"checkout-svc","payload":"not-object"}`,
	}
	for _, body := range tests {
		req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(body))
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d for body=%s", w.Code, http.StatusBadRequest, body)
		}
	}
}

func TestListDeliveriesByEventID(t *testing.T) {
	s := &fakeSubscriptionStore{
		listByEventFn: func(_ context.Context, eventID int64) ([]store.Delivery, error) {
			if eventID != 55 {
				t.Fatalf("eventID = %d, want 55", eventID)
			}
			return []store.Delivery{{ID: 101, EventID: 55, SubscriptionID: 9, Status: "delivered", Attempts: 1}}, nil
		},
		listAttemptsFn: func(_ context.Context, deliveryID int64) ([]store.DeliveryAttempt, error) {
			if deliveryID != 101 {
				t.Fatalf("deliveryID = %d, want 101", deliveryID)
			}
			code := 200
			return []store.DeliveryAttempt{{ID: 1, DeliveryID: 101, AttemptNo: 1, HTTPStatus: &code}}, nil
		},
	}
	r := api.NewRouter(s, matcher.New())

	req := httptest.NewRequest(http.MethodGet, "/deliveries?event_id=55", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusOK, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	deliveries, ok := body["deliveries"].([]any)
	if !ok || len(deliveries) != 1 {
		t.Fatalf("deliveries = %#v, want len 1", body["deliveries"])
	}
}

func TestListDeliveriesBySubscriptionID(t *testing.T) {
	s := &fakeSubscriptionStore{
		listBySubscriptionFn: func(_ context.Context, subscriptionID int64) ([]store.Delivery, error) {
			if subscriptionID != 9 {
				t.Fatalf("subscriptionID = %d, want 9", subscriptionID)
			}
			return []store.Delivery{{ID: 201, EventID: 88, SubscriptionID: 9, Status: "retrying", Attempts: 2}}, nil
		},
		listAttemptsFn: func(_ context.Context, deliveryID int64) ([]store.DeliveryAttempt, error) {
			if deliveryID != 201 {
				t.Fatalf("deliveryID = %d, want 201", deliveryID)
			}
			return []store.DeliveryAttempt{
				{ID: 1, DeliveryID: 201, AttemptNo: 1},
				{ID: 2, DeliveryID: 201, AttemptNo: 2},
			}, nil
		},
	}
	r := api.NewRouter(s, matcher.New())

	req := httptest.NewRequest(http.MethodGet, "/deliveries?subscription_id=9", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	deliveries, ok := body["deliveries"].([]any)
	if !ok || len(deliveries) != 1 {
		t.Fatalf("deliveries = %#v, want len 1", body["deliveries"])
	}
}

func TestListDeliveriesValidation(t *testing.T) {
	r := api.NewRouter(&fakeSubscriptionStore{}, matcher.New())

	tests := []string{
		"/deliveries",
		"/deliveries?event_id=1&subscription_id=2",
		"/deliveries?event_id=nope",
		"/deliveries?subscription_id=nope",
	}
	for _, path := range tests {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("path=%s status=%d want=%d", path, w.Code, http.StatusBadRequest)
		}
	}
}

func TestListDeliveriesStoreErrors(t *testing.T) {
	s := &fakeSubscriptionStore{
		listByEventFn: func(context.Context, int64) ([]store.Delivery, error) {
			return nil, errors.New("boom")
		},
		listAttemptsFn: func(context.Context, int64) ([]store.DeliveryAttempt, error) {
			return nil, errors.New("boom")
		},
	}
	r := api.NewRouter(s, matcher.New())

	req := httptest.NewRequest(http.MethodGet, "/deliveries?event_id=1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusInternalServerError)
	}

	s2 := &fakeSubscriptionStore{
		listByEventFn: func(context.Context, int64) ([]store.Delivery, error) {
			return []store.Delivery{{ID: 1}}, nil
		},
		listAttemptsFn: func(context.Context, int64) ([]store.DeliveryAttempt, error) {
			return nil, errors.New("boom")
		},
	}
	r2 := api.NewRouter(s2, matcher.New())
	req2 := httptest.NewRequest(http.MethodGet, "/deliveries?event_id=1", nil)
	w2 := httptest.NewRecorder()
	r2.ServeHTTP(w2, req2)
	if w2.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", w2.Code, http.StatusInternalServerError)
	}
}

func TestHealthz(t *testing.T) {
	r := api.NewRouter(&fakeSubscriptionStore{}, matcher.New())
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestReadyzStates(t *testing.T) {
	readyRouter := api.NewRouter(&fakeSubscriptionStore{
		readyFn: func(context.Context) error { return nil },
	}, matcher.New())
	readyReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	readyW := httptest.NewRecorder()
	readyRouter.ServeHTTP(readyW, readyReq)
	if readyW.Code != http.StatusOK {
		t.Fatalf("ready status = %d, want %d", readyW.Code, http.StatusOK)
	}

	notReadyRouter := api.NewRouter(&fakeSubscriptionStore{
		readyFn: func(context.Context) error { return errors.New("db down") },
	}, matcher.New())
	notReadyReq := httptest.NewRequest(http.MethodGet, "/readyz", nil)
	notReadyW := httptest.NewRecorder()
	notReadyRouter.ServeHTTP(notReadyW, notReadyReq)
	if notReadyW.Code != http.StatusServiceUnavailable {
		t.Fatalf("not-ready status = %d, want %d", notReadyW.Code, http.StatusServiceUnavailable)
	}
}
