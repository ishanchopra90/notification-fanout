package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strconv"

	"github.com/go-chi/chi/v5"
	"github.com/notification-fanout/service/internal/matcher"
	"github.com/notification-fanout/service/internal/store"
)

type SubscriptionStore interface {
	Ready(ctx context.Context) error
	CreateSubscription(ctx context.Context, webhookURL string, filter json.RawMessage, eventType, source *string) (store.Subscription, error)
	ListActiveSubscriptions(ctx context.Context) ([]store.Subscription, error)
	SoftDeleteSubscription(ctx context.Context, id int64) error
	IngestEventWithFanout(ctx context.Context, idempotencyKey *string, eventType, source string, payload json.RawMessage) (store.IngestResult, error)
	ListDeliveriesByEventID(ctx context.Context, eventID int64) ([]store.Delivery, error)
	ListDeliveriesBySubscriptionID(ctx context.Context, subscriptionID int64) ([]store.Delivery, error)
	ListDeliveryAttempts(ctx context.Context, deliveryID int64) ([]store.DeliveryAttempt, error)
}

type API struct {
	store   SubscriptionStore
	matcher *matcher.Matcher
}

type createSubscriptionRequest struct {
	WebhookURL string          `json:"webhook_url"`
	Filter     json.RawMessage `json:"filter"`
}

type ingestEventRequest struct {
	Type    string          `json:"type"`
	Source  string          `json:"source"`
	Payload json.RawMessage `json:"payload"`
}

type deliveryAuditItem struct {
	Delivery store.Delivery          `json:"delivery"`
	Attempts []store.DeliveryAttempt `json:"attempts"`
}

// NewRouter returns the HTTP router for the notification fanout service.
func NewRouter(subscriptionStore SubscriptionStore, m *matcher.Matcher) *chi.Mux {
	r := chi.NewRouter()
	api := &API{
		store:   subscriptionStore,
		matcher: m,
	}

	r.Post("/subscriptions", api.handleCreateSubscription)
	r.Get("/subscriptions", api.handleListSubscriptions)
	r.Delete("/subscriptions/{id}", api.handleDeleteSubscription)
	r.Post("/events", api.handleIngestEvent)
	r.Get("/deliveries", api.handleListDeliveries)
	r.Get("/healthz", api.handleHealthz)
	r.Get("/readyz", api.handleReadyz)

	return r
}

func (a *API) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req createSubscriptionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	if err := validateWebhookURL(req.WebhookURL); err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}
	if len(req.Filter) == 0 {
		writeJSONError(w, http.StatusBadRequest, "filter is required")
		return
	}

	filter, err := a.matcher.ParseFilter(req.Filter)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid filter: "+err.Error())
		return
	}

	sub, err := a.store.CreateSubscription(r.Context(), req.WebhookURL, req.Filter, filter.Type, filter.Source)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to create subscription")
		return
	}

	writeJSON(w, http.StatusCreated, sub)
}

func (a *API) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	subs, err := a.store.ListActiveSubscriptions(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to list subscriptions")
		return
	}
	writeJSON(w, http.StatusOK, subs)
}

func (a *API) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid subscription id")
		return
	}

	if err := a.store.SoftDeleteSubscription(r.Context(), id); err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to delete subscription")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *API) handleIngestEvent(w http.ResponseWriter, r *http.Request) {
	var req ingestEventRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Type == "" {
		writeJSONError(w, http.StatusBadRequest, "type is required")
		return
	}
	if req.Source == "" {
		writeJSONError(w, http.StatusBadRequest, "source is required")
		return
	}
	if len(req.Payload) == 0 {
		writeJSONError(w, http.StatusBadRequest, "payload is required")
		return
	}
	var payloadObj map[string]any
	if err := json.Unmarshal(req.Payload, &payloadObj); err != nil {
		writeJSONError(w, http.StatusBadRequest, "payload must be a JSON object")
		return
	}

	var idempotencyKey *string
	if header := r.Header.Get("Idempotency-Key"); header != "" {
		idempotencyKey = &header
	}

	result, err := a.store.IngestEventWithFanout(r.Context(), idempotencyKey, req.Type, req.Source, req.Payload)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to ingest event")
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"event_id":          result.EventID,
		"fanout_count":      result.FanoutCount,
		"idempotent_replay": result.IdempotentReplay,
	})
}

func (a *API) handleListDeliveries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	eventIDRaw := q.Get("event_id")
	subIDRaw := q.Get("subscription_id")

	if (eventIDRaw == "" && subIDRaw == "") || (eventIDRaw != "" && subIDRaw != "") {
		writeJSONError(w, http.StatusBadRequest, "exactly one of event_id or subscription_id must be provided")
		return
	}

	var (
		deliveries []store.Delivery
		err        error
	)

	if eventIDRaw != "" {
		eventID, parseErr := strconv.ParseInt(eventIDRaw, 10, 64)
		if parseErr != nil || eventID <= 0 {
			writeJSONError(w, http.StatusBadRequest, "event_id must be a positive integer")
			return
		}
		deliveries, err = a.store.ListDeliveriesByEventID(r.Context(), eventID)
	} else {
		subID, parseErr := strconv.ParseInt(subIDRaw, 10, 64)
		if parseErr != nil || subID <= 0 {
			writeJSONError(w, http.StatusBadRequest, "subscription_id must be a positive integer")
			return
		}
		deliveries, err = a.store.ListDeliveriesBySubscriptionID(r.Context(), subID)
	}
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "failed to list deliveries")
		return
	}

	items := make([]deliveryAuditItem, 0, len(deliveries))
	for _, d := range deliveries {
		attempts, attemptsErr := a.store.ListDeliveryAttempts(r.Context(), d.ID)
		if attemptsErr != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to list delivery attempts")
			return
		}
		items = append(items, deliveryAuditItem{
			Delivery: d,
			Attempts: attempts,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"deliveries": items,
	})
}

func (a *API) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := a.store.Ready(r.Context()); err != nil {
		writeJSONError(w, http.StatusServiceUnavailable, "not ready")
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func validateWebhookURL(raw string) error {
	if raw == "" {
		return errors.New("webhook_url is required")
	}

	parsed, err := url.Parse(raw)
	if err != nil {
		return errors.New("webhook_url must be a valid URL")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("webhook_url must use http or https")
	}
	if parsed.Host == "" {
		return errors.New("webhook_url host is required")
	}

	return nil
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
