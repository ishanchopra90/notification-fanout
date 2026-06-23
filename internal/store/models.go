package store

import (
	"encoding/json"
	"time"
)

type Event struct {
	ID             int64
	IdempotencyKey *string
	Type           string
	Source         string
	Payload        json.RawMessage
	CreatedAt      time.Time
}

type Subscription struct {
	ID         int64
	WebhookURL string
	Filter     json.RawMessage
	Type       *string
	Source     *string
	Active     bool
	CreatedAt  time.Time
	UpdatedAt  time.Time
	DeletedAt  *time.Time
}

type Delivery struct {
	ID             int64
	EventID        int64
	SubscriptionID int64
	Status         string
	Attempts       int
	NextAttemptAt  time.Time
	LastError      *string
	HTTPStatus     *int
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type DeliveryAttempt struct {
	ID           int64
	DeliveryID   int64
	AttemptNo    int
	AttemptedAt  time.Time
	HTTPStatus   *int
	ErrorMessage *string
	ResponseBody *string
}

type IngestResult struct {
	EventID          int64
	FanoutCount      int
	IdempotentReplay bool
}

type ClaimedDelivery struct {
	DeliveryID     int64
	EventID        int64
	SubscriptionID int64
	Attempts       int
	WebhookURL     string
	Payload        json.RawMessage
}
