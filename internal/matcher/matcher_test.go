package matcher_test

import (
	"encoding/json"
	"testing"

	"github.com/notification-fanout/service/internal/matcher"
)

func TestNew(t *testing.T) {
	m := matcher.New()
	if m == nil {
		t.Fatal("New returned nil")
	}
}

func TestParseFilterValidAndMatch(t *testing.T) {
	m := matcher.New()

	filterJSON := []byte(`{
		"type":"order.created",
		"source":"checkout-svc",
		"payload":{
			"region":"us",
			"amount":{"gt":100},
			"attempts":{"lte":3}
		}
	}`)

	filter, err := m.ParseFilter(filterJSON)
	if err != nil {
		t.Fatalf("ParseFilter() error = %v", err)
	}

	event := matcher.Event{
		Type:   "order.created",
		Source: "checkout-svc",
		Payload: map[string]any{
			"region":   "us",
			"amount":   120.0,
			"attempts": 3.0,
		},
	}

	if !m.Match(filter, event) {
		t.Fatal("Match() = false, want true")
	}
}

func TestParseFilterInvalidOperator(t *testing.T) {
	m := matcher.New()
	_, err := m.ParseFilter([]byte(`{"payload":{"amount":{"between":[10,20]}}}`))
	if err == nil {
		t.Fatal("ParseFilter() expected invalid operator error")
	}
}

func TestParseFilterInvalidNumericOperand(t *testing.T) {
	m := matcher.New()
	_, err := m.ParseFilter([]byte(`{"payload":{"amount":{"gt":"high"}}}`))
	if err == nil {
		t.Fatal("ParseFilter() expected numeric operand error")
	}
}

func TestParseFilterInvalidMultipleOperators(t *testing.T) {
	m := matcher.New()
	_, err := m.ParseFilter([]byte(`{"payload":{"amount":{"gt":1,"lt":9}}}`))
	if err == nil {
		t.Fatal("ParseFilter() expected multiple operators error")
	}
}

func TestMatchNoMatchCases(t *testing.T) {
	m := matcher.New()

	filter, err := m.ParseFilter([]byte(`{
		"type":"order.created",
		"source":"checkout-svc",
		"payload":{"region":"us"}
	}`))
	if err != nil {
		t.Fatalf("ParseFilter() error = %v", err)
	}

	cases := []matcher.Event{
		{Type: "order.updated", Source: "checkout-svc", Payload: map[string]any{"region": "us"}},
		{Type: "order.created", Source: "payments-svc", Payload: map[string]any{"region": "us"}},
		{Type: "order.created", Source: "checkout-svc", Payload: map[string]any{"region": "eu"}},
		{Type: "order.created", Source: "checkout-svc", Payload: map[string]any{}},
	}

	for i, event := range cases {
		if m.Match(filter, event) {
			t.Fatalf("case %d: Match() = true, want false", i)
		}
	}
}

func TestOperators(t *testing.T) {
	m := matcher.New()

	type tc struct {
		name       string
		filterJSON string
		value      any
		want       bool
	}

	tests := []tc{
		{name: "eq match literal", filterJSON: `{"payload":{"amount":{"eq":120}}}`, value: 120.0, want: true},
		{name: "eq no match literal", filterJSON: `{"payload":{"amount":{"eq":120}}}`, value: 99.0, want: false},
		{name: "eq shorthand match", filterJSON: `{"payload":{"region":"us"}}`, value: "us", want: true},
		{name: "gt match", filterJSON: `{"payload":{"amount":{"gt":100}}}`, value: 120.0, want: true},
		{name: "gt no match", filterJSON: `{"payload":{"amount":{"gt":100}}}`, value: 100.0, want: false},
		{name: "gte match", filterJSON: `{"payload":{"amount":{"gte":100}}}`, value: 100.0, want: true},
		{name: "gte no match", filterJSON: `{"payload":{"amount":{"gte":100}}}`, value: 99.0, want: false},
		{name: "lt match", filterJSON: `{"payload":{"amount":{"lt":100}}}`, value: 80.0, want: true},
		{name: "lt no match", filterJSON: `{"payload":{"amount":{"lt":100}}}`, value: 100.0, want: false},
		{name: "lte match", filterJSON: `{"payload":{"amount":{"lte":100}}}`, value: 100.0, want: true},
		{name: "lte no match", filterJSON: `{"payload":{"amount":{"lte":100}}}`, value: 101.0, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter, err := m.ParseFilter([]byte(tt.filterJSON))
			if err != nil {
				t.Fatalf("ParseFilter() error = %v", err)
			}
			event := matcher.Event{
				Type:   "order.created",
				Source: "checkout-svc",
				Payload: map[string]any{
					"amount": tt.value,
					"region": tt.value,
				},
			}
			if got := m.Match(filter, event); got != tt.want {
				t.Fatalf("Match() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseFilterFromJSONPayload(t *testing.T) {
	m := matcher.New()
	raw := json.RawMessage(`{"payload":{"count":{"gte":2}}}`)
	filter, err := m.ParseFilter(raw)
	if err != nil {
		t.Fatalf("ParseFilter() error = %v", err)
	}

	event := matcher.Event{
		Payload: map[string]any{"count": 3.0},
	}
	if !m.Match(filter, event) {
		t.Fatal("Match() = false, want true")
	}
}
