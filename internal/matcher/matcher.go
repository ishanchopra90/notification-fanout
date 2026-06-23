package matcher

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
)

// Matcher evaluates subscription filter rules against event payloads.
type Matcher struct{}

type Operator string

const (
	OpEQ  Operator = "eq"
	OpGT  Operator = "gt"
	OpGTE Operator = "gte"
	OpLT  Operator = "lt"
	OpLTE Operator = "lte"
)

// Event is the normalized event shape used for rule evaluation.
type Event struct {
	Type    string
	Source  string
	Payload map[string]any
}

// Filter is the parsed subscription filter.
type Filter struct {
	Type    *string
	Source  *string
	Payload map[string]Condition
}

// Condition represents one payload predicate.
type Condition struct {
	Operator Operator
	Value    any
}

type rawFilter struct {
	Type    *string        `json:"type"`
	Source  *string        `json:"source"`
	Payload map[string]any `json:"payload"`
}

// New returns a Matcher ready to evaluate filter rules.
func New() *Matcher {
	return &Matcher{}
}

// ParseFilter validates and parses a subscription filter JSON payload.
func (m *Matcher) ParseFilter(raw []byte) (Filter, error) {
	var rf rawFilter
	if err := json.Unmarshal(raw, &rf); err != nil {
		return Filter{}, fmt.Errorf("parse filter JSON: %w", err)
	}

	filter := Filter{
		Type:    rf.Type,
		Source:  rf.Source,
		Payload: make(map[string]Condition, len(rf.Payload)),
	}

	for key, value := range rf.Payload {
		condition, err := parsePayloadCondition(key, value)
		if err != nil {
			return Filter{}, err
		}
		filter.Payload[key] = condition
	}

	return filter, nil
}

// Match evaluates whether an event satisfies all conditions in the filter.
func (m *Matcher) Match(filter Filter, event Event) bool {
	if filter.Type != nil && *filter.Type != event.Type {
		return false
	}
	if filter.Source != nil && *filter.Source != event.Source {
		return false
	}

	for key, condition := range filter.Payload {
		eventValue, ok := event.Payload[key]
		if !ok {
			return false
		}
		if !matchCondition(condition, eventValue) {
			return false
		}
	}

	return true
}

func parsePayloadCondition(key string, value any) (Condition, error) {
	if obj, ok := value.(map[string]any); ok {
		if len(obj) != 1 {
			return Condition{}, fmt.Errorf("payload.%s: operator object must contain exactly one operator", key)
		}

		for opName, operand := range obj {
			operator := Operator(opName)
			switch operator {
			case OpEQ:
				return Condition{Operator: operator, Value: operand}, nil
			case OpGT, OpGTE, OpLT, OpLTE:
				if _, ok := normalizeNumber(operand); !ok {
					return Condition{}, fmt.Errorf("payload.%s.%s: operand must be numeric", key, opName)
				}
				return Condition{Operator: operator, Value: operand}, nil
			default:
				return Condition{}, fmt.Errorf("payload.%s: unsupported operator %q", key, opName)
			}
		}
	}

	return Condition{
		Operator: OpEQ,
		Value:    value,
	}, nil
}

func matchCondition(condition Condition, eventValue any) bool {
	switch condition.Operator {
	case OpEQ:
		return valuesEqual(condition.Value, eventValue)
	case OpGT, OpGTE, OpLT, OpLTE:
		left, leftOK := normalizeNumber(eventValue)
		right, rightOK := normalizeNumber(condition.Value)
		if !leftOK || !rightOK {
			return false
		}
		switch condition.Operator {
		case OpGT:
			return left > right
		case OpGTE:
			return left >= right
		case OpLT:
			return left < right
		case OpLTE:
			return left <= right
		}
	}
	return false
}

func valuesEqual(a, b any) bool {
	an, aok := normalizeNumber(a)
	bn, bok := normalizeNumber(b)
	if aok && bok {
		return an == bn
	}
	return reflect.DeepEqual(a, b)
}

func normalizeNumber(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case int32:
		return float64(n), true
	case int16:
		return float64(n), true
	case int8:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint64:
		if n > math.MaxInt64 {
			return 0, false
		}
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint8:
		return float64(n), true
	default:
		return 0, false
	}
}
