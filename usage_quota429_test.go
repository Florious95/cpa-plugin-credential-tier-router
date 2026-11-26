package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestQuota429ManagedRest(t *testing.T) {
	r, h := newHTTP403HubRuntime(t)
	now := time.Now().UTC()
	r.geoNow = func() time.Time { return now }
	event, err := json.Marshal(usageEvent{Provider: "antigravity", AuthIndex: "fixture", Failed: true, Failure: usageFailure{
		StatusCode: 429, Body: `{"error":{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"9792s"}]}}`,
	}})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := r.handleUsage(ctx, event); err != nil {
		t.Fatal(err)
	}
	state, err := r.store.load()
	if err != nil {
		t.Fatal(err)
	}
	q := state.Quota["fixture"]
	if r.poolLocked429() || state.PoolLocked429 || state.Pool429Generation != 0 || !q.ManagedRest || q.RestUntil == nil || !q.RestUntil.Equal(now.Add(16*time.Hour)) || !h.file.Disabled || h.file.Priority != -1 {
		t.Fatalf("expected persisted single-credential rest without a pool lock; quota=%+v disabled=%v priority=%d", q, h.file.Disabled, h.file.Priority)
	}
	if err := r.scheduledInspection(ctx, false); err != nil {
		t.Fatal(err)
	}
	if h.file.Disabled || h.file.Priority != -1 || len(h.requests) != 0 {
		t.Fatal("next inspection should retain -1 while releasing hard disable without quota probes")
	}
}

func TestQuota429ClassifierUsesTypedDetails(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       bool
	}{
		{"native status", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"}]}`, true},
		{"message only", `{"message":"QUOTA_EXHAUSTED"}`, false},
		{"missing type", `{"details":[{"reason":"QUOTA_EXHAUSTED"}]}`, false},
		{"other detail type", `{"details":[{"@type":"type.googleapis.com/google.rpc.RetryInfo","reason":"QUOTA_EXHAUSTED"}]}`, false},
		{"other reason", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RATE_LIMIT_EXCEEDED"}]}`, false},
		{"incomplete", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if isGoogleQuotaExhausted(tc.body) != tc.want {
				t.Fatal("wrong classification")
			}
		})
	}
}
