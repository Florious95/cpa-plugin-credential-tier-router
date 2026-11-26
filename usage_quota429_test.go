package main

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

const fixtureQuota429 = `{"error":{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"},{"@type":"type.googleapis.com/google.rpc.RetryInfo","retryDelay":"9792s"}]}}`

func quota429Event(t *testing.T) []byte {
	t.Helper()
	event, err := json.Marshal(usageEvent{Provider: "antigravity", AuthIndex: "fixture", Failed: true, Failure: usageFailure{StatusCode: 429, Body: fixtureQuota429}})
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestQuota429ManagedRest(t *testing.T) {
	r, h := newHTTP403HubRuntime(t)
	now := time.Now().UTC()
	r.geoNow = func() time.Time { return now }
	shortRest := now.Add(time.Hour)
	r.state.Quota["fixture"] = quotaSnapshot{ManagedRest: true, RestUntil: &shortRest}
	ctx := context.Background()
	if err := r.handleUsage(ctx, quota429Event(t)); err != nil {
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

func TestLateQuota429DoesNotAdoptExternallyDisabled(t *testing.T) {
	r, h := newHTTP403HubRuntime(t)
	r.state.PoolLocked429 = true
	r.state.Pool429Before = map[string]pool429Policy{"fixture": {Disabled: true, Priority: -1}}
	h.file.Disabled, h.file.Priority = true, -1
	original := string(h.document)
	if err := r.handleUsage(context.Background(), quota429Event(t)); err != nil {
		t.Fatal(err)
	}
	if !r.state.Pool429Before["fixture"].Disabled || r.state.Quota["fixture"].ManagedRest || string(h.document) != original {
		t.Fatal("late quota event gained auto-enable ownership of an externally disabled credential")
	}
}

func TestQuota429NullAuthDocumentDoesNotPanic(t *testing.T) {
	r, h := newHTTP403HubRuntime(t)
	h.document = json.RawMessage(`null`)
	if err := r.handleUsage(context.Background(), quota429Event(t)); err == nil {
		t.Fatal("expected a controlled projection error")
	}
	assertNoQuotaRestIntent(t, r)
	if string(h.document) != "null" || r.poolLocked429() {
		t.Fatal("malformed document was rewritten or quota event tripped the lock")
	}
}

func TestQuota429ValidatesLiveCredentialBeforeRest(t *testing.T) {
	for _, tc := range []struct {
		name, document, filename, identity string
		wantError                          bool
	}{
		{"manual disable", `{"type":"antigravity","disabled":true}`, "fixture.json", "fixture", false},
		{"manual disable with tier marker", `{"type":"antigravity","disabled":true,"credential_tier_router":{"managed":true,"tier":"paused","reason":"quota_exhausted"}}`, "fixture.json", "fixture", false},
		{"failed get", `{}`, "fixture.json", "not-fixture", true},
		{"unsafe filename", `{}`, "nested/fixture.json", "fixture", true},
		{"wrong provider", `{"type":"codex"}`, "fixture.json", "fixture", true},
		{"ambiguous disabled", `{"disabled":"true"}`, "fixture.json", "fixture", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, h := newHTTP403HubRuntime(t)
			h.document, h.file.Name, h.file.AuthIndex = json.RawMessage(tc.document), tc.filename, tc.identity
			if err := r.handleUsage(context.Background(), quota429Event(t)); (err != nil) != tc.wantError {
				t.Fatalf("unexpected error: %v", err)
			}
			assertNoQuotaRestIntent(t, r)
			if string(h.document) != tc.document {
				t.Fatal("credential document was overwritten")
			}
		})
	}
}

func assertNoQuotaRestIntent(t *testing.T, r *runtime) {
	t.Helper()
	s, err := r.store.load()
	if err != nil {
		t.Fatal(err)
	}
	if r.state.Quota["fixture"].ManagedRest || s.Quota["fixture"].ManagedRest || r.poolLocked429() {
		t.Fatal("invalid credential must not create rest ownership or lock the pool")
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
		{"duplicate reason", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RATE_LIMIT_EXCEEDED","reason":"QUOTA_EXHAUSTED"}]}`, false},
		{"case variant collision", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","Reason":"RATE_LIMIT_EXCEEDED","reason":"QUOTA_EXHAUSTED"}]}`, false},
		{"competing details", `{"details":[],"error":` + fixtureQuota429 + `}`, false},
		{"duplicate detail objects", `{"details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"},{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"QUOTA_EXHAUSTED"}]}`, false},
		{"duplicate error field", `{"error":null,` + fixtureQuota429[1:], false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if isGoogleQuotaExhausted(tc.body) != tc.want {
				t.Fatal("wrong classification")
			}
		})
	}
}
