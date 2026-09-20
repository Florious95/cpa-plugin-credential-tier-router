package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestRegistrationDeclaresUsagePlugin(t *testing.T) {
	capabilities, ok := registrationResult()["capabilities"].(map[string]bool)
	if !ok || !capabilities["usage_plugin"] {
		t.Fatalf("missing usage capability: %v", capabilities)
	}
}

func TestUsageGeo400MatchesAntigravityRegionBody(t *testing.T) {
	for _, body := range []string{
		`Error: 400: {"code":400,"message":"User location is not supported for the API use.","status":"FAILED_PRECONDITION"}`,
		`{"error":{"status":"FAILED_PRECONDITION","message":"User location is not supported for the API use."}}`,
		`{"error":{"status":"failed_precondition","message":"user location is not supported for the api use."}}`,
	} {
		if !isAntigravityGeo400(usageEvent{Provider: "antigravity", Failed: true, Failure: usageFailure{StatusCode: 400, Body: body}}) {
			t.Fatalf("did not match %s", body)
		}
	}
}

func TestUsageGeo400DowngradesCredential(t *testing.T) {
	host := &fakeHost{
		files:     []authFile{{AuthIndex: "idx-1", Name: "antigravity.json", Provider: "antigravity", Priority: 400}},
		documents: map[string]authDocument{"idx-1": {AuthIndex: "idx-1", Name: "antigravity.json", JSON: json.RawMessage(`{"email":"user@example.com","priority":400,"proxy_url":"socks5://old"}`)}},
	}
	r := newRuntime(host)
	r.store.path = filepath.Join(t.TempDir(), "state.json")
	r.state.Settings = defaultSettings()
	r.latest = plan{Credentials: []credentialState{{AuthIndex: "idx-1", CurrentTier: tierPrimary, ProposedTier: tierPrimary}}}
	now := time.Now().UTC()
	r.geoNow = func() time.Time { return now }
	raw, _ := json.Marshal(usageEvent{Provider: "antigravity", AuthIndex: "idx-1", Failed: true, Failure: usageFailure{StatusCode: 400, Body: `FAILED_PRECONDITION User location is not supported for the API use.`}})
	if err := r.handleUsage(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	var saved map[string]any
	if err := json.Unmarshal(host.saved["antigravity.json"], &saved); err != nil {
		t.Fatal(err)
	}
	if saved["priority"] != float64(-1) || saved["disabled"] != true {
		t.Fatalf("hard pause=%v", saved)
	}
	if saved["proxy_url"] != "socks5://old" {
		t.Fatal("unrelated network configuration changed")
	}
	q := r.state.Quota["idx-1"]
	if !q.ManagedRest || q.RestUntil == nil || !q.RestUntil.Equal(now.Add(2*time.Hour)) {
		t.Fatalf("rest=%+v", q)
	}
	if r.latest.Credentials[0].ProposedTier != tierPaused {
		t.Fatal("dashboard did not pause")
	}
}

func TestUsageGeo400IgnoresNonMatchingEvents(t *testing.T) {
	for i, event := range []usageEvent{
		{Provider: "codex", Failed: true, Failure: usageFailure{StatusCode: 400, Body: "FAILED_PRECONDITION User location is not supported for the API use."}},
		{Provider: "antigravity", Failed: false, Failure: usageFailure{StatusCode: 400, Body: "FAILED_PRECONDITION User location is not supported for the API use."}},
		{Provider: "antigravity", Failed: true, Failure: usageFailure{StatusCode: 429, Body: "FAILED_PRECONDITION User location is not supported for the API use."}},
		{Provider: "antigravity", Failed: true, Failure: usageFailure{StatusCode: 400, Body: "permission denied"}},
	} {
		if isAntigravityGeo400(event) {
			t.Fatalf("case %d unexpectedly matched", i)
		}
	}
}
