package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRegistrationDeclaresUsagePlugin(t *testing.T) {
	capabilities, ok := registrationResult()["capabilities"].(map[string]bool)
	if !ok || !capabilities["usage_plugin"] {
		t.Fatalf("registration capabilities=%v, usage_plugin must be enabled", registrationResult()["capabilities"])
	}
}

func TestUsageGeo400MatchesAntigravityRegionBody(t *testing.T) {
	for _, body := range []string{
		`Error: 400: {"code":400,"message":"User location is not supported for the API use.","status":"FAILED_PRECONDITION"}`,
		`{"error":{"status":"FAILED_PRECONDITION","message":"User location is not supported for the API use."}}`,
		`{"error":{"status":"failed_precondition","message":"user location is not supported for the api use."}}`,
	} {
		event := usageEvent{
			Provider: "antigravity",
			Failed:   true,
			Failure:  usageFailure{StatusCode: 400, Body: body},
		}
		if !isAntigravityGeo400(event) {
			t.Fatalf("event body did not match: %s", body)
		}
	}
}

func TestUsageGeo400DowngradesCredential(t *testing.T) {
	previous := runEgressCommand
	called := false
	runEgressCommand = func(context.Context, egressInvocation) error {
		called = true
		return nil
	}
	t.Cleanup(func() { runEgressCommand = previous })

	host := &fakeHost{documents: map[string]authDocument{
		"idx-1": {AuthIndex: "idx-1", Name: "antigravity.json", JSON: json.RawMessage(`{"email":"user@example.com","priority":400,"proxy_url":"socks5://old"}`)},
	}}
	r := newRuntime(host)
	r.store.path = filepath.Join(t.TempDir(), "state.json")
	r.state.Settings = defaultSettings()
	r.latest = plan{Credentials: []credentialState{{AuthIndex: "idx-1", CurrentTier: tierPrimary, ProposedTier: tierPrimary}}}
	r.geoNow = func() time.Time { return time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC) }
	body := `Error: 400: {"code":400,"message":"User location is not supported for the API use.","status":"FAILED_PRECONDITION"}`
	raw, err := json.Marshal(usageEvent{Provider: "antigravity", AuthIndex: "idx-1", Failed: true, Failure: usageFailure{StatusCode: 400, Body: body}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.handleUsage(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	r.egressWG.Wait()
	var saved map[string]any
	if err := json.Unmarshal(host.saved["antigravity.json"], &saved); err != nil {
		t.Fatal(err)
	}
	if got := int(saved["priority"].(float64)); got != -1 {
		t.Fatalf("saved priority=%d, want -1", got)
	}
	if saved["disabled"] != true {
		t.Fatalf("saved disabled=%v, want true", saved["disabled"])
	}
	if got := saved["proxy_url"]; got != "socks5://old" {
		t.Fatalf("proxy_url changed unexpectedly: %v", got)
	}
	quota := r.state.Quota["idx-1"]
	wantUntil := time.Date(2026, 9, 19, 14, 0, 0, 0, time.UTC)
	if quota.RestUntil == nil || !quota.RestUntil.Equal(wantUntil) {
		t.Fatalf("RestUntil=%v, want %v", quota.RestUntil, wantUntil)
	}
	if called {
		t.Fatal("single-account geo-400 must not switch the shared egress by default")
	}
	if got := r.latest.Credentials[0].ProposedTier; got != tierPaused {
		t.Fatalf("latest plan tier=%s, want paused", got)
	}
}

func TestUsageGeo400WritesFallbackAlertWhenCommandFails(t *testing.T) {
	t.Setenv("CREDENTIAL_TIER_ROUTER_GEO_ALERT_PATH", "")
	previous := runEgressCommand
	runEgressCommand = func(context.Context, egressInvocation) error { return errors.New("executable file not found") }
	t.Cleanup(func() { runEgressCommand = previous })

	host := &fakeHost{documents: map[string]authDocument{
		"idx-1": {AuthIndex: "idx-1", Name: "antigravity.json", JSON: json.RawMessage(`{"priority":400}`)},
	}}
	r := newRuntime(host)
	r.store.path = filepath.Join(t.TempDir(), "state", "state.json")
	r.state.Settings = defaultSettings()
	r.state.Settings.Geo400EgressEnabled = true
	r.state.Settings.Geo400AccountThreshold = 1
	body := `Error: 400: {"code":400,"message":"User location is not supported for the API use.","status":"FAILED_PRECONDITION"}`
	raw, err := json.Marshal(usageEvent{Provider: "antigravity", AuthIndex: "idx-1", Failed: true, Failure: usageFailure{StatusCode: 400, Body: body}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.handleUsage(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	r.egressWG.Wait()
	alert, err := os.ReadFile(filepath.Join(filepath.Dir(r.store.path), "geo-400-alert.json"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(alert), `"auth_index": "idx-1"`) || !strings.Contains(string(alert), "executable file not found") {
		t.Fatalf("unexpected fallback alert: %s", alert)
	}
}

func TestUsageGeo400IgnoresNonMatchingEvents(t *testing.T) {
	cases := []usageEvent{
		{Provider: "codex", Failed: true, Failure: usageFailure{StatusCode: 400, Body: "FAILED_PRECONDITION User location is not supported for the API use."}},
		{Provider: "antigravity", Failed: false, Failure: usageFailure{StatusCode: 400, Body: "FAILED_PRECONDITION User location is not supported for the API use."}},
		{Provider: "antigravity", Failed: true, Failure: usageFailure{StatusCode: 429, Body: "FAILED_PRECONDITION User location is not supported for the API use."}},
		{Provider: "antigravity", Failed: true, Failure: usageFailure{StatusCode: 400, Body: "permission denied"}},
	}
	for i, event := range cases {
		if isAntigravityGeo400(event) {
			t.Fatalf("case %d unexpectedly matched: %+v", i, event)
		}
	}
}

func TestUsageGeo400RunsConfiguredCommandAndDebounces(t *testing.T) {
	var mu sync.Mutex
	var calls []egressInvocation
	previous := runEgressCommand
	runEgressCommand = func(_ context.Context, invocation egressInvocation) error {
		mu.Lock()
		calls = append(calls, invocation)
		mu.Unlock()
		return nil
	}
	t.Cleanup(func() { runEgressCommand = previous })

	r := newRuntime(&fakeHost{})
	r.store.path = filepath.Join(t.TempDir(), "state.json")
	r.state.Settings = defaultSettings()
	r.state.Settings.EgressCommand = "/usr/local/bin/cpa-egress-cycle"
	r.state.Settings.EgressTarget = "to-test"
	r.state.Settings.Geo400DebounceMinutes = 5
	r.state.Settings.Geo400AccountThreshold = 1
	r.state.Settings.Geo400EgressEnabled = true
	r.geoNow = func() time.Time { return time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC) }
	body := `{"error":{"status":"FAILED_PRECONDITION","message":"User location is not supported for the API use."}}`
	raw, err := json.Marshal(usageEvent{
		Provider: "antigravity", AuthID: "auth-1", AuthIndex: "idx-1", Model: "gemini-3-pro",
		Failed: true, Failure: usageFailure{StatusCode: 400, Body: body},
	})
	if err != nil {
		t.Fatal(err)
	}
	if response := r.handle(context.Background(), "usage.handle", raw); !json.Valid(response) {
		t.Fatalf("usage.handle returned invalid response: %s", response)
	}
	if response := r.handle(context.Background(), "usage.handle", raw); !json.Valid(response) {
		t.Fatalf("duplicate usage.handle returned invalid response: %s", response)
	}
	// The command is deliberately asynchronous, but the test injection records
	// synchronously before returning from the goroutine's callback.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		count := len(calls)
		mu.Unlock()
		if count == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 1 {
		t.Fatalf("egress command calls=%d, want 1 after debounce: %+v", len(calls), calls)
	}
	if calls[0].Command != "/usr/local/bin/cpa-egress-cycle" || len(calls[0].Args) != 1 || calls[0].Args[0] != "to-test" {
		t.Fatalf("unexpected invocation: %+v", calls[0])
	}
}
