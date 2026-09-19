package main

import (
	"context"
	"encoding/json"
	"path/filepath"
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
