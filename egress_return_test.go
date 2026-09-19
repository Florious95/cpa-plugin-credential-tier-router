package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestEgressReturnDefaultsAndValidation(t *testing.T) {
	cfg := defaultSettings()
	if cfg.EgressReturnTarget != "to-wrap" || cfg.Geo400ReturnHours != 12 || cfg.Geo400RestHours != 2 || cfg.Geo400AccountThreshold != 2 {
		t.Fatalf("defaults target=%q return=%d rest=%d threshold=%d, want to-wrap/12/2/2", cfg.EgressReturnTarget, cfg.Geo400ReturnHours, cfg.Geo400RestHours, cfg.Geo400AccountThreshold)
	}
	parsed, err := parsePluginConfig([]byte("egress_return_target: office\ngeo400_return_hours: 0\n"))
	if err != nil {
		t.Fatal(err)
	}
	if parsed.EgressReturnTarget != "office" || parsed.Geo400ReturnHours != 0 {
		t.Fatalf("parsed return settings=%+v", parsed)
	}
	if _, err := parsePluginConfig([]byte("geo400_return_hours: -1\n")); err == nil {
		t.Fatal("negative geo400_return_hours must be rejected")
	}
}

func TestStateStoreDefaultsReturnHoursForLegacyState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	store := stateStore{path: path}
	state := persistedState{Settings: defaultSettings(), Quota: map[string]quotaSnapshot{}}
	if err := store.save(state); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	delete(document["settings"].(map[string]any), "geo400_return_hours")
	raw, _ = json.Marshal(document)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Settings.Geo400ReturnHours != 12 {
		t.Fatalf("legacy return hours=%d, want 12", loaded.Settings.Geo400ReturnHours)
	}
}

func TestDueEgressReturnRunsConfiguredCommandAndClearsState(t *testing.T) {
	previous := runEgressCommand
	var got egressInvocation
	runEgressCommand = func(_ context.Context, invocation egressInvocation) error {
		got = invocation
		return nil
	}
	t.Cleanup(func() { runEgressCommand = previous })

	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	r := newRuntime(&fakeHost{})
	r.store.path = filepath.Join(t.TempDir(), "state.json")
	r.state.Settings = defaultSettings()
	r.state.Settings.EgressCommand = "/bin/egress"
	r.state.Settings.EgressReturnTarget = "to-wrap"
	r.state.EgressReturnAt = ptrTime(now.Add(time.Minute))
	if err := r.maybeReturnEgress(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if got.Command != "" || r.state.EgressReturnAt == nil {
		t.Fatalf("future return triggered: command=%+v state=%v", got, r.state.EgressReturnAt)
	}
	r.state.EgressReturnAt = ptrTime(now.Add(-time.Minute))
	if err := r.maybeReturnEgress(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if got.Command != "/bin/egress" || len(got.Args) != 1 || got.Args[0] != "to-wrap" {
		t.Fatalf("unexpected return command: %+v", got)
	}
	if r.state.EgressReturnAt != nil {
		t.Fatalf("EgressReturnAt not cleared: %v", r.state.EgressReturnAt)
	}
}

func TestManualEgressReturnEndpoint(t *testing.T) {
	previous := runEgressCommand
	runEgressCommand = func(_ context.Context, _ egressInvocation) error { return nil }
	t.Cleanup(func() { runEgressCommand = previous })

	r := newRuntime(&fakeHost{})
	r.store.path = filepath.Join(t.TempDir(), "state.json")
	r.state.Settings = defaultSettings()
	r.state.EgressReturnAt = ptrTime(time.Now().UTC().Add(time.Hour))
	request, _ := json.Marshal(map[string]string{"Method": "POST", "Path": "/plugins/credential-tier-router/egress/return"})
	response, err := r.handleManagement(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || r.state.EgressReturnAt != nil {
		t.Fatalf("manual return response=%+v state=%v", response, r.state.EgressReturnAt)
	}
}
