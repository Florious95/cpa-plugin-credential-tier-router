package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestActivePoolDefaultsAndZeroDisablesCap(t *testing.T) {
	if got := defaultSettings().ActivePoolSize; got != 4 {
		t.Fatalf("default active pool size=%d, want 4", got)
	}
	cfg, err := parsePluginConfig([]byte(`{"active_pool_size":0}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ActivePoolSize != 0 {
		t.Fatalf("explicit zero active pool size=%d, want unlimited", cfg.ActivePoolSize)
	}
	cfg, err = parsePluginConfig([]byte(`{"active_pool_size":4}`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ActivePoolSize != 4 {
		t.Fatalf("parsed active pool size=%d, want 4", cfg.ActivePoolSize)
	}
}

func TestApplyActivePoolCapPromotesTopHealthyCandidatesPerProvider(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	cfg := defaultSettings()
	cfg.ActivePoolSize = 4
	credentials := make([]credentialState, 0, 7)
	for i, remaining := range []int{10, 90, 80, 70, 60, 50, 40} {
		index := string(rune('a' + i))
		credentials = append(credentials, credentialState{
			Provider: "antigravity", AuthIndex: index, ProposedTier: tierRegular,
			Quota: readyQuota(remaining, ptrTime(now.Add(time.Hour)), now),
		})
	}
	credentials = append(credentials, credentialState{
		Provider: "codex", AuthIndex: "codex-a", ProposedTier: tierRegular,
		Quota: readyQuota(100, ptrTime(now.Add(time.Hour)), now),
	})

	applyActivePoolCap(&cfg, credentials, now)
	for _, credential := range credentials {
		if credential.Provider == "codex" {
			if credential.ProposedTier != tierPrimary {
				t.Fatalf("other provider should have independent pool: %+v", credential)
			}
			continue
		}
		want := tierBackup
		if credential.AuthIndex == "b" || credential.AuthIndex == "c" || credential.AuthIndex == "d" || credential.AuthIndex == "e" {
			want = tierPrimary
		}
		if credential.ProposedTier != want {
			t.Fatalf("credential %s tier=%s, want %s", credential.AuthIndex, credential.ProposedTier, want)
		}
	}
}

func TestApplyActivePoolCapPreservesPausedAndUnavailable(t *testing.T) {
	now := time.Now().UTC()
	cfg := defaultSettings()
	cfg.ActivePoolSize = 2
	credentials := []credentialState{
		{Provider: "antigravity", AuthIndex: "paused", ProposedTier: tierPaused, Quota: readyQuota(100, nil, now)},
		{Provider: "antigravity", AuthIndex: "unavailable", ProposedTier: tierPrimary, Unavailable: true, Quota: readyQuota(100, nil, now)},
		{Provider: "antigravity", AuthIndex: "active-a", ProposedTier: tierBackup, Quota: readyQuota(80, nil, now)},
		{Provider: "antigravity", AuthIndex: "active-b", ProposedTier: tierBackup, Quota: readyQuota(70, nil, now)},
		{Provider: "antigravity", AuthIndex: "reserve", ProposedTier: tierBackup, Quota: readyQuota(60, nil, now)},
	}
	applyActivePoolCap(&cfg, credentials, now)
	if credentials[0].ProposedTier != tierPaused || credentials[1].ProposedTier != tierPrimary {
		t.Fatalf("paused/unavailable credentials were rewritten: %+v", credentials)
	}
	if credentials[4].ProposedTier != tierBackup {
		t.Fatalf("reserve credential=%s, want backup", credentials[4].ProposedTier)
	}
}

func TestApplyPlanHardDisablesPausedAndRestoresOnPromotion(t *testing.T) {
	host := &fakeHost{documents: map[string]authDocument{
		"a": {AuthIndex: "a", Name: "account.json", JSON: json.RawMessage(`{"priority":400,"disabled":false}`)},
	}}
	r := newRuntime(host)
	r.store.path = filepath.Join(t.TempDir(), "state.json")
	if err := r.applyPlan(context.Background(), []authFile{{AuthIndex: "a", Name: "account.json"}}, plan{Credentials: []credentialState{{AuthIndex: "a", ProposedTier: tierPaused, Changed: true}}}); err != nil {
		t.Fatal(err)
	}
	var saved map[string]any
	if err := json.Unmarshal(host.saved["account.json"], &saved); err != nil {
		t.Fatal(err)
	}
	if saved["disabled"] != true || int(saved["priority"].(float64)) != -1 {
		t.Fatalf("paused state=%v, want disabled=true priority=-1", saved)
	}
	host.documents["a"] = authDocument{AuthIndex: "a", Name: "account.json", JSON: host.saved["account.json"]}
	if err := r.applyPlan(context.Background(), []authFile{{AuthIndex: "a", Name: "account.json", Disabled: true}}, plan{Credentials: []credentialState{{AuthIndex: "a", ProposedTier: tierPrimary, Changed: true}}}); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(host.saved["account.json"], &saved); err != nil {
		t.Fatal(err)
	}
	if saved["disabled"] != false || int(saved["priority"].(float64)) != 400 {
		t.Fatalf("restored state=%v, want disabled=false priority=400", saved)
	}
}

func TestGeo400ThresholdRequiresDistinctAccountsAndSchedulesReturn(t *testing.T) {
	previous := runEgressCommand
	calls := 0
	runEgressCommand = func(_ context.Context, _ egressInvocation) error { calls++; return nil }
	t.Cleanup(func() { runEgressCommand = previous })
	host := &fakeHost{documents: map[string]authDocument{
		"a": {AuthIndex: "a", Name: "a.json", JSON: json.RawMessage(`{"priority":400,"disabled":false}`)},
		"b": {AuthIndex: "b", Name: "b.json", JSON: json.RawMessage(`{"priority":400,"disabled":false}`)},
		"c": {AuthIndex: "c", Name: "c.json", JSON: json.RawMessage(`{"priority":400,"disabled":false}`)},
	}}
	r := newRuntime(host)
	r.store.path = filepath.Join(t.TempDir(), "state.json")
	r.state.Settings = defaultSettings()
	r.state.Settings.Geo400EgressEnabled = true
	r.state.Settings.Geo400AccountThreshold = 2
	r.geoNow = func() time.Time { return time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC) }
	body := `{"error":{"status":"FAILED_PRECONDITION","message":"User location is not supported for the API use."}}`
	event := func(index string) []byte {
		raw, _ := json.Marshal(usageEvent{Provider: "antigravity", AuthIndex: index, Failed: true, Failure: usageFailure{StatusCode: 400, Body: body}})
		return raw
	}
	if err := r.handleUsage(context.Background(), event("a")); err != nil {
		t.Fatal(err)
	}
	r.egressWG.Wait()
	if calls != 0 {
		t.Fatalf("one account triggered egress calls=%d, want 0", calls)
	}
	if err := r.handleUsage(context.Background(), event("b")); err != nil {
		t.Fatal(err)
	}
	r.egressWG.Wait()
	if calls != 1 {
		t.Fatalf("two distinct accounts triggered egress calls=%d, want 1", calls)
	}
	wantReturn := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC).Add(12 * time.Hour)
	if r.state.EgressReturnAt == nil || !r.state.EgressReturnAt.Equal(wantReturn) {
		t.Fatalf("return deadline=%v, want %v", r.state.EgressReturnAt, wantReturn)
	}
	if err := r.handleUsage(context.Background(), event("b")); err != nil {
		t.Fatal(err)
	}
	if err := r.handleUsage(context.Background(), event("c")); err != nil {
		t.Fatal(err)
	}
	r.egressWG.Wait()
	if calls != 1 {
		t.Fatalf("accounts in one sliding window triggered egress calls=%d, want 1", calls)
	}
}
