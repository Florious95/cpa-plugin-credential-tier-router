package main

// Implementation-side tests from the lifecycle contract, independent of the
// tester's blind acceptance suite. Auth projections use real temporary files.
import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func assertManagedProjection(t *testing.T, h *reviewDiskHost, id string, disabled bool, priority int) {
	t.Helper()
	f := reviewAuth(t, h, id)
	if f.Disabled != disabled || f.Priority != priority {
		t.Fatalf("%s: disabled=%v priority=%d, want %v/%d", id, f.Disabled, f.Priority, disabled, priority)
	}
}

func TestManagedRestFourPhasesAndNoIncumbentChurn(t *testing.T) {
	for _, status := range []quotaStatus{quotaReady, quotaCached, quotaRetry, quotaUnknown} {
		t.Run(string(status), func(t *testing.T) {
			r, h := reviewRuntime(t, 8)
			reviewRun(t, r)
			if err := r.handleUsage(context.Background(), reviewGeo("a")); err != nil {
				t.Fatal(err)
			}
			assertManagedProjection(t, h, "a", true, -1)
			stickyMembers(t, r, "b", "c", "d", "e")
			until := *r.state.Quota["a"].RestUntil
			q := r.state.Quota["a"]
			q.Status = status
			if status == quotaRetry || status == quotaUnknown {
				q.Remaining = nil
			}
			r.state.Quota["a"] = q
			for round := 0; round < 8; round++ {
				if err := r.scheduledInspection(context.Background(), false); err != nil {
					t.Fatal(err)
				}
				assertManagedProjection(t, h, "a", false, -1)
				stickyProjection(t, h, "b", "c", "d", "e")
				if q := r.state.Quota["a"]; !q.ManagedRest || q.RestUntil == nil || !q.RestUntil.Equal(until) {
					t.Fatalf("rest extended/lost: %+v", q)
				}
				view, err := r.dashboard(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				for _, c := range view.Plan.Credentials {
					if c.AuthIndex == "a" && (!strings.Contains(c.Reason, "强制休眠至") || strings.Contains(c.Reason, "外部停用")) {
						t.Fatalf("rest mislabeled: %s", c.Reason)
					}
				}
			}
			reviewExpire(r, "a")
			if err := r.persistState(); err != nil {
				t.Fatal(err)
			}
			restarted := newRuntime(h)
			if err := restarted.scheduledInspection(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			assertManagedProjection(t, h, "a", false, 200)
			stickyMembers(t, restarted, "b", "c", "d", "e")
			if q := restarted.state.Quota["a"]; q.ManagedRest || q.RestUntil != nil {
				t.Fatalf("expired ownership retained: %+v", q)
			}
			reviewRun(t, restarted)
			stickyProjection(t, h, "b", "c", "d", "e")
		})
	}
}

func TestManagedRestExactExpiryAndStaleZeroCannotRearm(t *testing.T) {
	deadline := time.Now().UTC()
	for _, disabled := range []bool{false, true} {
		for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
			for _, remaining := range []*int{nil, ptrInt(0), ptrInt(75)} {
				cfg := defaultSettings()
				cache := map[string]quotaSnapshot{"a": {Status: quotaReady, Remaining: remaining, RestUntil: &deadline, ManagedRest: true, ObservedAt: deadline.Add(-time.Hour)}}
				result, _ := planPool(cfg, []authFile{{AuthIndex: "a", Provider: "antigravity", Name: "a.json", Priority: -1, Disabled: disabled}}, cache, map[string]activePoolState{"antigravity": {}}, deadline.Add(offset))
				c := result.Credentials[0]
				if c.ProposedDisabled {
					t.Fatalf("managed rest still disabled: %+v", c)
				}
				if offset < 0 {
					if c.ProposedTier != tierPaused {
						t.Fatal("early release")
					}
				} else {
					if c.ProposedTier == tierPaused || !c.Changed {
						t.Fatalf("expiry not projected: %+v", c)
					}
					if !cache["a"].ManagedRest || cache["a"].RestUntil == nil || !cache["a"].RestUntil.Equal(deadline) {
						t.Fatal("ownership must remain durable until projection succeeds")
					}
				}
			}
		}
	}
	// A new zero observed after expiry is real exhaustion, unlike stale cache.
	cache := map[string]quotaSnapshot{"a": {Status: quotaReady, Remaining: ptrInt(0), RestUntil: &deadline, ManagedRest: true, ObservedAt: deadline.Add(time.Second)}}
	result, _ := planPool(defaultSettings(), []authFile{{AuthIndex: "a", Provider: "antigravity", Name: "a.json", Priority: -1}}, cache, nil, deadline.Add(time.Second))
	if result.Credentials[0].ProposedTier != tierPaused || !cache["a"].RestUntil.After(deadline) {
		t.Fatal("fresh exhaustion was incorrectly released")
	}
}

func TestManagedRestRecoveryIsScopedEvenWithAutomationOff(t *testing.T) {
	r, h := reviewRuntime(t, 6)
	r.state.Settings.AutoApply = false
	// No bootstrap has occurred. Only a is owned; b is externally disabled.
	stickySetAuth(t, h, "a", map[string]any{"disabled": true, "priority": -1})
	stickySetAuth(t, h, "b", map[string]any{"disabled": true, "priority": -1})
	until := time.Now().Add(time.Hour)
	r.state.Quota["a"] = quotaSnapshot{RestUntil: &until, ManagedRest: true, Status: quotaUnknown}
	r.state.Quota["b"] = quotaSnapshot{RestUntil: &until, ManagedRest: false, Status: quotaUnknown}
	// Recovery ownership is serviced even after the provider is deselected.
	r.state.Settings.Providers = []string{"codex"}
	var originals = map[string]string{}
	for _, id := range []string{"b", "c", "d", "e", "f"} {
		d, _ := h.getAuth(context.Background(), id)
		originals[id] = string(d.JSON)
	}
	for _, expired := range []bool{false, true} {
		if expired {
			reviewExpire(r, "a")
		}
		if err := r.scheduledInspection(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		priority := -1
		if expired {
			priority = 200
		}
		assertManagedProjection(t, h, "a", false, priority)
		for id, original := range originals {
			d, _ := h.getAuth(context.Background(), id)
			if string(d.JSON) != original {
				t.Fatalf("recovery touched unrelated %s", id)
			}
		}
		if len(r.state.ActivePool) != 0 {
			t.Fatal("scoped recovery bootstrapped membership")
		}
	}
}

func TestManagedRestPreviewCannotGrantRecoveryWritePermission(t *testing.T) {
	r, h := reviewRuntime(t, 1)
	r.state.Settings.AutoApply = false
	h.quota("a", 0)
	if _, err := r.run(context.Background(), false, "preview"); err != nil {
		t.Fatal(err)
	}
	if r.state.Quota["a"].ManagedRest {
		t.Fatal("preview granted a durable write permission")
	}
	if err := r.scheduledInspection(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	assertManagedProjection(t, h, "a", false, 400)
}

func TestManagedRestWriteFailuresReplayBothPhasesAfterRestart(t *testing.T) {
	for _, expiry := range []bool{false, true} {
		t.Run(map[bool]string{false: "clear_disable", true: "restore_priority"}[expiry], func(t *testing.T) {
			r, h := reviewRuntime(t, 5)
			reviewRun(t, r)
			if err := r.handleUsage(context.Background(), reviewGeo("a")); err != nil {
				t.Fatal(err)
			}
			if expiry {
				if err := r.scheduledInspection(context.Background(), false); err != nil {
					t.Fatal(err)
				}
				reviewExpire(r, "a")
			}
			until := *r.state.Quota["a"].RestUntil
			r.host = &stickyFaultHost{reviewDiskHost: h, failSave: "a.json"}
			if err := r.scheduledInspection(context.Background(), false); err == nil {
				t.Fatal("expected injected write error")
			}
			restarted := newRuntime(h)
			q := restarted.state.Quota["a"]
			if !q.ManagedRest || q.RestUntil == nil || !q.RestUntil.Equal(until) {
				t.Fatalf("lost durable recovery: %+v", q)
			}
			if err := restarted.scheduledInspection(context.Background(), false); err != nil {
				t.Fatal(err)
			}
			priority := -1
			if expiry {
				priority = 200
			}
			assertManagedProjection(t, h, "a", false, priority)
			stickyMembers(t, restarted, "b", "c", "d", "e")
		})
	}
}

func TestManagedRestRecoveryPrecedesBlockedProbe(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	reviewRun(t, r)
	if err := r.handleUsage(context.Background(), reviewGeo("a")); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	h.hook = func(req hostHTTPRequest) {
		if req.AuthIndex == "a" {
			close(entered)
			<-release
		}
	}
	done := make(chan error, 1)
	go func() { done <- r.scheduledInspection(context.Background(), true) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("inspection did not start probe")
	}
	f := reviewAuth(t, h, "a")
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if f.Disabled || f.Priority != -1 {
		t.Fatalf("slow upstream blocked recovery: %+v", f)
	}
}

func TestManagedRestDuplicateGeoDoesNotExtendDeadline(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	reviewRun(t, r)
	now := time.Now().UTC()
	r.geoNow = func() time.Time { return now }
	for round := 0; round < 5; round++ {
		if err := r.handleUsage(context.Background(), reviewGeo("a")); err != nil {
			t.Fatal(err)
		}
		assertManagedProjection(t, h, "a", true, -1)
		if err := r.scheduledInspection(context.Background(), false); err != nil {
			t.Fatal(err)
		}
		assertManagedProjection(t, h, "a", false, -1)
		if !r.state.Quota["a"].RestUntil.Equal(now.Add(2*time.Hour - time.Duration(round)*time.Minute)) {
			t.Fatal("duplicate extended rest")
		}
		now = now.Add(time.Minute)
	}
	stickyMembers(t, r, "b", "c", "d", "e")
}

func TestManagedRestHostUnavailableStillClearsOneShotDisable(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	reviewRun(t, r)
	if err := r.handleUsage(context.Background(), reviewGeo("a")); err != nil {
		t.Fatal(err)
	}
	// Runtime availability can lag the breaker. The auth is still readable;
	// clearing disabled does not make it eligible because priority stays -1.
	stickySetAuth(t, h, "a", map[string]any{"unavailable": true})
	if err := r.scheduledInspection(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	assertManagedProjection(t, h, "a", false, -1)
	stickyMembers(t, r, "b", "c", "d", "e")
}

func TestManagedRestStateFailureDoesNotClearDisable(t *testing.T) {
	r, h := reviewRuntime(t, 1)
	if err := r.handleUsage(context.Background(), reviewGeo("a")); err != nil {
		t.Fatal(err)
	}
	r.store.path = t.TempDir()
	if err := r.scheduledInspection(context.Background(), false); err == nil {
		t.Fatal("expected state save failure")
	}
	assertManagedProjection(t, h, "a", true, -1)
}

func TestTiersOnlyLegacySettingsAndStateDropNetworkKeys(t *testing.T) {
	const legacy = `{"egress_command":"/must/not/execute","egress_target":"x","egress_return_target":"y","geo400_egress_enabled":true,"geo400_account_threshold":1,"geo400_debounce_minutes":5,"geo400_return_hours":12,"geo400_rest_hours":3}`
	cfg, err := parsePluginConfig([]byte(legacy))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Geo400RestHours != 3 {
		t.Fatal("rest config not retained")
	}
	for _, v := range []any{cfg, managementRegistration(), dashboardState{Settings: cfg}} {
		raw, _ := json.Marshal(v)
		if strings.Contains(strings.ToLower(string(raw)), "egress") || strings.Contains(string(raw), "geo400_account_threshold") {
			t.Fatalf("legacy network API leaked: %s", raw)
		}
	}
	r, _ := reviewRuntime(t, 0)
	raw := []byte(`{"settings":` + legacy + `,"egress_return_at":"2030-01-01T00:00:00Z","active_pool":{"antigravity":{"members":["a"],"generation":7}}}`)
	if err := os.WriteFile(r.store.path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := r.store.load()
	if err != nil {
		t.Fatal(err)
	}
	if err := r.store.save(loaded); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(r.store.path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(saved)), "egress") || !reflect.DeepEqual(loaded.ActivePool["antigravity"].Members, []string{"a"}) {
		t.Fatalf("migration changed wrong fields: %s", saved)
	}
	request, _ := json.Marshal(map[string]any{"Method": "POST", "Path": "/plugins/credential-tier-router/egress/return"})
	response, err := r.handleManagement(context.Background(), request)
	if err != nil || response.StatusCode != 404 {
		t.Fatalf("removed route still exposed: status=%d err=%v", response.StatusCode, err)
	}
	// Production sources/assets must not contain switch execution or route hooks.
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, "web/app.js", "web/index.html")
	for _, file := range files {
		if strings.HasSuffix(file, "_test.go") {
			continue
		}
		body, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{"os/exec", "Egress", "egress/return", "cpa-egress-cycle", "Geo400AccountThreshold", "Geo400ReturnHours"} {
			if strings.Contains(string(body), forbidden) {
				t.Fatalf("production leftover %s in %s", forbidden, file)
			}
		}
	}
}
