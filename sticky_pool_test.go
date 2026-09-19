package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"
)

func stickyMembers(t *testing.T, r *runtime, want ...string) {
	t.Helper()
	r.mu.Lock()
	got := slices.Clone(r.state.ActivePool["antigravity"].Members)
	r.mu.Unlock()
	if !slices.Equal(got, want) {
		t.Fatalf("members=%v want=%v", got, want)
	}
}

func stickyProjection(t *testing.T, h *reviewDiskHost, want ...string) {
	t.Helper()
	files, err := h.listAuth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range files {
		if f.Priority == 400 && !f.Disabled && !f.Unavailable {
			got = append(got, f.AuthIndex)
		}
	}
	sorted := slices.Clone(want)
	slices.Sort(sorted)
	if !slices.Equal(got, sorted) {
		t.Fatalf("enabled 400=%v want=%v", got, sorted)
	}
}

func stickySetAuth(t *testing.T, h *reviewDiskHost, id string, fields map[string]any) {
	t.Helper()
	doc, err := h.getAuth(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	var root map[string]any
	if err := json.Unmarshal(doc.JSON, &root); err != nil {
		t.Fatal(err)
	}
	for k, v := range fields {
		root[k] = v
	}
	raw, err := json.Marshal(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.saveAuth(context.Background(), doc.Name, raw); err != nil {
		t.Fatal(err)
	}
}

func TestStickyWorkersNeverCompeteWithFullReserves(t *testing.T) {
	for _, strategy := range []strategyName{strategyQuota, strategyReset, strategyRotate, strategyManual} {
		t.Run(string(strategy), func(t *testing.T) {
			r, h := reviewRuntime(t, 8)
			r.state.Settings.Strategy = strategy
			before := make(map[string]string)
			for _, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
				doc, _ := h.getAuth(context.Background(), id)
				before[id] = string(doc.JSON)
			}
			for cycle := 0; cycle < 10; cycle++ {
				for i, id := range []string{"a", "b", "c", "d"} {
					h.quota(id, max(1, 80-i*10-cycle*9))
				}
				for i, id := range []string{"e", "f", "g", "h"} {
					h.quota(id, 100-i)
				}
				result, err := r.run(context.Background(), true, "sticky production regression")
				if err != nil {
					t.Fatal(err)
				}
				if result.Changes != 0 {
					t.Fatalf("cycle %d rewrote %d projections", cycle, result.Changes)
				}
				stickyMembers(t, r, "a", "b", "c", "d")
				stickyProjection(t, h, "a", "b", "c", "d")
				if g := r.state.ActivePool["antigravity"].Generation; g != 1 {
					t.Fatalf("unchanged membership generation=%d", g)
				}
			}
			for id, original := range before {
				doc, _ := h.getAuth(context.Background(), id)
				if string(doc.JSON) != original {
					t.Fatalf("healthy auth %s was unnecessarily rewritten", id)
				}
			}
		})
	}
}

func TestStickyTransientRetainedUntilExactFailureThreshold(t *testing.T) {
	for _, cached := range []bool{false, true} {
		for threshold := 2; threshold <= 10; threshold++ {
			t.Run(fmt.Sprintf("cached=%v/threshold=%d", cached, threshold), func(t *testing.T) {
				r, h := reviewRuntime(t, 5)
				r.state.Settings.FailureThreshold = threshold
				if cached {
					reviewRun(t, r)
				}
				h.mu.Lock()
				delete(h.responses, "a")
				h.mu.Unlock()
				for failures := 1; failures <= threshold; failures++ {
					reviewRun(t, r)
					if failures < threshold {
						stickyMembers(t, r, "a", "b", "c", "d")
						stickyProjection(t, h, "a", "b", "c", "d")
					} else {
						stickyMembers(t, r, "b", "c", "d", "e")
						stickyProjection(t, h, "b", "c", "d", "e")
					}
					if q := r.state.Quota["a"]; q.FailCount != failures {
						t.Fatalf("quota=%+v want failures=%d", q, failures)
					}
				}
				h.quota("a", 100)
				reviewRun(t, r)
				stickyMembers(t, r, "b", "c", "d", "e")
			})
		}
	}
}

func TestStickyZeroReleasesExactlyOneSeat(t *testing.T) {
	r, h := reviewRuntime(t, 8)
	reviewRun(t, r)
	h.quota("a", 0)
	reviewRun(t, r)
	stickyMembers(t, r, "b", "c", "d", "e")
	stickyProjection(t, h, "b", "c", "d", "e")
	if f := reviewAuth(t, h, "a"); !f.Disabled || f.Priority != -1 {
		t.Fatalf("zero quota not hard retired: %+v", f)
	}
	h.quota("a", 100)
	reviewExpire(r, "a")
	reviewRun(t, r)
	stickyMembers(t, r, "b", "c", "d", "e")
	if f := reviewAuth(t, h, "a"); f.Disabled || f.Priority != 200 {
		t.Fatalf("recovered old member must wait as reserve: %+v", f)
	}
}

func TestStickyHealthAndInitializationBoundaries(t *testing.T) {
	now := time.Now().UTC()
	cfg := defaultSettings()
	cfg.Providers = []string{"antigravity"}
	cfg.ActivePoolSize = 1
	remaining := 1
	base := credentialState{Provider: "antigravity", AuthIndex: "a", CurrentTier: tierPrimary, ProposedTier: tierBackup, Quota: quotaSnapshot{Remaining: &remaining, Status: quotaReady}}
	for _, tc := range []struct {
		name   string
		change func(*credentialState)
		want   poolHealth
	}{
		{"positive", func(*credentialState) {}, poolAlive},
		{"retry", func(c *credentialState) { c.Quota.Status, c.Quota.Remaining = quotaRetry, nil }, poolTransient},
		{"unknown", func(c *credentialState) { c.Quota.Status = quotaUnknown }, poolTransient},
		{"no snapshot", func(c *credentialState) { c.Quota = quotaSnapshot{} }, poolTransient},
		{"zero", func(c *credentialState) { v := 0; c.Quota.Remaining = &v }, poolDead},
		{"negative", func(c *credentialState) { v := -1; c.Quota.Remaining = &v }, poolDead},
		{"rest future", func(c *credentialState) { v := now.Add(time.Nanosecond); c.Quota.RestUntil = &v }, poolDead},
		{"rest exact", func(c *credentialState) { c.Quota.RestUntil = &now }, poolAlive},
		{"rest past", func(c *credentialState) { v := now.Add(-time.Nanosecond); c.Quota.RestUntil = &v }, poolAlive},
		{"disabled", func(c *credentialState) { c.Disabled = true }, poolDead},
		{"unavailable", func(c *credentialState) { c.Unavailable = true }, poolDead},
		{"manual pause", func(c *credentialState) { c.ProposedTier = tierPaused }, poolDead},
		{"threshold", func(c *credentialState) { c.Quota.FailCount = cfg.FailureThreshold }, poolDead},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := base
			tc.change(&c)
			if got := classifyPoolHealth(cfg, c, now); got != tc.want {
				t.Fatalf("health=%v want=%v", got, tc.want)
			}
		})
	}
	unknown := base
	unknown.Quota = quotaSnapshot{Status: quotaRetry}
	inherited := selectActivePool(cfg, []credentialState{unknown}, nil, now)
	if !slices.Equal(inherited["antigravity"].Members, []string{"a"}) {
		t.Fatal("bootstrap dropped unprobed current 400")
	}
	empty := map[string]activePoolState{"antigravity": {Generation: 9}}
	next := selectActivePool(cfg, []credentialState{unknown}, empty, now)
	if len(next["antigravity"].Members) != 0 || next["antigravity"].Generation != 9 {
		t.Fatal("initialized empty pool incorrectly bootstrapped stale 400")
	}
	// Deduplicate persisted input, retain original order, isolate slices, and do
	// not truncate oversized transient incumbents just because cap=1.
	b := unknown
	b.AuthIndex = "b"
	previous := map[string]activePoolState{"antigravity": {Members: []string{"b", "a", "b", "deleted"}, Generation: 4}, "codex": {Members: []string{"other"}, Generation: 2}}
	next = selectActivePool(cfg, []credentialState{unknown, b}, previous, now)
	if !slices.Equal(next["antigravity"].Members, []string{"b", "a"}) || next["antigravity"].Generation != 5 {
		t.Fatalf("unexpected transition %+v", next)
	}
	next["codex"].Members[0] = "changed"
	if previous["codex"].Members[0] != "other" {
		t.Fatal("cloned state aliases original members")
	}
}

func TestStickyPositiveShrinkDrainsAndExpansionOnlyFills(t *testing.T) {
	r, h := reviewRuntime(t, 8)
	reviewRun(t, r)
	cfg := r.state.Settings
	cfg.ActivePoolSize = 2
	if err := r.updateSettings(cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.shutdown)
	stickyMembers(t, r, "a", "b", "c", "d")
	for i, id := range []string{"a", "b", "c"} {
		h.quota(id, 0)
		reviewRun(t, r)
		wants := [][]string{{"b", "c", "d"}, {"c", "d"}, {"d", "e"}}
		stickyMembers(t, r, wants[i]...)
		stickyProjection(t, h, wants[i]...)
	}
	cfg.ActivePoolSize = 4
	if err := r.updateSettings(cfg); err != nil {
		t.Fatal(err)
	}
	stickyMembers(t, r, "d", "e", "f", "g")
	stickyProjection(t, h, "d", "e", "f", "g")
}

func TestStickyRestartAndHotReloadRepairProjectionWithoutProbing(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	reviewRun(t, r)
	stickySetAuth(t, h, "a", map[string]any{"priority": 200})
	h.hook = func(hostHTTPRequest) { t.Error("restart replay must not probe upstream") }
	restarted := newRuntime(h)
	if err := restarted.configure(defaultSettings()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restarted.shutdown)
	stickyMembers(t, restarted, "a", "b", "c", "d")
	stickyProjection(t, h, "a", "b", "c", "d")
	stickySetAuth(t, h, "b", map[string]any{"priority": 200})
	if err := restarted.configure(defaultSettings()); err != nil {
		t.Fatal(err)
	}
	stickyProjection(t, h, "a", "b", "c", "d")
}

func TestStickyDeletionAndExternalDisableFillVacancy(t *testing.T) {
	for _, cause := range []string{"delete", "disabled", "unavailable"} {
		t.Run(cause, func(t *testing.T) {
			r, h := reviewRuntime(t, 5)
			reviewRun(t, r)
			if cause == "delete" {
				if err := os.Remove(filepath.Join(h.dir, "a.json")); err != nil {
					t.Fatal(err)
				}
			} else {
				stickySetAuth(t, h, "a", map[string]any{cause: true})
			}
			if err := r.reconcileActivePool(context.Background(), "inventory change"); err != nil {
				t.Fatal(err)
			}
			stickyMembers(t, r, "b", "c", "d", "e")
			stickyProjection(t, h, "b", "c", "d", "e")
		})
	}
}

func TestStickyPreviewNeverAcquiresOrReleasesLeases(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	if _, err := r.run(context.Background(), false, "preview"); err != nil {
		t.Fatal(err)
	}
	if len(r.state.ActivePool) != 0 {
		t.Fatal("preview bootstrapped leases")
	}
	reviewRun(t, r)
	before := cloneActivePool(r.state.ActivePool)
	h.quota("a", 0)
	if _, err := r.run(context.Background(), false, "preview zero"); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, r.state.ActivePool) {
		t.Fatal("preview changed membership")
	}
	stickyProjection(t, h, "a", "b", "c", "d")
	reviewRun(t, r)
	stickyMembers(t, r, "b", "c", "d", "e")
}

// Injection is at the real host boundary; all successful writes hit actual
// temporary auth JSON files, and restarts decode the actual persisted state.
type stickyFaultHost struct {
	*reviewDiskHost
	failSave string
	listErr  error
}

func (h *stickyFaultHost) saveAuth(ctx context.Context, name string, raw json.RawMessage) error {
	if name == h.failSave {
		return errors.New("injected projection write failure")
	}
	return h.reviewDiskHost.saveAuth(ctx, name, raw)
}
func (h *stickyFaultHost) listAuth(ctx context.Context) ([]authFile, error) {
	if h.listErr != nil {
		return nil, h.listErr
	}
	return h.reviewDiskHost.listAuth(ctx)
}

func TestStickyIntentDurabilityAndPartialProjectionRecovery(t *testing.T) {
	for _, failed := range []string{"a.json", "e.json"} {
		t.Run(failed, func(t *testing.T) {
			r, h := reviewRuntime(t, 5)
			// Reserve a sorts before outgoing e: catches promote-before-retire.
			stickySetAuth(t, h, "a", map[string]any{"priority": 200})
			stickySetAuth(t, h, "e", map[string]any{"priority": 400})
			reviewRun(t, r)
			r.host = &stickyFaultHost{reviewDiskHost: h, failSave: failed}
			h.quota("e", 0)
			if _, err := r.run(context.Background(), true, "partial write"); err == nil {
				t.Fatal("expected injected write error")
			}
			stickyMembers(t, r, "b", "c", "d", "a")
			if reviewAuth(t, h, "a").Priority != 200 {
				t.Fatal("admitted replacement despite failed projection")
			}
			restarted := newRuntime(h)
			stickyMembers(t, restarted, "b", "c", "d", "a")
			if err := restarted.reconcileActivePool(context.Background(), "replay"); err != nil {
				t.Fatal(err)
			}
			stickyProjection(t, h, "b", "c", "d", "a")
			if !reviewAuth(t, h, "e").Disabled {
				t.Fatal("outgoing member not hard retired on replay")
			}
		})
	}
}

func TestStickyFailedStateSaveOrInventoryDoesNotProject(t *testing.T) {
	for _, cause := range []string{"save", "list"} {
		t.Run(cause, func(t *testing.T) {
			r, h := reviewRuntime(t, 5)
			reviewRun(t, r)
			before := cloneActivePool(r.state.ActivePool)
			h.quota("a", 0)
			if cause == "save" {
				r.store.path = t.TempDir() // rename over a directory must fail
			} else {
				r.host = &stickyFaultHost{reviewDiskHost: h, listErr: errors.New("inventory unavailable")}
			}
			if _, err := r.run(context.Background(), true, cause); err == nil {
				t.Fatal("expected injected error")
			}
			if !reflect.DeepEqual(before, r.state.ActivePool) {
				t.Fatal("failed transaction changed in-memory membership")
			}
			stickyProjection(t, h, "a", "b", "c", "d")
		})
	}
}

func TestStickyRestRecoveryOwnershipSurvivesFailedEnable(t *testing.T) {
	r, h := reviewRuntime(t, 1)
	h.quota("a", 0)
	reviewRun(t, r)
	h.quota("a", 100)
	reviewExpire(r, "a")
	r.host = &stickyFaultHost{reviewDiskHost: h, failSave: "a.json"}
	if _, err := r.run(context.Background(), true, "failed enable"); err == nil {
		t.Fatal("expected write failure")
	}
	restarted := newRuntime(h)
	q := restarted.state.Quota["a"]
	if !q.ManagedRest || q.RestUntil == nil {
		t.Fatal("lost durable permission to enable managed rest")
	}
	if err := restarted.reconcileActivePool(context.Background(), "recover enable"); err != nil {
		t.Fatal(err)
	}
	stickyProjection(t, h, "a")
	if q := restarted.state.Quota["a"]; q.ManagedRest || q.RestUntil != nil {
		t.Fatal("successful enable did not clear ownership")
	}
}

func TestStickyGeoFillsWhileProbeBlockedAndCannotBeUndone(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	reviewRun(t, r)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	h.hook = func(req hostHTTPRequest) {
		if req.AuthIndex == "a" {
			once.Do(func() { close(entered) })
			<-release
		}
	}
	runDone := make(chan error, 1)
	go func() { _, err := r.run(context.Background(), true, "blocked probe"); runDone <- err }()
	<-entered
	geoDone := make(chan error, 1)
	go func() { geoDone <- r.handleUsage(context.Background(), reviewGeo("a")) }()
	select {
	case err := <-geoDone:
		if err != nil {
			close(release)
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("geo retirement/fill blocked on quota probe")
	}
	stickyMembers(t, r, "b", "c", "d", "e")
	stickyProjection(t, h, "b", "c", "d", "e")
	close(release)
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	stickyProjection(t, h, "b", "c", "d", "e")
	if !reviewAuth(t, h, "a").Disabled {
		t.Fatal("stale probe resurrected geo-paused member")
	}
}

func TestStickyConcurrentSaveAndDashboardPreserveMembership(t *testing.T) {
	r, _ := reviewRuntime(t, 5)
	reviewRun(t, r)
	var wg sync.WaitGroup
	for worker := 0; worker < 4; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < 20; i++ {
				var err error
				switch worker {
				case 0:
					err = r.reconcileActivePool(context.Background(), "concurrent")
				case 1:
					r.scheduleEgressReturn(time.Now().Add(time.Hour))
				case 2:
					err = r.persistState()
				case 3:
					_, err = r.dashboard(context.Background())
				}
				if err != nil {
					t.Error(err)
				}
			}
		}(worker)
	}
	wg.Wait()
	loaded, err := r.store.load()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(loaded.ActivePool, r.state.ActivePool) || loaded.EgressReturnAt == nil {
		t.Fatal("concurrent persistence overwrote latest durable state")
	}
}

func TestStickyCancelledProbeIsNotTerminalFailure(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	reviewRun(t, r)
	ctx, cancel := context.WithCancel(context.Background())
	h.hook = func(hostHTTPRequest) { cancel() }
	if _, err := r.run(ctx, true, "cancel"); !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if r.state.Quota["a"].FailCount != 0 {
		t.Fatal("cancelled lifecycle counted as account failure")
	}
	stickyMembers(t, r, "a", "b", "c", "d")
}

func TestStickyCorruptStateFailsClosed(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	broken := []byte(`{"active_pool":`)
	if err := os.WriteFile(r.store.path, broken, 0600); err != nil {
		t.Fatal(err)
	}
	r = newRuntime(h)
	if _, err := r.run(context.Background(), true, "corrupt"); err == nil {
		t.Fatal("corrupt state was treated as cold boot")
	}
	if err := r.configure(defaultSettings()); err == nil {
		t.Fatal("configure overwrote corrupt state")
	}
	if err := r.updateSettings(defaultSettings()); err == nil {
		t.Fatal("settings overwrote corrupt state")
	}
	if err := r.persistState(); err == nil {
		t.Fatal("background save overwrote corrupt state")
	}
	raw, _ := os.ReadFile(r.store.path)
	if string(raw) != string(broken) {
		t.Fatal("destroyed corrupt-state recovery evidence")
	}
	stickyProjection(t, h, "a", "b", "c", "d")
}
