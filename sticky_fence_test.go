package main

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"
)

func TestStickyInvariantAllFourWorkerHealthCombinations(t *testing.T) {
	cfg := defaultSettings()
	cfg.Providers = []string{"antigravity"}
	now := time.Now().UTC()
	// 3^4 combinations, including all-transient and all-dead incumbents.
	for combination := 0; combination < 81; combination++ {
		t.Run(fmt.Sprint(combination), func(t *testing.T) {
			previous := map[string]activePoolState{"antigravity": {Members: []string{"a", "b", "c", "d"}, Generation: 1}}
			var credentials []credentialState
			var survivors []string
			code := combination
			for i, id := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
				c := credentialState{Provider: "antigravity", AuthIndex: id, CurrentTier: tierBackup, ProposedTier: tierBackup, Quota: quotaSnapshot{Status: quotaReady, Remaining: ptrInt(100)}}
				if i < 4 {
					switch code % 3 {
					case 0:
						c.Quota.Remaining = ptrInt(1)
					case 1:
						c.Quota.Status, c.Quota.Remaining = quotaRetry, nil
					case 2:
						c.Quota.Remaining = ptrInt(0)
					}
					if code%3 != 2 {
						survivors = append(survivors, id)
					}
					code /= 3
				}
				credentials = append(credentials, c)
			}
			next := selectActivePool(cfg, credentials, previous, now)["antigravity"]
			want := slices.Clone(survivors)
			for _, id := range []string{"e", "f", "g", "h"} {
				if len(want) < 4 {
					want = append(want, id)
				}
			}
			if !slices.Equal(next.Members, want) {
				t.Fatalf("transition=%v want=%v", next.Members, want)
			}
			for _, c := range credentials[:4] {
				if slices.Contains(survivors, c.AuthIndex) && c.ProposedTier != tierPrimary {
					t.Fatalf("surviving lease lost 400: %+v", c)
				}
			}
		})
	}
}

func TestStickyGrowthToSixAndExplicitZeroOptOut(t *testing.T) {
	r, h := reviewRuntime(t, 8)
	reviewRun(t, r)
	cfg := r.state.Settings
	cfg.ActivePoolSize = 6
	if err := r.updateSettings(cfg); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(r.shutdown)
	stickyMembers(t, r, "a", "b", "c", "d", "e", "f")
	stickyProjection(t, h, "a", "b", "c", "d", "e", "f")
	cfg.ActivePoolSize = 0
	if err := r.updateSettings(cfg); err != nil {
		t.Fatal(err)
	}
	h.quota("a", 10)
	reviewRun(t, r)
	if reviewAuth(t, h, "a").Priority != 200 {
		t.Fatal("explicit size=0 did not restore strategy-only behavior")
	}
	// Dormant membership survives opt-out; re-enabling revalidates it.
	stickyMembers(t, r, "a", "b", "c", "d", "e", "f")
}

func TestStickyManualPauseOverridesUnknownProbe(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	reviewRun(t, r)
	r.state.Settings.Strategy = strategyManual
	r.state.Settings.ManualTiers["a"] = tierPaused
	r.state.Quota["a"] = quotaSnapshot{Status: quotaRetry, FailCount: 1}
	if err := r.reconcileActivePool(context.Background(), "manual pause during jitter"); err != nil {
		t.Fatal(err)
	}
	stickyMembers(t, r, "b", "c", "d", "e")
	if f := reviewAuth(t, h, "a"); f.Disabled || f.Priority != -1 {
		t.Fatalf("explicit manual pause lost to transient protection: %+v", f)
	}
}

func TestStickyReturnedPlanDoesNotAliasMutableDashboard(t *testing.T) {
	r, _ := reviewRuntime(t, 5)
	result, err := r.run(context.Background(), true, "management response")
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			if _, err := json.Marshal(result); err != nil {
				t.Error(err)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			r.markLatestGeoPaused(usageEvent{AuthIndex: "a"}, time.Now().Add(time.Hour))
		}
	}()
	wg.Wait()
	if result.Credentials[0].ProposedTier != tierPrimary {
		t.Fatal("geo update mutated an already returned management response")
	}
}

type stickyBlockingSaveHost struct {
	*reviewDiskHost
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (h *stickyBlockingSaveHost) saveAuth(ctx context.Context, name string, raw json.RawMessage) error {
	if name == "e.json" {
		h.once.Do(func() {
			close(h.entered)
			<-h.release
		})
	}
	return h.reviewDiskHost.saveAuth(ctx, name, raw)
}

func TestStickyGeoSerializedWithInFlightPromotionWrite(t *testing.T) {
	r, h := reviewRuntime(t, 5)
	reviewRun(t, r)
	blocked := &stickyBlockingSaveHost{reviewDiskHost: h, entered: make(chan struct{}), release: make(chan struct{})}
	r.host = blocked
	h.quota("a", 0)
	runDone := make(chan error, 1)
	go func() { _, err := r.run(context.Background(), true, "promote e"); runDone <- err }()
	<-blocked.entered
	geoDone := make(chan error, 1)
	go func() { geoDone <- r.handleUsage(context.Background(), reviewGeo("e")) }()
	// Promotion holds the fence until the host write returns. Geo must follow
	// it, not write disabled=true and then be overwritten by stale promotion.
	select {
	case err := <-geoDone:
		close(blocked.release)
		t.Fatalf("geo bypassed projection fence: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(blocked.release)
	for _, done := range []chan error{runDone, geoDone} {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("projection/geo deadlocked")
		}
	}
	stickyMembers(t, r, "b", "c", "d")
	stickyProjection(t, h, "b", "c", "d")
	if !reviewAuth(t, h, "e").Disabled {
		t.Fatal("in-flight promotion resurrected a geo-dead account")
	}
}
