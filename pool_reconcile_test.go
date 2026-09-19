package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"
)

func TestReconcileActivePoolPromotesReserveAfterPausedPrimary(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	host := &fakeHost{
		files: []authFile{
			{AuthIndex: "failed", Name: "failed.json", Provider: "antigravity", Priority: 400, Disabled: true},
			{AuthIndex: "reserve-a", Name: "reserve-a.json", Provider: "antigravity", Priority: 200, Disabled: false},
			{AuthIndex: "reserve-b", Name: "reserve-b.json", Provider: "antigravity", Priority: 200, Disabled: false},
		},
		documents: map[string]authDocument{
			"failed":    {AuthIndex: "failed", Name: "failed.json", JSON: json.RawMessage(`{"priority":400,"disabled":true}`)},
			"reserve-a": {AuthIndex: "reserve-a", Name: "reserve-a.json", JSON: json.RawMessage(`{"priority":200,"disabled":false}`)},
			"reserve-b": {AuthIndex: "reserve-b", Name: "reserve-b.json", JSON: json.RawMessage(`{"priority":200,"disabled":false}`)},
		},
	}
	r := newRuntime(host)
	r.store.path = filepath.Join(t.TempDir(), "state.json")
	r.state.Settings = defaultSettings()
	r.state.Settings.ActivePoolSize = 1
	r.state.Quota = map[string]quotaSnapshot{
		"failed":    {Remaining: ptrInt(0), RestUntil: ptrTime(now.Add(time.Hour)), Status: quotaReady, ObservedAt: now},
		"reserve-a": readyQuota(90, ptrTime(now.Add(time.Hour)), now),
		"reserve-b": readyQuota(80, ptrTime(now.Add(time.Hour)), now),
	}
	if err := r.reconcileActivePool(context.Background(), "补位"); err != nil {
		t.Fatal(err)
	}
	var promoted map[string]any
	if err := json.Unmarshal(host.saved["reserve-a.json"], &promoted); err != nil {
		t.Fatal(err)
	}
	if promoted["disabled"] != false || int(promoted["priority"].(float64)) != 400 {
		t.Fatalf("promoted account state=%v", promoted)
	}
}

func TestReconcileActivePoolRestoresExpiredManagedPause(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	host := &fakeHost{
		files: []authFile{{AuthIndex: "a", Name: "account.json", Provider: "antigravity", Priority: -1, Disabled: true}},
		documents: map[string]authDocument{
			"a": {AuthIndex: "a", Name: "account.json", JSON: json.RawMessage(`{"priority":-1,"disabled":true}`)},
		},
	}
	r := newRuntime(host)
	r.store.path = filepath.Join(t.TempDir(), "state.json")
	r.state.Settings = defaultSettings()
	r.state.Settings.ActivePoolSize = 1
	r.state.Quota = map[string]quotaSnapshot{
		"a": {Remaining: ptrInt(80), RestUntil: ptrTime(time.Now().UTC().Add(-time.Minute)), ManagedRest: true, Status: quotaReady, ObservedAt: now.Add(-time.Hour)},
	}
	// reconcile uses wall clock; an expired deadline remains expired regardless of
	// the exact current time in this test.
	if err := r.reconcileActivePool(context.Background(), "到期恢复"); err != nil {
		t.Fatal(err)
	}
	var saved map[string]any
	if err := json.Unmarshal(host.saved["account.json"], &saved); err != nil {
		t.Fatal(err)
	}
	if saved["disabled"] != false || int(saved["priority"].(float64)) != 400 {
		t.Fatalf("restored account state=%v", saved)
	}
}
