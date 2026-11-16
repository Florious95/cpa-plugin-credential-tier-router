package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestPool429ProtectionConfigDefaults(t *testing.T) {
	for _, tc := range []struct {
		name, raw        string
		enabled, invalid bool
	}{
		{"empty", "", true, false},
		{"legacyJSON", `{}`, true, false},
		{"legacyYAML", "auto_apply: false", true, false},
		{"jsonOff", `{"pool_lock_429_enabled":false}`, false, false},
		{"jsonOn", `{"pool_lock_429_enabled":true}`, true, false},
		{"yamlOff", "pool_lock_429_enabled: false", false, false},
		{"yamlOn", "pool_lock_429_enabled: true", true, false},
		{"invalidYAML", "pool_lock_429_enabled: nonsense", false, true},
		{"invalidJSON", `{"pool_lock_429_enabled":"false"}`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parsePluginConfig([]byte(tc.raw))
			if (err != nil) != tc.invalid || (!tc.invalid && cfg.PoolLock429Enabled != tc.enabled) {
				t.Fatalf("enabled=%v error=%v", cfg.PoolLock429Enabled, err)
			}
		})
	}
}

func TestPool429ProtectionSwitchLifecycle(t *testing.T) {
	r, h := newPool429Fixture(t)
	original := map[string]json.RawMessage{}
	for id, raw := range h.docs {
		original[id] = append(json.RawMessage(nil), raw...)
	}
	cfg := r.state.Settings
	cfg.PoolLock429Enabled = false
	if err := r.updateSettings(cfg); err != nil {
		t.Fatal(err)
	}
	pool429Send(t, r)
	if r.poolLocked429() || r.state.Pool429Before != nil {
		t.Fatal("disabled protection still tripped the pool")
	}
	for id, raw := range original {
		if !bytes.Equal(raw, h.docs[id]) {
			t.Fatal("debug-mode429 changed an auth file")
		}
	}
	cfg.PoolLock429Enabled = true
	if err := r.updateSettings(cfg); err != nil {
		t.Fatal(err)
	}
	pool429Send(t, r)
	pool429AssertAllPaused(t, r, h)
	generation := r.state.Pool429Generation
	cfg.PoolLock429Enabled = false
	if err := r.updateSettings(cfg); err != nil {
		t.Fatal(err)
	}
	pool429Send(t, r)
	pool429AssertAllPaused(t, r, h)
	if r.state.Pool429Generation != generation {
		t.Fatal("disabled protection retripped an existing latch")
	}
	if _, err := r.restorePool429(context.Background()); err != nil {
		t.Fatal(err)
	}
	pool429Send(t, r)
	if r.poolLocked429() {
		t.Fatal("debug mode did not survive manual restore")
	}
	// The switch controls only429, not ordinary403's existing timed lifecycle.
	r.state.Settings.ActivePoolSize = 0
	if err := r.handleUsage(context.Background(), []byte(`{"Provider":"antigravity","AuthIndex":"fixture-ag-00","Failed":true,"Failure":{"StatusCode":403}}`)); err != nil {
		t.Fatal(err)
	}
	if r.poolLocked429() || !r.state.Quota["fixture-ag-00"].ManagedRest || !h.files["fixture-ag-00"].Disabled {
		t.Fatal("debug mode altered ordinary403 protection")
	}
}

func TestPool429ProtectionSettingsPersistence(t *testing.T) {
	r, h := newPool429Fixture(t)
	// Toggle-only saves must not rebalance credentials or restart the worker.
	r.state.Settings.AutoApply = true
	r.state.ActivePool = map[string]activePoolState{"antigravity": {Members: []string{"fixture-ag-05"}, Generation: 1}}
	writes := 0
	h.onSave = func(bool) { writes++ }
	cfg := r.state.Settings
	cfg.PoolLock429Enabled = false
	if err := r.updateSettings(cfg); err != nil {
		t.Fatal(err)
	}
	if writes != 0 || r.cancel != nil {
		t.Fatal("protection-only save rebalanced accounts or restarted the worker")
	}
	loaded, err := r.store.load()
	if err != nil || loaded.Settings.PoolLock429Enabled {
		t.Fatal("explicit false did not survive state persistence")
	}
	// An older settings client omits the new option; that must preserve false.
	r.state.Settings.AutoApply = false
	for _, null := range []bool{false, true} {
		raw, _ := json.Marshal(r.state.Settings)
		var body map[string]any
		json.Unmarshal(raw, &body)
		delete(body, "pool_lock_429_enabled")
		if null {
			body["pool_lock_429_enabled"] = nil
		}
		raw, _ = json.Marshal(body)
		wire, _ := json.Marshal(map[string]any{"Method": "PUT", "Path": "/plugins/credential-tier-router/settings", "Body": base64.StdEncoding.EncodeToString(raw)})
		response, err := r.handleManagement(context.Background(), wire)
		if err != nil || response.StatusCode != 200 || r.state.Settings.PoolLock429Enabled {
			t.Fatal("missing/null option unexpectedly changed existing protection")
		}
	}
	defer r.shutdown()
	// Legacy state has no flag: seeded defaults must enable production protection.
	raw, _ := json.Marshal(r.state)
	var legacy map[string]any
	json.Unmarshal(raw, &legacy)
	delete(legacy["settings"].(map[string]any), "pool_lock_429_enabled")
	raw, _ = json.Marshal(legacy)
	store := stateStore{path: filepath.Join(t.TempDir(), "legacy-state.json")}
	if err := os.WriteFile(store.path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err = store.load()
	if err != nil || !loaded.Settings.PoolLock429Enabled {
		t.Fatal("legacy state did not default protection on")
	}
}
