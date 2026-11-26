package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreBlocksEnabledAGAddedAfterPlanning(t *testing.T) {
	for _, filename := range []string{"late.json", "nested/late.json"} {
		t.Run(filename, func(t *testing.T) {
			r, h := newPool429Fixture(t)
			pool429Send(t, r)
			finalList := h.listN + 3 // Initial lock projection, plan, final inventory.
			h.onList = func(n int) {
				if n != finalList {
					return
				}
				h.mu.Lock()
				defer h.mu.Unlock()
				h.files["late"] = authFile{AuthIndex: "late", Name: filename, Provider: "antigravity", Priority: 200}
				h.docs["late"] = json.RawMessage(`{"type":"antigravity","disabled":false,"priority":200}`)
			}
			if _, err := r.restorePool429(context.Background()); err == nil {
				t.Fatal("enabled AG outside the completed restore opened the gate")
			}
			state, err := r.store.load()
			if err != nil || !state.PoolLocked429 || !state.Pool429Restoring || h.files["fixture-ag-00"].Disabled {
				t.Fatalf("restore must keep the gate without rolling back successful accounts: %v", err)
			}
		})
	}
}

func TestRestoreScansPhysicalDirectoryBeforeWatcherSeesNewAG(t *testing.T) {
	dir := t.TempDir()
	approved := filepath.Join(dir, "approved.json")
	late := filepath.Join(dir, "late.json")
	write := func(path, body string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(approved, `{"type":"antigravity","disabled":false}`)
	files := []authFile{{Name: "approved.json", Path: approved, AuthIndex: "approved", Provider: "antigravity"}}
	before := map[string]pool429Policy{"approved": {}}
	allowed := map[string]bool{"approved": true}
	for _, body := range []string{
		`{"type":"antigravity","disabled":false}`,
		`{"provider":"antigravity","disabled":false}`,
		`{"type":"antigravity","provider":"codex","disabled":false}`,
		`{"type":"codex","provider":"antigravity","disabled":false}`,
		`{"type":"antigravity","Disabled":true}`,
		`{"type":"antigravity","disabled":false,"DISABLED":true}`,
		`{"type":"antigravity","disabled":false,"TYPE":"codex"}`,
		`{"provider":"antigravity","disabled":false,"PROVIDER":"codex"}`,
	} {
		write(late, body)
		if err := verifyRestoreDisk(files, before, allowed); err == nil {
			t.Fatalf("physical AG credential absent from runtime inventory escaped the gate: %s", body)
		}
	}
	write(late, `{"type":"antigravity","disabled":true}`)
	if err := verifyRestoreDisk(files, before, allowed); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(approved); err != nil {
		t.Fatal(err)
	}
	if err := verifyRestoreDisk(files, before, allowed); err == nil {
		t.Fatal("stale runtime inventory hid a physically missing snapshot credential")
	}
}
