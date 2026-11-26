package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// The final inventory is fresh, not the inventory from before the auth writes.
func (r *runtime) verifyRestoreInventory(ctx context.Context, before map[string]pool429Policy, result plan) error {
	files, err := r.host.listAuth(ctx)
	if err != nil {
		return fmt.Errorf("recheck credentials before unlock: %w", err)
	}
	allowed := make(map[string]bool, len(before))
	for _, credential := range result.Credentials {
		if _, known := before[credential.AuthIndex]; known && !credential.ProposedDisabled {
			allowed[credential.AuthIndex] = true
		}
	}
	checked := make(map[string]bool, len(before))
	for _, file := range files {
		if providerOf(file) != "antigravity" {
			continue
		}
		if managedAuthFile(file) {
			checked[file.AuthIndex] = true
		}
		if !file.Disabled && (!managedAuthFile(file) || !allowed[file.AuthIndex]) {
			return fmt.Errorf("enabled AG credential outside the completed restore; pool remains locked (index %q)", file.AuthIndex)
		}
	}
	for index := range before {
		if !checked[index] {
			return fmt.Errorf("snapshot credential disappeared before unlock: %s", index)
		}
	}
	if _, native := r.host.(hostAdapter); native {
		return verifyRestoreDisk(files, before, allowed)
	}
	return nil
}

// CPA's host.auth.list uses the in-memory manager while it is running. Scan
// the physical auth directory too, so a fresh file cannot bypass the gate
// while the watcher is still registering it.
func verifyRestoreDisk(files []authFile, before map[string]pool429Policy, allowed map[string]bool) error {
	dirs, requiredPaths, allowedPaths := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, file := range files {
		path := filepath.Clean(file.Path)
		_, inSnapshot := before[file.AuthIndex]
		if !filepath.IsAbs(path) || filepath.Base(path) != file.Name {
			if inSnapshot {
				return fmt.Errorf("cannot verify on-disk path for restored credential %s", file.AuthIndex)
			}
			continue
		}
		dirs[filepath.Dir(path)] = true
		if inSnapshot {
			requiredPaths[path] = true
		}
		if allowed[file.AuthIndex] {
			allowedPaths[path] = true
		}
	}
	if len(dirs) == 0 {
		return fmt.Errorf("cannot verify physical auth directory before unlock")
	}
	for dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("scan physical auth directory before unlock: %w", err)
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(strings.ToLower(entry.Name()), ".json") {
				continue
			}
			if !entry.Type().IsRegular() {
				return fmt.Errorf("non-regular credential in auth directory; pool remains locked")
			}
			path := filepath.Join(dir, entry.Name())
			raw, err := os.ReadFile(path)
			var file *authFile
			if err != nil || json.Unmarshal(raw, &file) != nil || file == nil {
				return fmt.Errorf("unreadable physical credential; pool remains locked")
			}
			isAG := strings.EqualFold(strings.TrimSpace(file.Type), "antigravity") || strings.EqualFold(strings.TrimSpace(file.Provider), "antigravity")
			if isAG && !file.Disabled && !allowedPaths[path] {
				return fmt.Errorf("enabled AG file was not restored; pool remains locked")
			}
			delete(requiredPaths, path)
		}
	}
	if len(requiredPaths) != 0 {
		return fmt.Errorf("snapshot credential missing on disk; pool remains locked")
	}
	return nil
}
