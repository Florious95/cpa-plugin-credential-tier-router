package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
)

type stateStore struct {
	path string
}

func (s stateStore) load() (persistedState, error) {
	state := persistedState{Settings: defaultSettings(), Quota: map[string]quotaSnapshot{}}
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return persistedState{}, err
	}
	if err := json.Unmarshal(raw, &state); err != nil {
		return persistedState{}, err
	}
	if state.Quota == nil {
		state.Quota = map[string]quotaSnapshot{}
	}
	if state.Settings.ManualTiers == nil {
		state.Settings.ManualTiers = map[string]tierName{}
	}
	if err := state.Settings.validate(); err != nil {
		return persistedState{}, err
	}
	return state, nil
}

func (s stateStore) save(state persistedState) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), ".state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}
