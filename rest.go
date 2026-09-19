package main

import "time"

func inheritAntigravityRest(quota *quotaSnapshot, previous quotaSnapshot, now time.Time) {
	if quota == nil || quota.RestUntil != nil || previous.RestUntil == nil || !previous.RestUntil.After(now) {
		return
	}
	until := previous.RestUntil.UTC()
	quota.RestUntil = &until
	// Rest ownership is persisted separately from the latest probe result. A
	// successful probe must not turn a managed rest into an ordinary snapshot.
	quota.ManagedRest = true
}

func applyAntigravityRest(cfg *settings, file authFile, current tierName, quota *quotaSnapshot, now time.Time) bool {
	if cfg == nil || quota == nil || providerOf(file) != "antigravity" {
		return false
	}

	exhausted := quota.Remaining != nil && *quota.Remaining <= 0
	if quota.RestUntil != nil {
		if quota.RestUntil.After(now) {
			return false
		}
		// An expired rest is released here. A recovered quota must not be
		// immediately re-paused merely because the persisted tier is still paused.
		quota.RestUntil = nil
		quota.ManagedRest = false
		if !exhausted {
			return false
		}
	}
	if !exhausted && current != tierPaused {
		return false
	}

	until := now.Add(cfg.restDuration())
	quota.RestUntil = &until
	quota.ManagedRest = true
	return true
}
