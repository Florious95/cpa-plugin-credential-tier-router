package main

import "time"

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
		if !exhausted {
			return false
		}
	}
	if !exhausted && current != tierPaused {
		return false
	}

	until := now.Add(cfg.restDuration())
	quota.RestUntil = &until
	return true
}
