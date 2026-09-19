package main

import (
	"context"
	"sort"
	"time"
)

// applyActivePoolCap keeps healthy incumbents in the primary tier and fills
// vacancies by rank. The cap is per provider because CPA selects credentials per provider.
// A zero cap leaves the strategy's normal tier decisions unchanged.
func applyActivePoolCap(cfg *settings, credentials []credentialState, now time.Time) {
	if cfg == nil || cfg.ActivePoolSize <= 0 {
		return
	}
	groups := make(map[string][]int)
	candidates := make(map[string][]int)
	for index := range credentials {
		credential := &credentials[index]
		groups[credential.Provider] = append(groups[credential.Provider], index)
		if poolCandidate(*credential, now) {
			candidates[credential.Provider] = append(candidates[credential.Provider], index)
		}
	}
	for provider, indexes := range groups {
		eligible := candidates[provider]
		sort.SliceStable(eligible, func(left, right int) bool {
			return poolCandidateBefore(*cfg, credentials[eligible[left]], credentials[eligible[right]])
		})

		// Incumbents are non-preemptive: a healthy credential that already
		// occupies a primary slot stays there while vacancies are filled around
		// it. Only an over-cap historical state is trimmed among incumbents.
		incumbents := make([]int, 0, len(eligible))
		for _, index := range eligible {
			if credentials[index].CurrentTier == tierPrimary {
				incumbents = append(incumbents, index)
			}
		}
		if len(incumbents) > cfg.ActivePoolSize {
			incumbents = incumbents[:cfg.ActivePoolSize]
		}
		selected := make(map[int]bool, min(cfg.ActivePoolSize, len(eligible)))
		for _, index := range incumbents {
			selected[index] = true
		}
		needed := cfg.ActivePoolSize - len(selected)
		for _, index := range eligible {
			if needed == 0 {
				break
			}
			if selected[index] {
				continue
			}
			selected[index] = true
			needed--
		}
		for _, index := range eligible {
			credential := &credentials[index]
			if selected[index] {
				credential.ProposedTier = tierPrimary
				credential.Reason = "活跃池现任/补位"
			} else {
				credential.ProposedTier = tierBackup
				credential.Reason = "活跃池后备"
			}
			credential.Changed = credential.ProposedTier != credential.CurrentTier
		}
		// A failed/unknown probe is not eligible for the active pool. Demote a
		// stale primary as well, otherwise a probe failure could exceed the cap.
		for _, index := range indexes {
			credential := &credentials[index]
			if selected[index] || credential.Unavailable || credential.ProposedTier != tierPrimary {
				continue
			}
			credential.ProposedTier = tierBackup
			credential.Reason = "活跃池外后备"
			credential.Changed = credential.ProposedTier != credential.CurrentTier
		}
	}
}

func poolCandidate(credential credentialState, now time.Time) bool {
	if credential.Disabled || credential.Unavailable || credential.ProposedTier == tierPaused {
		return false
	}
	if credential.Quota.Remaining == nil || *credential.Quota.Remaining <= 0 {
		return false
	}
	return credential.Quota.RestUntil == nil || !credential.Quota.RestUntil.After(now)
}

func poolCandidateBefore(cfg settings, left, right credentialState) bool {
	leftRemaining := remainingValue(left)
	rightRemaining := remainingValue(right)
	if cfg.Strategy == strategyRotate || cfg.Strategy == strategyManual {
		return left.AuthIndex < right.AuthIndex
	}
	if cfg.Strategy == strategyReset {
		leftReset := resetValue(left)
		rightReset := resetValue(right)
		switch {
		case leftReset.Before(rightReset):
			return true
		case rightReset.Before(leftReset):
			return false
		}
	}
	if leftRemaining != rightRemaining {
		return leftRemaining > rightRemaining
	}
	return left.AuthIndex < right.AuthIndex
}

func remainingValue(credential credentialState) int {
	if credential.Quota.Remaining == nil {
		return -1
	}
	return *credential.Quota.Remaining
}

func resetValue(credential credentialState) time.Time {
	if credential.Quota.ResetAt == nil {
		return time.Time{}
	}
	return credential.Quota.ResetAt.UTC()
}

// reconcileActivePool applies a pool change from cached quota observations without
// issuing a second upstream probe. It is used by passive failure events so a reserve
// is promoted before the next affinity selection.
func (r *runtime) reconcileActivePool(ctx context.Context, trigger string) error {
	r.mu.Lock()
	cfg := normalizeSettings(r.state.Settings)
	cache := cloneQuota(r.state.Quota)
	r.mu.Unlock()
	if cfg.ActivePoolSize <= 0 {
		return nil
	}

	r.runMu.Lock()
	defer r.runMu.Unlock()
	files, err := r.host.listAuth(ctx)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	selected := providerSet(cfg.Providers)
	credentials := make([]credentialState, 0, len(files))
	for _, file := range files {
		if !managedAuthFile(file) {
			continue
		}
		provider := providerOf(file)
		if provider == "" || !selected[provider] {
			continue
		}
		previous := cache[file.AuthIndex]
		restExpired := provider == "antigravity" && previous.ManagedRest && previous.RestUntil != nil && !previous.RestUntil.After(now)
		effectiveDisabled := file.Disabled && !restExpired
		current := tierFromPriority(file.Priority, effectiveDisabled)
		if restExpired && current == tierPaused {
			current = tierRegular
		}
		quota := previous
		if quota.Status == "" {
			quota.Status = quotaUnknown
		}
		planningFile := file
		planningFile.Disabled = effectiveDisabled
		proposed, reason := chooseTier(cfg, planningFile, current, quota, now)
		credentials = append(credentials, credentialState{
			Provider: provider, Account: maskAccount(firstText(file.Email, file.Account, file.Name)), AuthIndex: file.AuthIndex,
			CurrentTier: current, ProposedTier: proposed, Reason: reason, Quota: quota,
			Disabled: effectiveDisabled, Unavailable: file.Unavailable,
			Changed: proposed != current && !file.Unavailable,
		})
	}
	if len(credentials) == 0 {
		return nil
	}
	applyActivePoolCap(&cfg, credentials, now)
	for index := range credentials {
		credentials[index].Changed = credentials[index].ProposedTier != credentials[index].CurrentTier && !credentials[index].Unavailable
	}
	result := plan{GeneratedAt: now, Strategy: cfg.Strategy, Credentials: credentials}
	for _, credential := range credentials {
		if credential.Changed {
			result.Changes++
		}
		if credential.Quota.Status == quotaUnknown {
			result.Unknown++
		}
	}
	if err := r.applyPlan(ctx, files, result); err != nil {
		return err
	}
	r.mu.Lock()
	r.latest = result
	state := r.state
	r.mu.Unlock()
	if err := r.store.save(state); err != nil {
		return err
	}
	if result.Changes > 0 {
		r.recordHistory(trigger, result.Changes, result.Unknown, historySummary(result))
	}
	return nil
}
