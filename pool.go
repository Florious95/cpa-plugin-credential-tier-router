package main

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"
)

type poolHealth uint8

const (
	poolDead poolHealth = iota
	poolTransient
	poolAlive
)

func classifyPoolHealth(cfg settings, c credentialState, now time.Time) poolHealth {
	if c.Disabled || c.Unavailable || c.ProposedTier == tierPaused ||
		(c.Quota.Remaining != nil && *c.Quota.Remaining <= 0) ||
		(c.Quota.RestUntil != nil && c.Quota.RestUntil.After(now)) ||
		c.Quota.FailCount >= cfg.FailureThreshold {
		return poolDead
	}
	if c.Quota.Remaining == nil || c.Quota.Status == quotaRetry || c.Quota.Status == quotaUnknown {
		return poolTransient
	}
	return poolAlive
}

func cloneActivePool(source map[string]activePoolState) map[string]activePoolState {
	out := make(map[string]activePoolState, len(source))
	for provider, state := range source {
		state.Members = slices.Clone(state.Members)
		out[provider] = state
	}
	return out
}

// selectActivePool is a pure membership transition. Only vacancies are ranked.
// Positive-size shrink is non-preemptive: surviving leases may exceed the new
// admission target until departures drain the excess. Zero explicitly opts out
// of pool management, preserving the legacy strategy-only configuration.
func selectActivePool(cfg settings, credentials []credentialState, previous map[string]activePoolState, now time.Time) map[string]activePoolState {
	next := cloneActivePool(previous)
	if cfg.ActivePoolSize <= 0 {
		return next
	}
	groups := make(map[string][]int)
	for i, c := range credentials {
		groups[c.Provider] = append(groups[c.Provider], i)
	}
	for _, provider := range cfg.Providers {
		indexes := groups[provider]
		byID := make(map[string]int, len(indexes))
		for _, i := range indexes {
			byID[credentials[i].AuthIndex] = i
		}
		old, initialized := previous[provider]
		seed := old.Members
		if !initialized {
			// Migration inherits actual incumbents, including unprobed ones.
			// Never rank existing workers against fresh reserves during bootstrap.
			seed = nil
			for _, i := range indexes {
				if credentials[i].CurrentTier == tierPrimary {
					seed = append(seed, credentials[i].AuthIndex)
				}
			}
			sort.Strings(seed)
		}
		members := make([]string, 0, len(seed))
		selected := make(map[string]bool, len(seed))
		for _, id := range seed {
			i, exists := byID[id]
			// Absence from a successful complete inventory is deletion. A list
			// error never reaches this function and cannot evict the whole pool.
			if !exists || selected[id] || classifyPoolHealth(cfg, credentials[i], now) == poolDead {
				continue
			}
			members = append(members, id)
			selected[id] = true
		}
		needed := cfg.ActivePoolSize - len(members)
		if needed > 0 {
			var reserves []int
			for _, i := range indexes {
				if !selected[credentials[i].AuthIndex] && classifyPoolHealth(cfg, credentials[i], now) == poolAlive {
					reserves = append(reserves, i)
				}
			}
			sort.SliceStable(reserves, func(i, j int) bool {
				return poolCandidateBefore(cfg, credentials[reserves[i]], credentials[reserves[j]])
			})
			for _, i := range reserves {
				if needed <= 0 {
					break
				}
				id := credentials[i].AuthIndex
				members = append(members, id)
				selected[id] = true
				needed--
			}
		}
		generation := old.Generation
		if !initialized || !slices.Equal(old.Members, members) {
			generation++
		}
		next[provider] = activePoolState{Members: members, Generation: generation}
		for _, i := range indexes {
			c := &credentials[i]
			if selected[c.AuthIndex] {
				c.ProposedTier, c.Reason = tierPrimary, "持久工位：在岗保活/空缺补位"
			} else if !c.Disabled && !c.Unavailable && c.ProposedTier != tierPaused {
				c.ProposedTier, c.Reason = tierBackup, "持久工位外后备"
			}
			c.Changed = c.ProposedTier != c.CurrentTier && !c.Unavailable
		}
	}
	return next
}

// Stateless entry retained for strategy unit tests; runtime always supplies its
// persisted membership to selectActivePool instead of bootstrapping each cycle.
func applyActivePoolCap(cfg *settings, credentials []credentialState, now time.Time) {
	if cfg != nil {
		selectActivePool(*cfg, credentials, nil, now)
	}
}

func poolCandidateBefore(cfg settings, left, right credentialState) bool {
	if cfg.Strategy == strategyRotate || cfg.Strategy == strategyManual {
		return left.AuthIndex < right.AuthIndex
	}
	if cfg.Strategy == strategyReset {
		leftReset, rightReset := resetValue(left), resetValue(right)
		switch {
		case leftReset.Before(rightReset):
			return true
		case rightReset.Before(leftReset):
			return false
		}
	}
	if remainingValue(left) != remainingValue(right) {
		return remainingValue(left) > remainingValue(right)
	}
	return left.AuthIndex < right.AuthIndex
}

func remainingValue(c credentialState) int {
	if c.Quota.Remaining == nil {
		return -1
	}
	return *c.Quota.Remaining
}

func resetValue(c credentialState) time.Time {
	if c.Quota.ResetAt == nil {
		return time.Time{}
	}
	return c.Quota.ResetAt.UTC()
}

// planPool computes intent from a fresh inventory. It does not mutate runtime
// state or write auth files, so preview cannot acquire or release a lease.
func planPool(cfg settings, files []authFile, cache map[string]quotaSnapshot, pools map[string]activePoolState, now time.Time) (plan, map[string]activePoolState) {
	selected := providerSet(cfg.Providers)
	var credentials []credentialState
	byID := make(map[string]authFile, len(files))
	for _, file := range files {
		provider := providerOf(file)
		if !managedAuthFile(file) || !selected[provider] {
			continue
		}
		byID[file.AuthIndex] = file
		quota := cache[file.AuthIndex]
		previousRest := quota.RestUntil
		restExpired := provider == "antigravity" && quota.ManagedRest && quota.RestUntil != nil && !quota.RestUntil.After(now)
		effectiveDisabled := file.Disabled && !restExpired
		current := tierFromPriority(file.Priority, effectiveDisabled)
		if restExpired && current == tierPaused {
			current = tierRegular
		}
		if quota.Status == "" {
			quota.Status = quotaUnknown
		}
		if !effectiveDisabled {
			applyAntigravityRest(&cfg, file, current, &quota, now)
		}
		planningFile := file
		planningFile.Disabled = effectiveDisabled
		proposed, reason := chooseTier(cfg, planningFile, current, quota, now)
		if provider == "antigravity" && cfg.Strategy == strategyManual && proposed == tierPaused && quota.RestUntil == nil {
			if applyAntigravityRest(&cfg, file, tierPaused, &quota, now) {
				reason = fmt.Sprintf("手动暂停，休眠至 %s", quota.RestUntil.UTC().Format(time.RFC3339))
			}
		}
		if restExpired && file.Disabled && proposed != tierPaused {
			// Keep ownership durable until the enabling write has succeeded.
			// Otherwise a crash between intent and projection strands this file
			// as an apparently external disable on restart.
			quota.RestUntil, quota.ManagedRest = previousRest, true
		}
		cache[file.AuthIndex] = quota
		credentials = append(credentials, credentialState{
			Provider: provider, Account: maskAccount(firstText(file.Email, file.Account, file.Name)), AuthIndex: file.AuthIndex,
			CurrentTier: current, ProposedTier: proposed, Reason: reason, Quota: quota,
			Disabled: effectiveDisabled, Unavailable: file.Unavailable,
		})
	}
	next := selectActivePool(cfg, credentials, pools, now)
	result := plan{GeneratedAt: now, Strategy: cfg.Strategy, Credentials: credentials}
	for i := range result.Credentials {
		c := &result.Credentials[i]
		file := byID[c.AuthIndex]
		// Compare the actual projection, not just tier names: a managed rest
		// can expire while the file still has disabled=true and priority=400.
		c.Changed = !c.Unavailable && (file.Priority != c.ProposedTier.priority() || file.Disabled != (c.ProposedTier == tierPaused))
		if c.Changed {
			result.Changes++
		}
		if c.Quota.Status == quotaUnknown {
			result.Unknown++
		}
	}
	sortCredentials(result.Credentials)
	return result, next
}

// reconcileActivePool is immediate, cache-only reconciliation; it never waits
// for a slow quota probe. All projections, including geo pauses, share commitMu.
func (r *runtime) reconcileActivePool(ctx context.Context, trigger string) error {
	r.commitMu.Lock()
	defer r.commitMu.Unlock()
	_, err := r.commitPool(ctx, nil, true, trigger)
	return err
}

// commitPool requires commitMu. Observations are merged into current state only
// after acquiring that fence, and auth inventory is re-read before planning.
func (r *runtime) commitPool(ctx context.Context, observations map[string]quotaSnapshot, apply bool, trigger string) (plan, error) {
	if r.loadErr != nil {
		return plan{}, fmt.Errorf("load state: %w", r.loadErr)
	}
	files, err := r.host.listAuth(ctx)
	if err != nil {
		return plan{}, err
	}
	r.mu.Lock()
	cfg := normalizeSettings(r.state.Settings)
	cache := cloneQuota(r.state.Quota)
	pools := cloneActivePool(r.state.ActivePool)
	r.mu.Unlock()
	for id, observation := range observations {
		// A geo rest committed while probing is newer than this observation.
		// Keep even an expired deadline until planPool resolves its ownership.
		if previous := cache[id]; previous.RestUntil != nil {
			observation.RestUntil, observation.ManagedRest = previous.RestUntil, previous.ManagedRest
		}
		cache[id] = observation
	}
	result, next := planPool(cfg, files, cache, pools, time.Now().UTC())
	r.mu.Lock()
	oldQuota, oldPools := r.state.Quota, r.state.ActivePool
	r.state.Quota = cache
	if apply {
		r.state.ActivePool = next
	}
	// Write-ahead desired state: a crash/partial auth write is replayable.
	// Never promote anyone when the durable intent could not be saved.
	err = r.store.save(r.state)
	if err != nil {
		r.state.Quota, r.state.ActivePool = oldQuota, oldPools
	}
	r.mu.Unlock()
	if err != nil {
		return plan{}, err
	}
	if apply {
		if err := r.applyPlan(ctx, files, result); err != nil {
			r.recordHistory(trigger, result.Changes, 1, "工位意图已保存，待重放："+safeError(err))
			_ = r.persistState()
			return plan{}, err
		}
		r.mu.Lock()
		for i := range result.Credentials {
			c := &result.Credentials[i]
			q := r.state.Quota[c.AuthIndex]
			if !c.Unavailable && c.ProposedTier != tierPaused && q.ManagedRest && q.RestUntil != nil && !q.RestUntil.After(result.GeneratedAt) {
				q.RestUntil, q.ManagedRest = nil, false
				r.state.Quota[c.AuthIndex] = q
				c.Quota = q
			}
		}
		r.mu.Unlock()
		r.recordHistory(trigger, result.Changes, result.Unknown, historySummary(result))
	}
	r.mu.Lock()
	r.latest = result
	// Usage events mutate the dashboard; the caller may concurrently encode
	// its returned plan as a management response.
	r.latest.Credentials = append([]credentialState(nil), result.Credentials...)
	r.mu.Unlock()
	if err := r.persistState(); err != nil {
		return plan{}, err
	}
	return result, nil
}
