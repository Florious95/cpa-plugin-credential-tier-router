package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var errPool429Changed = errors.New("429熔断状态已变化，请刷新后重新确认人工恢复")

func (r *runtime) poolLocked429() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.state.PoolLocked429 || r.pool429Pending
}

func (r *runtime) pickWithPoolLock(raw []byte) []byte {
	var request struct {
		Provider  string
		Providers []string
	}
	if err := json.Unmarshal(raw, &request); err != nil {
		return failure("invalid_scheduler", "invalid scheduler request", false)
	}
	ag := strings.EqualFold(strings.TrimSpace(request.Provider), "antigravity")
	for _, provider := range request.Providers {
		ag = ag || strings.EqualFold(strings.TrimSpace(provider), "antigravity")
	}
	if ag && r.poolLocked429() {
		// An explicit rejection is essential: an empty/invalid auth ID falls back.
		return success(map[string]any{"Handled": true, "Reject": true, "RejectCode": "antigravity_pool_429_locked", "RejectReason": "Antigravity全池429熔断，等待人工恢复"})
	}
	return success(map[string]bool{"Handled": false})
}

func (r *runtime) tripPool429(ctx context.Context) error {
	// Close selection immediately, without waiting for a slow probe or commit.
	// Never hold mu while acquiring commitMu (the normal order is commitMu -> mu).
	r.mu.Lock()
	r.pool429Pending = true
	r.pool429Revision++
	revision := r.pool429Revision
	r.mu.Unlock()
	r.commitMu.Lock()
	defer r.commitMu.Unlock()
	r.mu.Lock()
	r.state.PoolLocked429 = true
	r.state.Pool429Generation++
	if r.state.Pool429Since == nil {
		now := time.Now().UTC()
		r.state.Pool429Since = &now
	}
	r.mu.Unlock()
	changed, err := r.pauseAllAntigravityCredentials(ctx)
	errorsCount := 0
	if err != nil {
		errorsCount = 1
	}
	r.recordHistory("429全池熔断", changed, errorsCount, "Antigravity全池已锁定，不会自动恢复；需人工解除")
	saveErr := r.persistState()
	r.mu.Lock()
	if r.pool429Revision == revision {
		r.pool429Pending = false
	}
	r.latest = plan{}
	r.mu.Unlock()
	return errors.Join(err, saveErr)
}

// Caller holds commitMu. This is durable intent plus per-file projection, not a
// filesystem batch transaction. Keep disabling other accounts after any error.
func (r *runtime) pauseAllAntigravityCredentials(ctx context.Context) (int, error) {
	files, listErr := r.host.listAuth(ctx)
	r.mu.Lock()
	initial := r.state.Pool429Before == nil
	if initial {
		r.state.Pool429Before = map[string]pool429Policy{}
	}
	for _, file := range files {
		if providerOf(file) == "antigravity" {
			if _, exists := r.state.Pool429Before[file.AuthIndex]; !exists {
				previous := pool429Policy{Disabled: true, Priority: -1}
				if initial {
					previous = pool429Policy{Disabled: file.Disabled, Priority: file.Priority}
				}
				r.state.Pool429Before[file.AuthIndex] = previous // New arrivals need separate operator enablement.
			}
		}
	}
	r.mu.Unlock()
	failures := []error{listErr, r.persistState()}
	changed := 0
	for _, file := range files {
		if providerOf(file) != "antigravity" {
			continue
		}
		if !managedAuthFile(file) || file.AuthIndex == "" {
			failures = append(failures, errors.New("AG凭据没有可写认证文件，全池路由仍锁定"))
			continue
		}
		if file.Disabled && file.Priority == -1 {
			continue
		}
		document, err := r.host.getAuth(ctx, file.AuthIndex)
		if err == nil {
			var root map[string]any
			if err = json.Unmarshal(document.JSON, &root); err == nil && root == nil {
				err = errors.New("AG认证文档不是JSON对象")
			}
			if err == nil {
				root["disabled"], root["priority"] = true, -1
				var updated []byte
				if updated, err = json.Marshal(root); err == nil {
					err = r.host.saveAuth(ctx, firstText(document.Name, file.Name), updated)
				}
			}
		}
		if err != nil {
			failures = append(failures, fmt.Errorf("停用AG凭据%s失败: %w", file.AuthIndex, err))
			continue
		}
		changed++
	}
	return changed, errors.Join(failures...)
}

func (r *runtime) enforcePoolLock429(ctx context.Context) error {
	r.commitMu.Lock()
	defer r.commitMu.Unlock()
	r.mu.Lock()
	locked := r.state.PoolLocked429
	r.mu.Unlock()
	if !locked {
		return nil
	}
	_, err := r.pauseAllAntigravityCredentials(ctx)
	return err
}

func (r *runtime) restorePool429(ctx context.Context) (plan, error) {
	r.commitMu.Lock()
	defer r.commitMu.Unlock()
	r.mu.Lock()
	if r.pool429Pending {
		r.mu.Unlock()
		return plan{}, errPool429Changed
	}
	locked, revision := r.state.PoolLocked429, r.pool429Revision
	r.mu.Unlock()
	if !locked {
		return r.inspect(ctx) // Idempotent: never replay a previous restore snapshot.
	}
	if _, err := r.pauseAllAntigravityCredentials(ctx); err != nil {
		return plan{}, err
	}
	files, err := r.host.listAuth(ctx)
	if err != nil {
		return plan{}, err
	}
	r.mu.Lock()
	cfg := normalizeSettings(r.state.Settings)
	cache := cloneQuota(r.state.Quota)
	pools := cloneActivePool(r.state.ActivePool)
	before := r.state.Pool429Before
	r.mu.Unlock()
	cfg.Providers = []string{"antigravity"}
	var actual, planning []authFile
	for _, file := range files {
		if providerOf(file) != "antigravity" || !managedAuthFile(file) {
			continue
		}
		actual = append(actual, file)
		if previous, exists := before[file.AuthIndex]; exists {
			file.Disabled, file.Priority = previous.Disabled, previous.Priority
		}
		planning = append(planning, file)
	}
	result, next := planPool(cfg, planning, cache, pools, time.Now().UTC())
	byID := map[string]authFile{}
	for _, file := range actual {
		byID[file.AuthIndex] = file
	}
	result.Changes = 0
	for i := range result.Credentials {
		c := &result.Credentials[i]
		file := byID[c.AuthIndex]
		c.Changed = file.Priority != c.ProposedTier.priority() || file.Disabled != c.ProposedDisabled
		if c.Changed {
			result.Changes++
		}
	}
	if err = r.applyPlanWithPoolLock(ctx, actual, result, true); err != nil {
		_, pauseErr := r.pauseAllAntigravityCredentials(ctx)
		return plan{}, errors.Join(err, pauseErr)
	}
	for _, c := range result.Credentials {
		q := cache[c.AuthIndex]
		if !c.Unavailable && c.ProposedTier != tierPaused && q.ManagedRest && q.RestUntil != nil && !q.RestUntil.After(result.GeneratedAt) {
			q.RestUntil, q.ManagedRest = nil, false
			cache[c.AuthIndex] = q
		}
	}
	r.mu.Lock()
	if revision != r.pool429Revision || r.pool429Pending {
		r.mu.Unlock()
		_, pauseErr := r.pauseAllAntigravityCredentials(ctx)
		return plan{}, errors.Join(errPool429Changed, pauseErr)
	}
	oldQuota, oldPools, oldSince, oldHistory := r.state.Quota, r.state.ActivePool, r.state.Pool429Since, r.state.History
	r.state.Quota, r.state.ActivePool = cache, next
	r.state.PoolLocked429, r.state.Pool429Since, r.state.Pool429Before = false, nil, nil
	r.state.History = append([]historyEntry{{At: time.Now().UTC(), Trigger: "429人工恢复", Changes: result.Changes, Summary: "已人工解除全池熔断；原停用/普通休眠及上游冷却保持"}}, oldHistory...)
	if len(r.state.History) > 12 {
		r.state.History = r.state.History[:12]
	}
	err = r.store.save(r.state)
	if err != nil {
		r.state.PoolLocked429, r.state.Pool429Since, r.state.Pool429Before = true, oldSince, before
		r.state.Quota, r.state.ActivePool, r.state.History = oldQuota, oldPools, oldHistory
	} else {
		r.latest = plan{} // Re-read the actual post-write projection on the next GET.
	}
	r.mu.Unlock()
	if err != nil {
		_, pauseErr := r.pauseAllAntigravityCredentials(ctx)
		return plan{}, errors.Join(err, pauseErr)
	}
	return result, nil
}
