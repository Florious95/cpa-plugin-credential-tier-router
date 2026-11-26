package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
)

type runtime struct {
	host hostAPI

	mu    sync.Mutex
	runMu sync.Mutex
	// Lock order: commitMu -> mu. Never hold mu across a host callback.
	// Slow probes hold runMu only; geo events can commit without waiting.
	commitMu  sync.Mutex
	loadErr   error
	state     persistedState
	latest    plan
	store     stateStore
	cancel    context.CancelFunc
	done      chan struct{}
	nextProbe *time.Time
	geoNow    func() time.Time

	closed          bool
	pool429Pending  bool
	pool429Revision uint64
}

func newRuntime(host hostAPI) *runtime {
	statePath := os.Getenv("CREDENTIAL_TIER_ROUTER_STATE_PATH")
	if statePath == "" {
		statePath = filepath.Join("credential-tier-router", "state.json")
	}
	r := &runtime{
		host:   host,
		store:  stateStore{path: statePath},
		done:   make(chan struct{}),
		geoNow: time.Now,
	}
	if state, err := r.store.load(); err == nil {
		r.state = state
		r.state.Settings = normalizeSettings(r.state.Settings)
	} else {
		r.loadErr = err // Corruption is not a new installation; do not bootstrap over it.
		r.state = persistedState{Settings: defaultSettings(), Quota: map[string]quotaSnapshot{}}
	}
	return r
}

func (r *runtime) handle(ctx context.Context, method string, request []byte) []byte {
	switch method {
	case "plugin.register", "plugin.reconfigure":
		cfg, err := decodeLifecycleConfig(request)
		if err != nil {
			return failure("invalid_config", err.Error(), false)
		}
		if err := r.configure(cfg); err != nil {
			return failure("invalid_config", err.Error(), false)
		}
		return success(registrationResult())
	case "plugin.shutdown":
		r.shutdown()
		return success(map[string]string{"status": "ok"})
	case "management.register":
		return success(managementRegistration())
	case "scheduler.pick":
		return r.pickWithPoolLock(request)
	case "usage.handle":
		if err := r.handleUsage(ctx, request); err != nil {
			return failure("invalid_usage", err.Error(), false)
		}
		return success(map[string]any{})
	case "management.handle":
		return r.handleManagementCall(ctx, request)
	default:
		return failure("invalid_request", fmt.Sprintf("unsupported method %q", method), false)
	}
}

// Management failures must remain HTTP responses. A plugin failure envelope is
// translated by CPA into a plain 502 body, which breaks JSON clients.
func (r *runtime) handleManagementCall(ctx context.Context, request []byte) (response []byte) {
	defer func() {
		if recovered := recover(); recovered != nil {
			response = success(jsonManagementResponse(500, map[string]string{
				"error": safeError(fmt.Errorf("management handler panic: %v", recovered)),
			}))
		}
	}()
	management, err := r.handleManagement(ctx, request)
	if err != nil {
		return success(jsonManagementResponse(500, map[string]string{"error": safeError(err)}))
	}
	return success(management)
}

func decodeLifecycleConfig(raw []byte) (settings, error) {
	if len(raw) == 0 {
		return defaultSettings(), nil
	}
	var wire struct {
		ConfigYAML json.RawMessage `json:"config_yaml"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		return settings{}, err
	}
	if len(wire.ConfigYAML) == 0 || string(wire.ConfigYAML) == "null" {
		return defaultSettings(), nil
	}
	var encoded string
	if err := json.Unmarshal(wire.ConfigYAML, &encoded); err != nil {
		return settings{}, err
	}
	configBytes, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		configBytes = []byte(encoded)
	}
	return parsePluginConfig(configBytes)
}

func (r *runtime) configure(config settings) error {
	config = normalizeSettings(config)
	if err := config.validate(); err != nil {
		return err
	}
	r.commitMu.Lock()
	if r.loadErr != nil {
		r.commitMu.Unlock()
		return fmt.Errorf("load state: %w", r.loadErr)
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.commitMu.Unlock()
		return errors.New("plugin is shut down")
	}
	// Settings saved from the plugin page are the durable source of truth. Host
	// config seeds a new installation but does not erase page changes on reload.
	if _, err := os.Stat(r.store.path); errors.Is(err, os.ErrNotExist) {
		r.state.Settings = config
	}
	if r.state.Quota == nil {
		r.state.Quota = map[string]quotaSnapshot{}
	}
	replay := r.state.Settings.AutoApply && len(r.state.ActivePool) > 0
	err := r.store.save(r.state)
	r.mu.Unlock()
	r.commitMu.Unlock()
	if err != nil {
		if !r.poolLocked429() {
			return fmt.Errorf("save state: %w", err)
		}
		r.recordHistory("429启动保护", 0, 1, "状态重写失败；保留调度拒绝闸与人工恢复端点："+safeError(err))
	}
	r.commitMu.Lock()
	r.recoverRestPauseIntents(context.Background())
	r.commitMu.Unlock()
	if r.poolLocked429() {
		if err := r.enforcePoolLock429(context.Background()); err != nil {
			// A partial projection must not prevent capability registration:
			// otherwise the host falls back to unprotected native scheduling.
			r.recordHistory("429启动保护", 0, 1, "停用投影待重试；保护闸仍生效："+safeError(err))
			_ = r.persistState()
		}
	}
	if replay {
		if err := r.reconcileActivePool(context.Background(), "加载工位重放"); err != nil {
			if !r.poolLocked429() {
				return err
			}
			r.recordHistory("429启动保护", 0, 1, "工位重放待重试；保护闸仍生效："+safeError(err))
			_ = r.persistState()
		}
	}
	r.restartWorker()
	return nil
}

func nextProbeAt(now time.Time, cfg settings) time.Time {
	return now.UTC().Add(cfg.interval())
}

func (r *runtime) restartWorker() {
	r.mu.Lock()
	if r.cancel != nil {
		r.cancel()
	}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	r.done = make(chan struct{})
	done := r.done
	r.mu.Unlock()
	go func() {
		defer close(done)
		for {
			r.mu.Lock()
			cfg := normalizeSettings(r.state.Settings)
			next := nextProbeAt(time.Now(), cfg)
			if cfg.AutoApply {
				r.nextProbe = &next
			} else {
				r.nextProbe = nil
			}
			r.mu.Unlock()
			timer := time.NewTimer(time.Until(next))
			select {
			case <-ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
				_ = r.scheduledInspection(ctx, cfg.AutoApply)
			}
		}
	}()
}

func (r *runtime) scheduledInspection(ctx context.Context, autoApply bool) error {
	if !autoApply && r.poolLocked429() {
		return r.enforcePoolLock429(ctx)
	}
	// The passive breaker owns recovery independently of tier automation.
	// Release its one-shot disable before potentially slow upstream probes.
	if err := r.reconcileManagedRest(ctx); err != nil {
		return err
	}
	if autoApply {
		_, err := r.run(ctx, true, "自动调度")
		return err
	}
	return nil
}

func (r *runtime) shutdown() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return
	}
	r.closed = true
	cancel := r.cancel
	done := r.done
	r.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-time.After(3 * time.Second):
		}
	}
}

func registrationResult() map[string]any {
	return map[string]any{
		"schema_version": 1,
		"metadata": map[string]any{
			"Name":             "Credential Tiers",
			"Version":          pluginVersion,
			"Author":           "Florious95",
			"GitHubRepository": "https://github.com/Florious95/cpa-plugin-credential-tier-router",
			"Description":      "Routes Codex and Antigravity credentials through clear primary, regular, backup, and paused tiers.",
			"ConfigFields":     []any{},
		},
		"capabilities": map[string]bool{"management_api": true, "usage_plugin": true, "scheduler": true},
	}
}

func managementRegistration() map[string]any {
	return map[string]any{
		"routes": []map[string]string{
			{"Method": "GET", "Path": "/plugins/" + pluginID + "/state"},
			{"Method": "POST", "Path": "/plugins/" + pluginID + "/preview"},
			{"Method": "POST", "Path": "/plugins/" + pluginID + "/apply"},
			{"Method": "POST", "Path": "/plugins/" + pluginID + "/pool/restore"},
			{"Method": "PUT", "Path": "/plugins/" + pluginID + "/settings"},
		},
		"resources": []map[string]string{{"Path": "/status", "Menu": "Credential Tiers", "Description": "Manage Codex and Antigravity credential tiers."}},
	}
}

func (r *runtime) dashboard(ctx context.Context) (dashboardState, error) {
	r.mu.Lock()
	latest := r.latest
	latest.Credentials = append([]credentialState(nil), latest.Credentials...)
	cfg := r.state.Settings
	history := append([]historyEntry(nil), r.state.History...)
	next := r.nextProbe
	locked := r.state.PoolLocked429 || r.pool429Pending
	since, generation := r.state.Pool429Since, r.state.Pool429Generation
	r.mu.Unlock()
	if locked || len(latest.Credentials) == 0 {
		var err error
		latest, err = r.inspect(ctx)
		if err != nil {
			return dashboardState{}, err
		}
	}
	status := "ready"
	if locked {
		status = "pool_locked_429"
	}
	return dashboardState{PoolLocked429: locked, Pool429Since: since, Pool429Generation: generation, PluginStatus: status, Settings: cfg, Plan: latest, History: history, NextProbeAt: next}, nil
}

func (r *runtime) inspect(ctx context.Context) (plan, error) {
	files, err := r.host.listAuth(ctx)
	if err != nil {
		return plan{}, err
	}
	r.mu.Lock()
	cfg := r.state.Settings
	quotaCache := cloneQuota(r.state.Quota)
	locked := r.state.PoolLocked429 || r.pool429Pending
	r.mu.Unlock()
	selectedProviders := providerSet(cfg.Providers)
	credentials := make([]credentialState, 0)
	for _, file := range files {
		if !managedAuthFile(file) {
			continue
		}
		provider := providerOf(file)
		if provider == "" || (!selectedProviders[provider] && !(locked && provider == "antigravity")) {
			continue
		}
		current := tierFromPriority(file.Priority, file.Disabled)
		quota := quotaCache[file.AuthIndex]
		if quota.Status == "" {
			quota.Status = quotaUnknown
		}
		reason := "等待额度探测"
		if quota.ManagedRest && quota.RestUntil != nil && quota.RestUntil.After(time.Now()) {
			reason = fmt.Sprintf("强制休眠至 %s", quota.RestUntil.UTC().Format(time.RFC3339))
		}
		if locked && provider == "antigravity" {
			reason = "429 全池熔断：等待人工恢复，不会自动解封"
		}
		proposed, disabled := current, file.Disabled
		if locked && provider == "antigravity" {
			proposed, disabled = tierPaused, true
		}
		credentials = append(credentials, credentialState{
			Provider: provider, Account: maskAccount(firstText(file.Email, file.Account, file.Name)), AuthIndex: file.AuthIndex,
			CurrentTier: current, ProposedTier: proposed, Reason: reason, Quota: quota,
			Disabled: file.Disabled, ProposedDisabled: disabled, Unavailable: file.Unavailable,
		})
	}
	sortCredentials(credentials)
	return plan{GeneratedAt: time.Now().UTC(), Strategy: cfg.Strategy, Credentials: credentials}, nil
}

func (r *runtime) run(ctx context.Context, apply bool, trigger string) (plan, error) {
	if r.loadErr != nil {
		return plan{}, fmt.Errorf("load state: %w", r.loadErr)
	}
	if !r.runMu.TryLock() {
		return plan{}, errors.New("已有一轮探测正在执行")
	}
	defer r.runMu.Unlock()
	if r.poolLocked429() {
		if err := r.enforcePoolLock429(ctx); err != nil {
			return plan{}, err
		}
	}
	files, err := r.host.listAuth(ctx)
	if err != nil {
		return plan{}, err
	}
	r.mu.Lock()
	cfg := r.state.Settings
	cache := cloneQuota(r.state.Quota)
	r.mu.Unlock()
	providers := providerSet(cfg.Providers)
	observations := make(map[string]quotaSnapshot)
	for _, file := range files {
		if !managedAuthFile(file) || !providers[providerOf(file)] || (providerOf(file) == "antigravity" && r.poolLocked429()) {
			continue
		}
		now := time.Now().UTC()
		quota, err := probeCredential(ctx, r.host, file, cfg, now)
		if err != nil {
			quota = failedQuota(cache[file.AuthIndex], err, cfg.FailureThreshold, now)
		}
		if err := ctx.Err(); err != nil {
			return plan{}, err
		}
		observations[file.AuthIndex] = quota
	}
	r.commitMu.Lock()
	defer r.commitMu.Unlock()
	if err := ctx.Err(); err != nil {
		return plan{}, err
	}
	return r.commitPool(ctx, observations, apply, trigger)
}

func failedQuota(previous quotaSnapshot, probeErr error, threshold int, now time.Time) quotaSnapshot {
	failCount := previous.FailCount + 1
	message := safeError(probeErr)
	if previous.Remaining != nil && failCount < threshold {
		previous.Status = quotaCached
		previous.FailCount = failCount
		previous.LastError = message
		return previous
	}
	status := quotaRetry
	if failCount >= threshold {
		status = quotaUnknown
	}
	return quotaSnapshot{
		ObservedAt:   now,
		Status:       status,
		FailCount:    failCount,
		LastError:    message,
		RestUntil:    previous.RestUntil,
		ManagedRest:  previous.ManagedRest,
		NeedsUnpause: previous.NeedsUnpause,
		PauseID:      previous.PauseID,
	}
}

func chooseTier(cfg settings, file authFile, current tierName, quota quotaSnapshot, now time.Time) (tierName, string) {
	if providerOf(file) == "antigravity" && quota.ManagedRest && quota.RestUntil != nil && quota.RestUntil.After(now) {
		return tierPaused, fmt.Sprintf("强制休眠至 %s", quota.RestUntil.UTC().Format(time.RFC3339))
	}
	if file.Disabled {
		return tierPaused, "凭证已由外部停用，不自动恢复"
	}
	if providerOf(file) == "antigravity" && quota.RestUntil != nil && quota.RestUntil.After(now) {
		return tierPaused, fmt.Sprintf("强制休眠至 %s", quota.RestUntil.UTC().Format(time.RFC3339))
	}
	if file.Unavailable {
		return current, "凭证当前不可用，保持原层级"
	}
	if cfg.Strategy == strategyManual && cfg.ManualTiers[file.AuthIndex] == tierPaused {
		return tierPaused, "手动暂停"
	}
	if quota.Status == quotaUnknown || quota.Status == quotaRetry || quota.Remaining == nil {
		return current, "额度暂时未知，保持原层级"
	}
	remaining := *quota.Remaining
	if remaining <= 0 {
		return tierPaused, "额度已耗尽，等待重置"
	}
	switch cfg.Strategy {
	case strategyRotate:
		return tierRegular, "健康凭证同层轮换"
	case strategyReset:
		if quota.ResetAt != nil && quota.ResetAt.After(now) && quota.ResetAt.Sub(now) <= 24*time.Hour {
			return tierPrimary, "24 小时内重置且仍有余额"
		}
		return tierRegular, "健康凭证常规使用"
	case strategyManual:
		if selected, ok := cfg.ManualTiers[file.AuthIndex]; ok {
			return selected, "手动设置"
		}
		return current, "尚未手动调整"
	default:
		switch {
		case remaining >= 50:
			return tierPrimary, "剩余额度不少于 50%"
		case remaining >= 20:
			return tierRegular, "剩余额度为 20%–49%"
		default:
			return tierBackup, "剩余额度为 1%–19%"
		}
	}
}

func (r *runtime) applyPlan(ctx context.Context, files []authFile, result plan) error {
	return r.applyPlanWithPoolLock(ctx, files, result, false)
}

// Explicit restore retains the routing lock and consumes rest permissions in
// the final state commit. Its successful writes remove each pause ID at once.
func (r *runtime) applyPlanWithPoolLock(ctx context.Context, files []authFile, result plan, allowLocked bool) error {
	byIndex := make(map[string]authFile, len(files))
	for _, file := range files {
		byIndex[file.AuthIndex] = file
	}
	ordered := append([]credentialState(nil), result.Credentials...)
	// Retire every outgoing projection before any admission. A failed retire
	// aborts promotion rather than temporarily exceeding the occupied seats.
	sort.SliceStable(ordered, func(i, j int) bool {
		return ordered[i].ProposedTier != tierPrimary && ordered[j].ProposedTier == tierPrimary
	})
	for _, credential := range ordered {
		file := byIndex[credential.AuthIndex]
		if !allowLocked && providerOf(file) == "antigravity" {
			r.mu.Lock()
			locked, pending := r.state.PoolLocked429, r.pool429Pending
			r.mu.Unlock()
			if pending && !locked {
				continue // Preserve original flags until the trip captures ownership.
			}
			if locked {
				credential.ProposedTier, credential.ProposedDisabled = tierPaused, true
				credential.Changed = file.Priority != -1 || !file.Disabled
			}
		}
		if !credential.Changed {
			continue
		}
		document, err := r.host.getAuth(ctx, credential.AuthIndex)
		if err != nil {
			return err
		}
		var root map[string]any
		if err := json.Unmarshal(document.JSON, &root); err != nil {
			return err
		}
		if root == nil {
			return fmt.Errorf("credential %s document is not a JSON object", credential.AuthIndex)
		}
		currentDisabled, isBoolean := root["disabled"].(bool)
		if root["disabled"] != nil && !isBoolean {
			return fmt.Errorf("credential %s disabled field is not boolean", credential.AuthIndex)
		}
		if !credential.ProposedDisabled && !file.Disabled && currentDisabled {
			return fmt.Errorf("credential %s was disabled after planning", credential.AuthIndex)
		}
		consume := !credential.ProposedDisabled && (credential.Quota.NeedsUnpause || file.RestPauseMarker != "")
		if !credential.ProposedDisabled && currentDisabled && (!allowLocked || consume) && !ownsRestDisable(root, file.RestPauseMarker) {
			err := fmt.Errorf("credential %s no longer has its managed-rest pause marker", credential.AuthIndex)
			if !allowLocked {
				return errors.Join(err, r.consumeUnpause(credential.AuthIndex))
			}
			return err // Preserve the mismatch for the next restore planner to exclude.
		}
		root["priority"] = credential.ProposedTier.priority()
		// Enablement and tier are independent. Only the usage-event transaction
		// hard-disables a managed geo rest; inspections retain priority=-1.
		root["disabled"] = credential.ProposedDisabled
		if !credential.ProposedDisabled {
			delete(root, "cpa_pause_id")
		}
		root["credential_tier_router"] = map[string]any{
			"managed": true, "tier": credential.ProposedTier, "updated_at": time.Now().UTC().Format(time.RFC3339),
		}
		updated, err := json.Marshal(root)
		if err != nil {
			return err
		}
		if consume && !allowLocked {
			// Spend the permission durably first. A failed auth write must
			// leave the credential disabled, never leave stale enable rights.
			if err := r.consumeUnpause(credential.AuthIndex); err != nil {
				return err
			}
		}
		if err := r.host.saveAuth(ctx, firstText(document.Name, file.Name), updated); err != nil {
			return err
		}
	}
	return nil
}

// Call under commitMu, before applying an enable projection. On a state
// write failure the caller must not write the credential.
func (r *runtime) consumeUnpause(index string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.state.Quota[index]
	if !q.NeedsUnpause {
		return nil
	}
	priorID := q.PauseID
	q.NeedsUnpause, q.PauseID = false, ""
	r.state.Quota[index] = q
	if err := r.store.save(r.state); err != nil {
		q.NeedsUnpause, q.PauseID = true, priorID
		r.state.Quota[index] = q
		return fmt.Errorf("persist consumed rest permission for %s: %w", index, err)
	}
	return nil
}

func (r *runtime) updateSettings(next settings) error {
	if r.loadErr != nil {
		return fmt.Errorf("load state: %w", r.loadErr)
	}
	next = normalizeSettings(next)
	if err := next.validate(); err != nil {
		return err
	}
	r.commitMu.Lock()
	r.mu.Lock()
	previous := r.state.Settings
	policy := next
	policy.PoolLock429Enabled = previous.PoolLock429Enabled
	protectionOnly := previous.PoolLock429Enabled != next.PoolLock429Enabled && reflect.DeepEqual(previous, policy)
	r.state.Settings = next
	err := r.store.save(r.state)
	if err != nil {
		r.state.Settings = previous
	} else {
		r.latest = plan{}
	}
	r.mu.Unlock()
	r.commitMu.Unlock()
	if err != nil {
		return err
	}
	if protectionOnly {
		return nil // The usage gate changes immediately; do not rebalance or reset its worker timer.
	}
	if next.AutoApply && next.ActivePoolSize > 0 {
		if err := r.reconcileActivePool(context.Background(), "设置工位调整"); err != nil {
			return err
		}
	}
	r.restartWorker()
	return nil
}

func (r *runtime) recordHistory(trigger string, changes, errorsCount int, summary string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := historyEntry{At: time.Now().UTC(), Trigger: trigger, Summary: summary, Changes: changes, Errors: errorsCount}
	r.state.History = append([]historyEntry{entry}, r.state.History...)
	if len(r.state.History) > 12 {
		r.state.History = r.state.History[:12]
	}
}

func historySummary(result plan) string {
	if result.Changes == 0 {
		return "本轮探测完成，没有需要调整的凭证"
	}
	return fmt.Sprintf("本轮调整 %d 个凭证；新的优先级主要影响后续请求", result.Changes)
}

func safeError(err error) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Error())
	message = strings.ReplaceAll(message, "Bearer ", "Bearer [REDACTED]")
	if len(message) > 180 {
		message = message[:180]
	}
	return message
}

func cloneQuota(source map[string]quotaSnapshot) map[string]quotaSnapshot {
	out := make(map[string]quotaSnapshot, len(source))
	for key, value := range source {
		out[key] = value
	}
	return out
}

func providerSet(providers []string) map[string]bool {
	out := map[string]bool{}
	for _, provider := range providers {
		out[provider] = true
	}
	return out
}

func maskAccount(account string) string {
	account = strings.TrimSpace(account)
	if at := strings.Index(account, "@"); at > 1 {
		name := account[:at]
		domain := account[at:]
		if len(name) > 2 {
			name = name[:2] + strings.Repeat("•", min(5, len(name)-2))
		}
		return name + domain
	}
	if len(account) <= 5 {
		return account
	}
	return account[:3] + "•••" + account[len(account)-2:]
}

func sortCredentials(credentials []credentialState) {
	sort.SliceStable(credentials, func(i, j int) bool {
		if credentials[i].Provider != credentials[j].Provider {
			return credentials[i].Provider < credentials[j].Provider
		}
		return credentials[i].Account < credentials[j].Account
	})
}

func success(result any) []byte {
	raw, err := json.Marshal(result)
	if err != nil {
		return failure("internal_error", err.Error(), false)
	}
	envelope, _ := json.Marshal(map[string]any{"ok": true, "result": json.RawMessage(raw)})
	return envelope
}

func failure(code, message string, retryable bool) []byte {
	raw, _ := json.Marshal(map[string]any{"ok": false, "error": map[string]any{"code": code, "message": message, "retryable": retryable}})
	return raw
}
