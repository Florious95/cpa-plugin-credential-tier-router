package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

type runtime struct {
	host hostAPI

	mu          sync.Mutex
	runMu       sync.Mutex
	state       persistedState
	latest      plan
	store       stateStore
	cancel      context.CancelFunc
	done        chan struct{}
	nextProbe   *time.Time
	geoMu       sync.Mutex
	geoEvents   map[string]time.Time
	geoAccounts map[string]time.Time
	geoEgressAt time.Time
	geoNow      func() time.Time

	// quotaRevision prevents an in-flight probe from overwriting a newer
	// passive geo-400 pause.
	quotaRevision uint64
	egressRetryAt *time.Time
	egressWG      sync.WaitGroup
	wake          chan struct{}
	closed        bool
}

func newRuntime(host hostAPI) *runtime {
	statePath := os.Getenv("CREDENTIAL_TIER_ROUTER_STATE_PATH")
	if statePath == "" {
		statePath = filepath.Join("credential-tier-router", "state.json")
	}
	r := &runtime{
		host:        host,
		store:       stateStore{path: statePath},
		done:        make(chan struct{}),
		geoEvents:   make(map[string]time.Time),
		geoAccounts: make(map[string]time.Time),
		wake:        make(chan struct{}, 1),
		geoNow:      time.Now,
	}
	if state, err := r.store.load(); err == nil {
		r.state = state
		r.state.Settings = normalizeSettings(r.state.Settings)
	} else {
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
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
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
	state := r.state
	r.mu.Unlock()
	if err := r.store.save(state); err != nil {
		return fmt.Errorf("save state: %w", err)
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
			returnAt := r.state.EgressReturnAt
			retryAt := r.egressRetryAt
			r.mu.Unlock()

			var probeTimer *time.Timer
			var probeC <-chan time.Time
			if cfg.AutoApply {
				next := nextProbeAt(time.Now(), cfg)
				r.mu.Lock()
				r.nextProbe = &next
				r.mu.Unlock()
				probeTimer = time.NewTimer(time.Until(next))
				probeC = probeTimer.C
			} else {
				r.mu.Lock()
				r.nextProbe = nil
				r.mu.Unlock()
			}

			var returnTimer *time.Timer
			var returnC <-chan time.Time
			if returnAt != nil {
				due := *returnAt
				if retryAt != nil && retryAt.After(due) {
					due = *retryAt
				}
				delay := time.Until(due)
				if delay < 0 {
					delay = 0
				}
				returnTimer = time.NewTimer(delay)
				returnC = returnTimer.C
			}
			select {
			case <-ctx.Done():
				if probeTimer != nil {
					probeTimer.Stop()
				}
				if returnTimer != nil {
					returnTimer.Stop()
				}
				return
			case <-r.wake:
				if probeTimer != nil {
					probeTimer.Stop()
				}
				if returnTimer != nil {
					returnTimer.Stop()
				}
			case <-returnC:
				if probeTimer != nil {
					probeTimer.Stop()
				}
				_ = r.maybeReturnEgress(context.Background(), time.Now().UTC())
			case <-probeC:
				if returnTimer != nil {
					returnTimer.Stop()
				}
				_, _ = r.run(context.Background(), true, "自动调度")
			}
		}
	}()
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
	r.egressWG.Wait()
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
		"capabilities": map[string]bool{"management_api": true, "usage_plugin": true},
	}
}

func managementRegistration() map[string]any {
	return map[string]any{
		"routes": []map[string]string{
			{"Method": "GET", "Path": "/plugins/" + pluginID + "/state"},
			{"Method": "POST", "Path": "/plugins/" + pluginID + "/preview"},
			{"Method": "POST", "Path": "/plugins/" + pluginID + "/apply"},
			{"Method": "PUT", "Path": "/plugins/" + pluginID + "/settings"},
			{"Method": "POST", "Path": "/plugins/" + pluginID + "/egress/return"},
		},
		"resources": []map[string]string{{"Path": "/status", "Menu": "Credential Tiers", "Description": "Manage Codex and Antigravity credential tiers."}},
	}
}

func (r *runtime) dashboard(ctx context.Context) (dashboardState, error) {
	r.mu.Lock()
	latest := r.latest
	cfg := r.state.Settings
	history := append([]historyEntry(nil), r.state.History...)
	next := r.nextProbe
	returnAt := r.state.EgressReturnAt
	r.mu.Unlock()
	if len(latest.Credentials) == 0 {
		var err error
		latest, err = r.inspect(ctx)
		if err != nil {
			return dashboardState{}, err
		}
	}
	return dashboardState{PluginStatus: "ready", Settings: cfg, Plan: latest, History: history, NextProbeAt: next, EgressReturnAt: returnAt}, nil
}

func (r *runtime) inspect(ctx context.Context) (plan, error) {
	files, err := r.host.listAuth(ctx)
	if err != nil {
		return plan{}, err
	}
	r.mu.Lock()
	cfg := r.state.Settings
	quotaCache := cloneQuota(r.state.Quota)
	r.mu.Unlock()
	selectedProviders := providerSet(cfg.Providers)
	credentials := make([]credentialState, 0)
	for _, file := range files {
		if !managedAuthFile(file) {
			continue
		}
		provider := providerOf(file)
		if provider == "" || !selectedProviders[provider] {
			continue
		}
		current := tierFromPriority(file.Priority, file.Disabled)
		quota := quotaCache[file.AuthIndex]
		if quota.Status == "" {
			quota.Status = quotaUnknown
		}
		credentials = append(credentials, credentialState{
			Provider: provider, Account: maskAccount(firstText(file.Email, file.Account, file.Name)), AuthIndex: file.AuthIndex,
			CurrentTier: current, ProposedTier: current, Reason: "等待额度探测", Quota: quota,
			Disabled: file.Disabled, Unavailable: file.Unavailable,
		})
	}
	sortCredentials(credentials)
	return plan{GeneratedAt: time.Now().UTC(), Strategy: cfg.Strategy, Credentials: credentials}, nil
}

func (r *runtime) run(ctx context.Context, apply bool, trigger string) (plan, error) {
	if !r.runMu.TryLock() {
		return plan{}, errors.New("已有一轮探测正在执行")
	}
	defer r.runMu.Unlock()
	files, err := r.host.listAuth(ctx)
	if err != nil {
		return plan{}, err
	}
	r.mu.Lock()
	cfg := r.state.Settings
	cache := cloneQuota(r.state.Quota)
	startRevision := r.quotaRevision
	r.mu.Unlock()
	selectedProviders := providerSet(cfg.Providers)
	now := time.Now().UTC()
	credentials := make([]credentialState, 0)
	for _, file := range files {
		if !managedAuthFile(file) {
			continue
		}
		provider := providerOf(file)
		if provider == "" || !selectedProviders[provider] {
			continue
		}
		previousQuota := cache[file.AuthIndex]
		restExpired := provider == "antigravity" && previousQuota.ManagedRest && previousQuota.RestUntil != nil && !previousQuota.RestUntil.After(now)
		effectiveDisabled := file.Disabled && !restExpired
		current := tierFromPriority(file.Priority, effectiveDisabled)
		if restExpired && current == tierPaused {
			// Let a recovered managed pause re-enter normal tier calculation;
			// otherwise applyAntigravityRest would immediately start a new rest.
			current = tierRegular
		}
		quota, probeErr := probeCredential(ctx, r.host, file, cfg, now)
		if probeErr != nil {
			quota = failedQuota(previousQuota, probeErr, cfg.FailureThreshold, now)
		}
		inheritAntigravityRest(&quota, previousQuota, now)
		if !effectiveDisabled {
			applyAntigravityRest(&cfg, file, current, &quota, now)
		}
		planningFile := file
		planningFile.Disabled = effectiveDisabled
		proposed, reason := chooseTier(cfg, planningFile, current, quota, now)
		// Manual pauses are a managed rest as well, even when the latest quota
		// is healthy. This gives them the same expiry/recovery semantics.
		if provider == "antigravity" && cfg.Strategy == strategyManual && proposed == tierPaused && quota.RestUntil == nil {
			if applyAntigravityRest(&cfg, file, tierPaused, &quota, now) {
				reason = fmt.Sprintf("手动暂停，休眠至 %s", quota.RestUntil.UTC().Format(time.RFC3339))
			}
		}
		needsPauseWrite := proposed == tierPaused && !file.Disabled && quota.RestUntil != nil && quota.RestUntil.After(now)
		needsRestoreWrite := proposed != tierPaused && file.Disabled && restExpired
		changed := (proposed != current || needsPauseWrite || needsRestoreWrite) && !file.Unavailable
		cache[file.AuthIndex] = quota
		credentials = append(credentials, credentialState{
			Provider: provider, Account: maskAccount(firstText(file.Email, file.Account, file.Name)), AuthIndex: file.AuthIndex,
			CurrentTier: current, ProposedTier: proposed, Reason: reason, Quota: quota,
			Disabled: effectiveDisabled, Unavailable: file.Unavailable, Changed: changed,
		})
	}
	applyActivePoolCap(&cfg, credentials, now)
	sortCredentials(credentials)
	result := plan{GeneratedAt: now, Strategy: cfg.Strategy, Credentials: credentials}
	for _, credential := range credentials {
		if credential.Changed {
			result.Changes++
		}
		if credential.Quota.Status == quotaUnknown {
			result.Unknown++
		}
	}
	if apply {
		// A passive geo event may arrive while an upstream probe is blocked. Do
		// not apply the stale plan over its newer hard pause.
		r.mu.Lock()
		revisionChanged := r.quotaRevision != startRevision
		newQuota := cloneQuota(r.state.Quota)
		r.mu.Unlock()
		if revisionChanged {
			for index := range result.Credentials {
				credential := &result.Credentials[index]
				quota, ok := newQuota[credential.AuthIndex]
				if !ok || !quota.ManagedRest || quota.RestUntil == nil || !quota.RestUntil.After(now) {
					continue
				}
				credential.Quota = quota
				credential.ProposedTier = tierPaused
				credential.Reason = fmt.Sprintf("地区400，休眠至 %s", quota.RestUntil.UTC().Format(time.RFC3339))
				credential.Changed = !credential.Unavailable
			}
			applyActivePoolCap(&cfg, result.Credentials, now)
			result.Changes = 0
			result.Unknown = 0
			for _, credential := range result.Credentials {
				if credential.Changed {
					result.Changes++
				}
				if credential.Quota.Status == quotaUnknown {
					result.Unknown++
				}
			}
		}
		if err := r.applyPlan(ctx, files, result); err != nil {
			r.recordHistory(trigger, result.Changes, 1, "应用失败："+safeError(err))
			return plan{}, err
		}
		r.recordHistory(trigger, result.Changes, result.Unknown, historySummary(result))
	}
	r.mu.Lock()
	// Preserve a newer passive pause committed while this run was probing.
	if r.quotaRevision != startRevision {
		for authIndex, quota := range r.state.Quota {
			if quota.ManagedRest {
				cache[authIndex] = quota
			}
		}
	}
	r.state.Quota = cache
	r.latest = result
	state := r.state
	r.mu.Unlock()
	if err := r.store.save(state); err != nil {
		return plan{}, err
	}
	return result, nil
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
		ObservedAt:  now,
		Status:      status,
		FailCount:   failCount,
		LastError:   message,
		RestUntil:   previous.RestUntil,
		ManagedRest: previous.ManagedRest,
	}
}

func chooseTier(cfg settings, file authFile, current tierName, quota quotaSnapshot, now time.Time) (tierName, string) {
	if file.Disabled {
		return tierPaused, "凭证已由外部停用，不自动恢复"
	}
	if providerOf(file) == "antigravity" && quota.RestUntil != nil && quota.RestUntil.After(now) {
		return tierPaused, fmt.Sprintf("强制休眠至 %s", quota.RestUntil.UTC().Format(time.RFC3339))
	}
	if file.Unavailable {
		return current, "凭证当前不可用，保持原层级"
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
	byIndex := make(map[string]authFile, len(files))
	for _, file := range files {
		byIndex[file.AuthIndex] = file
	}
	for _, credential := range result.Credentials {
		if !credential.Changed {
			continue
		}
		file := byIndex[credential.AuthIndex]
		document, err := r.host.getAuth(ctx, credential.AuthIndex)
		if err != nil {
			return err
		}
		var root map[string]any
		if err := json.Unmarshal(document.JSON, &root); err != nil {
			return err
		}
		root["priority"] = credential.ProposedTier.priority()
		// A managed pause must be runtime-ineligible so CPA can evict any
		// session-affinity binding. Promotion writes the inverse atomically.
		root["disabled"] = credential.ProposedTier == tierPaused
		root["credential_tier_router"] = map[string]any{
			"managed": true, "tier": credential.ProposedTier, "updated_at": time.Now().UTC().Format(time.RFC3339),
		}
		updated, err := json.Marshal(root)
		if err != nil {
			return err
		}
		if err := r.host.saveAuth(ctx, firstText(document.Name, file.Name), updated); err != nil {
			return err
		}
	}
	return nil
}

func (r *runtime) updateSettings(next settings) error {
	next = normalizeSettings(next)
	if err := next.validate(); err != nil {
		return err
	}
	r.mu.Lock()
	r.state.Settings = next
	if next.Geo400ReturnHours == 0 {
		r.state.EgressReturnAt = nil
	}
	r.latest = plan{}
	state := r.state
	r.mu.Unlock()
	if err := r.store.save(state); err != nil {
		return err
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
