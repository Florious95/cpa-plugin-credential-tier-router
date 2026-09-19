package main

import "time"

type tierName string

const (
	tierPrimary tierName = "primary"
	tierRegular tierName = "regular"
	tierBackup  tierName = "backup"
	tierPaused  tierName = "paused"
)

func (t tierName) valid() bool {
	switch t {
	case tierPrimary, tierRegular, tierBackup, tierPaused:
		return true
	default:
		return false
	}
}

func (t tierName) priority() int {
	switch t {
	case tierPrimary:
		return 400
	case tierRegular:
		return 300
	case tierBackup:
		return 200
	case tierPaused:
		return -1
	default:
		return 300
	}
}

func tierFromPriority(priority int, disabled bool) tierName {
	if disabled || priority < 0 {
		return tierPaused
	}
	switch {
	case priority >= 400:
		return tierPrimary
	case priority >= 300:
		return tierRegular
	case priority >= 200:
		return tierBackup
	default:
		return tierRegular
	}
}

type quotaStatus string

const (
	quotaReady   quotaStatus = "ready"
	quotaCached  quotaStatus = "cached"
	quotaRetry   quotaStatus = "retrying"
	quotaUnknown quotaStatus = "unknown"
)

type quotaSnapshot struct {
	Remaining   *int        `json:"remaining,omitempty"`
	ResetAt     *time.Time  `json:"reset_at,omitempty"`
	RestUntil   *time.Time  `json:"rest_until,omitempty"`
	ManagedRest bool        `json:"managed_rest,omitempty"`
	ObservedAt  time.Time   `json:"observed_at"`
	Status      quotaStatus `json:"status"`
	FailCount   int         `json:"fail_count,omitempty"`
	LastError   string      `json:"last_error,omitempty"`
}

type credentialState struct {
	Provider     string        `json:"provider"`
	Account      string        `json:"account"`
	AuthIndex    string        `json:"auth_index"`
	CurrentTier  tierName      `json:"current_tier"`
	ProposedTier tierName      `json:"proposed_tier"`
	Reason       string        `json:"reason"`
	Quota        quotaSnapshot `json:"quota"`
	Disabled     bool          `json:"disabled"`
	Unavailable  bool          `json:"unavailable"`
	Changed      bool          `json:"changed"`
}

type plan struct {
	GeneratedAt time.Time         `json:"generated_at"`
	Strategy    strategyName      `json:"strategy"`
	Credentials []credentialState `json:"credentials"`
	Changes     int               `json:"changes"`
	Unknown     int               `json:"unknown"`
}

type historyEntry struct {
	At      time.Time `json:"at"`
	Trigger string    `json:"trigger"`
	Summary string    `json:"summary"`
	Changes int       `json:"changes"`
	Errors  int       `json:"errors"`
}

// Membership is authoritative; auth-file priorities are its recoverable projection.
// A present provider with an empty Members list is initialized, not a bootstrap.
type activePoolState struct {
	Members    []string `json:"members"`
	Generation uint64   `json:"generation"`
}

type persistedState struct {
	ActivePool     map[string]activePoolState `json:"active_pool,omitempty"`
	Settings       settings                   `json:"settings"`
	Quota          map[string]quotaSnapshot   `json:"quota"`
	History        []historyEntry             `json:"history"`
	EgressReturnAt *time.Time                 `json:"egress_return_at,omitempty"`
}

type dashboardState struct {
	PluginStatus   string         `json:"plugin_status"`
	Settings       settings       `json:"settings"`
	Plan           plan           `json:"plan"`
	History        []historyEntry `json:"history"`
	NextProbeAt    *time.Time     `json:"next_probe_at,omitempty"`
	EgressReturnAt *time.Time     `json:"egress_return_at,omitempty"`
}

type usageEvent struct {
	Provider  string       `json:"Provider"`
	AuthID    string       `json:"AuthID"`
	AuthIndex string       `json:"AuthIndex"`
	Model     string       `json:"Model"`
	Failed    bool         `json:"Failed"`
	Failure   usageFailure `json:"Failure"`
}

type usageFailure struct {
	StatusCode int    `json:"StatusCode"`
	Body       string `json:"Body"`
}

type egressInvocation struct {
	Command string
	Args    []string
}
