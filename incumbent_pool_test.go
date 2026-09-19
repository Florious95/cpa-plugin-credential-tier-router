package main

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func incumbentCredential(index string, current tierName, remaining int, now time.Time) credentialState {
	return credentialState{
		Provider:     "antigravity",
		AuthIndex:    index,
		CurrentTier:  current,
		ProposedTier: current,
		Quota:        readyQuota(remaining, nil, now),
	}
}

func TestActivePoolCapProtectsHealthyIncumbents(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	cfg := defaultSettings()
	cfg.ActivePoolSize = 4
	credentials := []credentialState{
		incumbentCredential("a", tierPrimary, 100, now),
		incumbentCredential("b", tierPrimary, 100, now),
		incumbentCredential("c", tierPrimary, 100, now),
		incumbentCredential("d", tierPrimary, 100, now),
		incumbentCredential("e", tierBackup, 100, now),
		incumbentCredential("f", tierBackup, 100, now),
		incumbentCredential("g", tierBackup, 100, now),
		incumbentCredential("h", tierBackup, 100, now),
	}
	for _, remaining := range []int{80, 50, 20} {
		for i := range credentials {
			credentials[i].ProposedTier = credentials[i].CurrentTier
			credentials[i].Changed = false
		}
		for i := 0; i < 4; i++ {
			credentials[i].Quota = readyQuota(remaining, nil, now)
		}
		applyActivePoolCap(&cfg, credentials, now)
		for i := 0; i < 4; i++ {
			if credentials[i].ProposedTier != tierPrimary || credentials[i].Changed {
				t.Fatalf("incumbent %s was preempted at %d%%: proposed=%s changed=%v", credentials[i].AuthIndex, remaining, credentials[i].ProposedTier, credentials[i].Changed)
			}
		}
		for i := 4; i < len(credentials); i++ {
			if credentials[i].ProposedTier != tierBackup {
				t.Fatalf("reserve %s stole a protected slot at %d%%: proposed=%s", credentials[i].AuthIndex, remaining, credentials[i].ProposedTier)
			}
		}
	}
}

func TestActivePoolCapProtectsIncumbentOnTransientProbeFailure(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	cfg := defaultSettings()
	cfg.ActivePoolSize = 4
	failed := failedQuota(quotaSnapshot{}, errors.New("temporary quota probe failure"), cfg.FailureThreshold, now)
	if failed.Status != quotaRetry || failed.Remaining != nil {
		t.Fatalf("first probe failure=%+v, want retry with unknown remaining", failed)
	}
	credentials := []credentialState{
		{
			Provider: "antigravity", AuthIndex: "shanavask", CurrentTier: tierPrimary,
			ProposedTier: tierPrimary, Quota: failed,
		},
	}
	for i, remaining := range []int{95, 90, 85, 80} {
		credentials = append(credentials, credentialState{
			Provider: "antigravity", AuthIndex: fmt.Sprintf("reserve-%d", i),
			CurrentTier: tierBackup, ProposedTier: tierBackup,
			Quota: readyQuota(remaining, nil, now),
		})
	}

	applyActivePoolCap(&cfg, credentials, now)
	if got := credentials[0].ProposedTier; got != tierPrimary || credentials[0].Changed {
		t.Fatalf("transiently unprobed incumbent was demoted: proposed=%s changed=%v reason=%s", got, credentials[0].Changed, credentials[0].Reason)
	}
	for index, credential := range credentials[1:] {
		want := tierPrimary
		if index == cfg.ActivePoolSize-1 {
			want = tierBackup
		}
		if credential.ProposedTier != want {
			t.Fatalf("reserve %s tier=%s, want %s", credential.AuthIndex, credential.ProposedTier, want)
		}
	}
}

func TestActivePoolCapFillsOnlyVacantIncumbentSlot(t *testing.T) {
	now := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	cfg := defaultSettings()
	cfg.ActivePoolSize = 4
	credentials := []credentialState{
		incumbentCredential("a", tierPrimary, 80, now),
		incumbentCredential("b", tierPrimary, 70, now),
		incumbentCredential("c", tierPrimary, 0, now),
		incumbentCredential("d", tierPrimary, 50, now),
		incumbentCredential("e", tierBackup, 100, now),
		incumbentCredential("f", tierBackup, 100, now),
		incumbentCredential("g", tierBackup, 100, now),
	}
	credentials[2].ProposedTier = tierPaused
	applyActivePoolCap(&cfg, credentials, now)
	for _, index := range []int{0, 1, 3, 4} {
		if credentials[index].ProposedTier != tierPrimary {
			t.Fatalf("expected %s to occupy primary after one vacancy, got %s", credentials[index].AuthIndex, credentials[index].ProposedTier)
		}
	}
	if credentials[2].ProposedTier != tierPaused {
		t.Fatalf("exhausted incumbent was not paused: %s", credentials[2].ProposedTier)
	}
	for _, index := range []int{5, 6} {
		if credentials[index].ProposedTier != tierBackup {
			t.Fatalf("reserve %s was promoted despite no vacancy: %s", credentials[index].AuthIndex, credentials[index].ProposedTier)
		}
	}
}
