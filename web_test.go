package main

import (
	"os"
	"strings"
	"testing"
)

func readWebAssetForTest(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return string(data)
}

func TestSettingsPanelExposesRestAndEgressControls(t *testing.T) {
	html := readWebAssetForTest(t, "web/index.html")
	for _, control := range []string{
		`id="restDurationHours" type="number" min="1" step="1" value="16"`,
		`id="egressCommand" type="text" value="/usr/local/bin/cpa-egress-cycle"`,
		`id="egressTarget" type="text" value="to-2.5x"`,
		`id="geo400DebounceMinutes" type="number" min="1" step="1" value="5"`,
	} {
		if !strings.Contains(html, control) {
			t.Errorf("settings panel missing %q", control)
		}
	}
}

func TestSettingsPanelRoundTripsRuntimePolicyFields(t *testing.T) {
	app := readWebAssetForTest(t, "web/app.js")
	for _, snippet := range []string{
		"byId('restDurationHours').value=String(settings.rest_duration_hours||16)",
		"byId('egressCommand').value=settings.egress_command||'/usr/local/bin/cpa-egress-cycle'",
		"byId('egressTarget').value=settings.egress_target||'to-2.5x'",
		"byId('geo400DebounceMinutes').value=String(settings.geo400_debounce_minutes||5)",
		"rest_duration_hours:Math.max(1,Number(byId('restDurationHours').value)||16)",
		"egress_command:byId('egressCommand').value.trim()||'/usr/local/bin/cpa-egress-cycle'",
		"egress_target:byId('egressTarget').value.trim()||'to-2.5x'",
		"geo400_debounce_minutes:Math.max(1,Number(byId('geo400DebounceMinutes').value)||5)",
	} {
		if !strings.Contains(app, snippet) {
			t.Errorf("app.js missing settings round-trip marker %q", snippet)
		}
	}
}
