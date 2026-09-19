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

func TestCountdownFormatSupportsMinutePrecision(t *testing.T) {
	app := readWebAssetForTest(t, "web/app.js")
	for _, snippet := range []string{
		"Math.ceil(delta/60000)",
		"+' 分钟后'",
		"Math.round(delta/3600000)",
		"+' 小时后'",
	} {
		if !strings.Contains(app, snippet) {
			t.Errorf("app.js missing minute-precision countdown marker %q", snippet)
		}
	}
}

func TestPreviewRefreshesNextProbeMetadata(t *testing.T) {
	app := readWebAssetForTest(t, "web/app.js")
	for _, snippet := range []string{
		"current.plan=await api('/preview',{method:'POST'});var refreshed=await api('/state')",
		"current.next_probe_at=refreshed.next_probe_at",
	} {
		if !strings.Contains(app, snippet) {
			t.Errorf("preview path missing refreshed scheduler metadata marker %q", snippet)
		}
	}
}

func TestPreviewUsesPostMethod(t *testing.T) {
	app := readWebAssetForTest(t, "web/app.js")
	if !strings.Contains(app, "current.plan=await api('/preview',{method:'POST'})") {
		t.Fatal("preview must call the POST /preview management route")
	}
}

func TestManagementAPIReportsNonJSONBodiesWithHTTPContext(t *testing.T) {
	app := readWebAssetForTest(t, "web/app.js")
	for _, snippet := range []string{
		"var body=await response.text()",
		"JSON.parse(body)",
		"CPA 返回了无效响应（HTTP '+response.status+')",
	} {
		if !strings.Contains(app, snippet) {
			t.Errorf("app.js missing non-JSON response handling marker %q", snippet)
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
