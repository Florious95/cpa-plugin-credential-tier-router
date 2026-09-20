package main

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

func TestTiersOnlySettingsCardsAreIndependent(t *testing.T) {
	html := readWebAssetForTest(t, "web/index.html")
	sections := regexp.MustCompile(`(?s)<section\b[^>]*>.*?</section>`).FindAllString(html, -1)
	var first, second string
	for _, section := range sections {
		if strings.Contains(section, `class="panel strategy-section"`) {
			first = section
		}
		if strings.Contains(section, `id="customSettings"`) {
			second = section
		}
	}
	if first == "" || second == "" || strings.Index(html, first) > strings.Index(html, second) {
		t.Fatal("ordered independent configuration cards missing")
	}
	for _, id := range []string{"strategyGrid", "intervalMinutes", "failureThreshold", "antigravityGroup"} {
		if !strings.Contains(first, `id="`+id+`"`) || strings.Contains(second, `id="`+id+`"`) {
			t.Fatalf("base control %s is not exclusive to first card", id)
		}
	}
	for _, id := range []string{"activePoolSize", "restDurationHours", "geo400RestHours"} {
		if strings.Contains(first, `id="`+id+`"`) || !strings.Contains(second, `id="`+id+`"`) {
			t.Fatalf("custom control %s is not exclusive to second card", id)
		}
	}
}

func TestTiersOnlyUIBehavior(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("Node is needed for the optional JS behavioral test")
	}
	const script = `
const fs=require('fs'),vm=require('vm'),assert=require('assert');
const html=fs.readFileSync('web/index.html','utf8'), nodes=new Map();
for(const m of html.matchAll(/\bid="([^"]+)"/g))nodes.set(m[1],{value:'',checked:false,dataset:{},classList:{add(){},remove(){}},setAttribute(){},addEventListener(){}});
for(const id of ['providerFilter','tierFilter','healthFilter'])nodes.get(id).value='all';
const providers=['codex','antigravity'].map(value=>({value,checked:true}));
const document={getElementById(id){assert(nodes.has(id),'missing DOM control: '+id);return nodes.get(id)},querySelectorAll(selector){return selector.includes('.provider-choice')?providers:[]}};
let source=fs.readFileSync('web/app.js','utf8');
source=source.replace('initTheme();bind();if(demo){load()}else{connectFromCPA()}', 'globalThis.ui={readSettings,render,renderRows,setCurrent(x){current=x}};bind()');
const sandbox={document,URLSearchParams,location:{search:''},setInterval(){},console};
vm.runInNewContext(source,sandbox);
const state={settings:{auto_apply:false,strategy:'quota_bands',active_pool_size:6,interval_minutes:30,providers:['codex','antigravity'],antigravity_group:'gemini',failure_threshold:5,rest_duration_hours:20,geo400_rest_hours:3,manual_tiers:{}},plan:{credentials:[],changes:0,unknown:0},history:[]};
sandbox.ui.setCurrent(state);sandbox.ui.render();
assert.equal(nodes.get('activePoolSize').value,'6');
assert.equal(nodes.get('restDurationHours').value,'20');
assert.equal(nodes.get('geo400RestHours').value,'3');
nodes.get('activePoolSize').value='2';nodes.get('restDurationHours').value='16';nodes.get('geo400RestHours').value='2';
const saved=JSON.parse(JSON.stringify(sandbox.ui.readSettings()));
assert.deepEqual(saved,{...state.settings,active_pool_size:2,rest_duration_hours:16,geo400_rest_hours:2});
assert(!JSON.stringify(saved).toLowerCase().includes('egress'));
for(const status of ['unknown','retrying','cached','ready']) {
 state.plan.credentials=[{auth_index:'a',account:'test',provider:'antigravity',current_tier:'paused',proposed_tier:'paused',disabled:false,reason:'凭证已由外部停用，不自动恢复',quota:{status,managed_rest:true,rest_until:new Date(Date.now()+30*60000).toISOString()}}];
 sandbox.ui.render();
 const rendered=nodes.get('accountRows').innerHTML;
 assert(rendered.includes('强制休眠至')&&rendered.includes('剩余 30 分钟'),rendered);
 assert(!rendered.includes('外部停用'),rendered);
 assert.equal(nodes.get('activeMetric').textContent,0);
}
console.log('UI card bindings, settings round trip, four quota states and countdown: PASS');
`
	output, err := exec.Command(node, "-e", script).CombinedOutput()
	if err != nil {
		t.Fatalf("UI behavior: %v\n%s", err, output)
	}
	t.Log(strings.TrimSpace(string(output)))
}
