package main

import (
	"encoding/base64"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestCPAMC1274EncV2ManagementKeyDecode(t *testing.T) {
	const (
		host      = "api-gpt2.team-agent.net"
		plaintext = `{"state":{"managementKey":"cpamc-v1274-test-key"}}`
		keyPrefix = "cli-proxy-api-webui::secure-storage|"
	)
	key := []byte(keyPrefix + host)
	encoded := make([]byte, len(plaintext))
	for i, b := range []byte(plaintext) {
		encoded[i] = b ^ key[i%len(key)]
	}
	sample := "enc::v2::" + base64.StdEncoding.EncodeToString(encoded)

	harness := filepath.Join(t.TempDir(), "decode-enc-v2.js")
	if err := os.WriteFile(harness, []byte(`
const fs = require('node:fs');
const vm = require('node:vm');
const source = fs.readFileSync('web/app.js', 'utf8');
const start = source.indexOf('function xorBytes');
const end = source.indexOf('function readCPAKey');
if (start < 0 || end < 0) throw new Error('web decoder helpers are unavailable');
const helpers = source.slice(start, end);
const decode = vm.runInNewContext('(function(){' + helpers + ';return decodeCPAStorage;})()', {
  atob: value => Buffer.from(value, 'base64').toString('latin1'),
  TextDecoder,
  TextEncoder,
  Uint8Array,
  location: {host: process.env.ENC_V2_HOST},
  navigator: {userAgent: 'UA-that-must-not-affect-v2'},
  window: {location: {host: process.env.ENC_V2_HOST}},
});
const decoded = decode(process.env.ENC_V2_SAMPLE);
if (!decoded || decoded.state?.managementKey !== 'cpamc-v1274-test-key') {
  throw new Error('enc::v2 did not decode to the expected management key');
}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("node", harness)
	cmd.Dir = "."
	cmd.Env = append(os.Environ(), "ENC_V2_HOST="+host, "ENC_V2_SAMPLE="+sample)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("CPAMC v1.27.4 enc::v2 decode failed: %v\n%s", err, output)
	}
}
