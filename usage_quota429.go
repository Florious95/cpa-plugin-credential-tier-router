package main

import (
	"encoding/json"
	"strings"
)

// Only an unambiguous Google ErrorInfo qualifies. Unknown payloads retain the
// full-pool breaker; never classify via message text or permissive JSON fields.
func isGoogleQuotaExhausted(body string) bool {
	dec := json.NewDecoder(strings.NewReader(body))
	dec.UseNumber()
	if !uniqueQuotaJSON(dec, 0) {
		return false
	}
	var status map[string]json.RawMessage
	if json.Unmarshal([]byte(body), &status) != nil || status == nil {
		return false
	}
	if wrapped, ok := status["error"]; ok {
		if _, conflict := status["details"]; conflict {
			return false
		}
		status = nil
		if json.Unmarshal(wrapped, &status) != nil || status == nil {
			return false
		}
	}
	var details []map[string]json.RawMessage
	if json.Unmarshal(status["details"], &details) != nil {
		return false
	}
	found := false
	for _, d := range details {
		var kind, reason string
		if d == nil || json.Unmarshal(d["@type"], &kind) != nil || kind == "" {
			return false
		}
		if kind != "type.googleapis.com/google.rpc.ErrorInfo" {
			continue
		}
		if found || json.Unmarshal(d["reason"], &reason) != nil || reason != "QUOTA_EXHAUSTED" {
			return false
		}
		found = true
	}
	return found
}

// encoding/json ordinarily accepts duplicate keys and lets later values win.
// Reject duplicate (including case-variant) keys before decoding the objects.
func uniqueQuotaJSON(dec *json.Decoder, depth int) bool {
	if depth > 64 {
		return false
	}
	tok, err := dec.Token()
	if err != nil {
		return false
	}
	switch tok {
	case json.Delim('{'), json.Delim('['):
		seen := map[string]bool{}
		for dec.More() {
			if tok == json.Delim('{') {
				key, errKey := dec.Token()
				name, ok := key.(string)
				name = strings.ToLower(name)
				if errKey != nil || !ok || seen[name] {
					return false
				}
				seen[name] = true
			}
			if !uniqueQuotaJSON(dec, depth+1) {
				return false
			}
		}
		_, err = dec.Token()
		return err == nil
	default:
		_, isDelimiter := tok.(json.Delim)
		return !isDelimiter
	}
}
