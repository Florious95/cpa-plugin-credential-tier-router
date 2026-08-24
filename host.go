package main

/*
#include "bridge.h"
*/
import "C"

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"unsafe"
)

type authFile struct {
	Name        string `json:"name"`
	AuthIndex   string `json:"auth_index"`
	Type        string `json:"type,omitempty"`
	Provider    string `json:"provider,omitempty"`
	Status      string `json:"status,omitempty"`
	Disabled    bool   `json:"disabled"`
	Unavailable bool   `json:"unavailable"`
	Priority    int    `json:"priority"`
	Account     string `json:"account,omitempty"`
	Email       string `json:"email,omitempty"`
}

type authDocument struct {
	AuthIndex string          `json:"auth_index"`
	Name      string          `json:"name,omitempty"`
	JSON      json.RawMessage `json:"json"`
}

type hostHeader map[string][]string

type hostHTTPRequest struct {
	AuthIndex string     `json:"auth_index,omitempty"`
	Method    string     `json:"Method"`
	URL       string     `json:"URL"`
	Headers   hostHeader `json:"Headers,omitempty"`
	Body      []byte     `json:"Body,omitempty"`
}

type hostHTTPResponse struct {
	StatusCode int
	Headers    hostHeader
	Body       []byte
}

func (r *hostHTTPResponse) UnmarshalJSON(data []byte) error {
	var wire struct {
		StatusCode      *int       `json:"StatusCode"`
		StatusCodeLower *int       `json:"status_code"`
		Headers         hostHeader `json:"Headers"`
		HeadersLower    hostHeader `json:"headers"`
		Body            *string    `json:"Body"`
		BodyLower       *string    `json:"body"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.StatusCode != nil {
		r.StatusCode = *wire.StatusCode
	} else if wire.StatusCodeLower != nil {
		r.StatusCode = *wire.StatusCodeLower
	}
	if wire.Headers != nil {
		r.Headers = wire.Headers
	} else {
		r.Headers = wire.HeadersLower
	}
	encoded := wire.Body
	if encoded == nil {
		encoded = wire.BodyLower
	}
	if encoded == nil || *encoded == "" {
		return nil
	}
	decoded, err := base64.StdEncoding.DecodeString(*encoded)
	if err != nil {
		if wire.Body == nil {
			r.Body = []byte(*encoded)
			return nil
		}
		return fmt.Errorf("decode host response body: %w", err)
	}
	r.Body = decoded
	return nil
}

type hostAPI interface {
	listAuth(context.Context) ([]authFile, error)
	getAuth(context.Context, string) (authDocument, error)
	saveAuth(context.Context, string, json.RawMessage) error
	httpDo(context.Context, hostHTTPRequest) (hostHTTPResponse, error)
}

type hostAdapter struct{}

func (hostAdapter) listAuth(ctx context.Context) ([]authFile, error) {
	var out struct {
		Files []authFile `json:"files"`
	}
	if err := callHost(ctx, "host.auth.list", map[string]any{}, &out); err != nil {
		return nil, err
	}
	return out.Files, nil
}

func (hostAdapter) getAuth(ctx context.Context, authIndex string) (authDocument, error) {
	var out authDocument
	err := callHost(ctx, "host.auth.get", map[string]string{"auth_index": authIndex}, &out)
	return out, err
}

func (hostAdapter) saveAuth(ctx context.Context, name string, document json.RawMessage) error {
	var out json.RawMessage
	return callHost(ctx, "host.auth.save", map[string]any{"name": name, "json": document}, &out)
}

func (hostAdapter) httpDo(ctx context.Context, request hostHTTPRequest) (hostHTTPResponse, error) {
	var out hostHTTPResponse
	err := callHost(ctx, "host.http.do", request, &out)
	return out, err
}

func callHost(ctx context.Context, method string, payload, target any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", method, err)
	}
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))
	var requestPtr *C.uint8_t
	if len(raw) > 0 {
		ptr := C.CBytes(raw)
		if ptr == nil {
			return fmt.Errorf("allocate %s request", method)
		}
		defer C.free(ptr)
		requestPtr = (*C.uint8_t)(ptr)
	}
	var response C.cliproxy_buffer
	code := C.call_credential_tier_router_host_api(cMethod, requestPtr, C.size_t(len(raw)), &response)
	var responseBytes []byte
	if response.ptr != nil && response.len > 0 {
		responseBytes = C.GoBytes(unsafe.Pointer(response.ptr), C.int(response.len))
		C.free_credential_tier_router_host_buffer(unsafe.Pointer(response.ptr), response.len)
	}
	if len(responseBytes) == 0 {
		return fmt.Errorf("host callback %s returned no response, code=%d", method, int(code))
	}
	var envelope struct {
		OK     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(responseBytes, &envelope); err != nil {
		return fmt.Errorf("decode host callback %s: %w", method, err)
	}
	if !envelope.OK || code != 0 {
		if envelope.Error != nil {
			return fmt.Errorf("host callback %s: %s: %s", method, envelope.Error.Code, envelope.Error.Message)
		}
		return fmt.Errorf("host callback %s failed, code=%d", method, int(code))
	}
	if target != nil && len(envelope.Result) > 0 {
		if err := json.Unmarshal(envelope.Result, target); err != nil {
			return fmt.Errorf("decode host callback result %s: %w", method, err)
		}
	}
	return nil
}

func providerOf(file authFile) string {
	provider := strings.ToLower(strings.TrimSpace(file.Provider))
	if provider == "" {
		provider = strings.ToLower(strings.TrimSpace(file.Type))
	}
	switch provider {
	case "codex", "chatgpt", "chat-gpt":
		return "codex"
	case "antigravity":
		return "antigravity"
	default:
		return ""
	}
}

func managedAuthFile(file authFile) bool {
	name := strings.TrimSpace(file.Name)
	if name == "" || filepath.Base(name) != name || strings.ContainsAny(name, `/\\`) || !strings.HasSuffix(strings.ToLower(name), ".json") {
		return false
	}
	return providerOf(file) != ""
}
