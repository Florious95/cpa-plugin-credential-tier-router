package main

/*
#include <string.h>
#include "bridge.h"
*/
import "C"

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"unsafe"
)

var pluginRuntime = newRuntime(hostAdapter{})

func main() {
	if os.Getenv("CREDENTIAL_TIER_ROUTER_PREVIEW") != "1" {
		return
	}
	html, err := renderStatusHTML()
	if err != nil {
		panic(err)
	}
	http.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(html)
	})
	fmt.Println("Credential Tiers preview: http://127.0.0.1:4173/?demo=1")
	if err := http.ListenAndServe("127.0.0.1:4173", nil); err != nil {
		panic(err)
	}
}

//export cliproxy_plugin_init
func cliproxy_plugin_init(host *C.cliproxy_host_api, plugin *C.cliproxy_plugin_api) C.int {
	if plugin == nil {
		return -1
	}
	C.store_credential_tier_router_host_api(host)
	pluginRuntime = newRuntime(hostAdapter{})
	C.set_credential_tier_router_plugin_api(plugin)
	return 0
}

//export credentialTierRouterPluginCall
func credentialTierRouterPluginCall(method *C.char, request *C.uint8_t, requestLen C.size_t, response *C.cliproxy_buffer) C.int {
	if response == nil {
		return -1
	}
	methodName := ""
	if method != nil {
		methodName = C.GoString(method)
	}
	requestBytes, ok := copyRequest(request, requestLen)
	if !ok {
		return writePluginResponse(response, failure("invalid_request", "request length is invalid", false))
	}
	return writePluginResponse(response, pluginRuntime.handle(context.Background(), methodName, requestBytes))
}

//export credentialTierRouterPluginFreeBuffer
func credentialTierRouterPluginFreeBuffer(ptr unsafe.Pointer, length C.size_t) {
	_ = length
	C.free(ptr)
}

//export credentialTierRouterPluginShutdown
func credentialTierRouterPluginShutdown() {
	pluginRuntime.shutdown()
}

func copyRequest(request *C.uint8_t, requestLen C.size_t) ([]byte, bool) {
	length := int(requestLen)
	if length < 0 || C.size_t(length) != requestLen {
		return nil, false
	}
	if length == 0 {
		return nil, true
	}
	if request == nil {
		return nil, false
	}
	return append([]byte(nil), unsafe.Slice((*byte)(unsafe.Pointer(request)), length)...), true
}

func writePluginResponse(response *C.cliproxy_buffer, data []byte) C.int {
	if len(data) == 0 {
		response.ptr = nil
		response.len = 0
		return 0
	}
	ptr := C.malloc(C.size_t(len(data)))
	if ptr == nil {
		return -1
	}
	C.memcpy(ptr, unsafe.Pointer(&data[0]), C.size_t(len(data)))
	response.ptr = (*C.uint8_t)(ptr)
	response.len = C.size_t(len(data))
	return 0
}
