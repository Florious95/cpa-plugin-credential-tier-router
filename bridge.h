#ifndef CREDENTIAL_TIER_ROUTER_BRIDGE_H
#define CREDENTIAL_TIER_ROUTER_BRIDGE_H

#include <stdint.h>
#include <stdlib.h>

typedef struct cliproxy_buffer {
	uint8_t* ptr;
	size_t len;
} cliproxy_buffer;

typedef int (*cliproxy_host_call_fn)(void* host_ctx, const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response);
typedef void (*cliproxy_host_free_buffer_fn)(void* ptr, size_t len);

typedef struct cliproxy_host_api {
	uint32_t abi_version;
	void* host_ctx;
	cliproxy_host_call_fn call;
	cliproxy_host_free_buffer_fn free_buffer;
} cliproxy_host_api;

typedef int (*cliproxy_plugin_call_fn)(char* method, uint8_t* request, size_t request_len, cliproxy_buffer* response);
typedef void (*cliproxy_plugin_free_buffer_fn)(void* ptr, size_t len);
typedef void (*cliproxy_plugin_shutdown_fn)(void);

typedef struct cliproxy_plugin_api {
	uint32_t abi_version;
	cliproxy_plugin_call_fn call;
	cliproxy_plugin_free_buffer_fn free_buffer;
	cliproxy_plugin_shutdown_fn shutdown;
} cliproxy_plugin_api;

void store_credential_tier_router_host_api(const cliproxy_host_api* host);
void set_credential_tier_router_plugin_api(cliproxy_plugin_api* plugin);
int call_credential_tier_router_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response);
void free_credential_tier_router_host_buffer(void* ptr, size_t len);

#endif

