#include "bridge.h"

extern int credentialTierRouterPluginCall(char* method, uint8_t* request, size_t request_len, cliproxy_buffer* response);
extern void credentialTierRouterPluginFreeBuffer(void* ptr, size_t len);
extern void credentialTierRouterPluginShutdown(void);

static const cliproxy_host_api* stored_host;

void store_credential_tier_router_host_api(const cliproxy_host_api* host) {
	stored_host = host;
}

void set_credential_tier_router_plugin_api(cliproxy_plugin_api* plugin) {
	plugin->abi_version = 1;
	plugin->call = credentialTierRouterPluginCall;
	plugin->free_buffer = credentialTierRouterPluginFreeBuffer;
	plugin->shutdown = credentialTierRouterPluginShutdown;
}

int call_credential_tier_router_host_api(const char* method, const uint8_t* request, size_t request_len, cliproxy_buffer* response) {
	if (stored_host == NULL || stored_host->call == NULL) {
		return 1;
	}
	return stored_host->call(stored_host->host_ctx, method, request, request_len, response);
}

void free_credential_tier_router_host_buffer(void* ptr, size_t len) {
	if (stored_host != NULL && stored_host->free_buffer != NULL && ptr != NULL) {
		stored_host->free_buffer(ptr, len);
	}
}

