# Credential Tiers

Credential Tiers is a native [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) plugin that manages Codex and Antigravity credentials through four clear tiers: Primary, Regular, Backup, and Paused.

The embedded Management Center page uses named policies instead of numeric scores and provides quota refresh, filtering, previews, manual overrides, and an audit history. The management key stays in page memory and is never persisted by the UI.

## Features

- Four policies: quota bands, balanced rotation, reset-soon first, and manual primary/backup.
- Quota bands map `>=50%` to Primary, `20–49%` to Regular, `1–19%` to Backup, and `0%` to Paused.
- A failed quota probe keeps the last known result until the configured consecutive-failure threshold is reached.
- Paused credentials use CPA priority `-1` rather than writing `disabled=true`, so a later quota reset can restore them automatically.
- Credential updates preserve the complete auth document and change only CPA priority.
- Nested backup files are excluded from scheduling and writeback.
- Credentials within the same tier remain available to CPA's normal round-robin selection.

## Installation

Install **Credential Tiers** from CLIProxyAPI's Plugin Store, or download the archive for your platform from the [latest release](https://github.com/William-zgx/cpa-plugin-credential-tier-router/releases/latest).

Release archives use this format:

```text
credential-tier-router_<version>_<goos>_<goarch>.zip
```

Extract the dynamic library at the archive root into CPA's matching plugin directory, for example:

```text
plugins/linux/amd64/credential-tier-router.so
```

Enable the plugin in `config.yaml`:

```yaml
plugins:
  enabled: true
  configs:
    credential-tier-router:
      enabled: true
      auto_apply: false
      strategy: quota_bands
      interval_minutes: 15
      provider_scope: codex|antigravity
      antigravity_group: gemini
      failure_threshold: 3
```

Start with `auto_apply: false`, open **Credential Tiers** in Management Center, refresh quota, and review the preview before enabling automatic writeback.

## Policies

| Policy | Behavior |
| --- | --- |
| Quota bands | Assign tiers from remaining quota. |
| Balanced rotation | Keep healthy credentials in one tier and let CPA rotate them. |
| Reset-soon first | Prefer credentials that reset within 24 hours and still have quota. |
| Manual primary/backup | Apply a fixed named tier to each credential. |

The plugin currently manages `codex` and `antigravity` auth files. For Antigravity, the UI can evaluate either the Gemini quota group or the Claude/GPT quota group.

## Build

Go 1.24 and a C compiler are required because CPA's native plugin ABI uses cgo.

```bash
make test
make vet
make package VERSION=0.1.1
```

The package target writes a platform zip and checksum into `dist/`.

## Security

- Management API requests require CPA's management key.
- The embedded page keeps the key only in JavaScript memory.
- Auth tokens are read through CPA host APIs and are not returned to the browser.
- The plugin rejects nested or unsafe auth filenames before writeback.
- State files are written with owner-only permissions.

Please report vulnerabilities privately through GitHub's security advisory feature rather than a public issue.

## License

MIT
