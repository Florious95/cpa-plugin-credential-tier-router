# Credential Tiers

Credential Tiers is a native [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) plugin that manages Codex and Antigravity credentials through four clear tiers: Primary, Regular, Backup, and Paused.

The embedded Management Center page uses named policies instead of numeric scores and provides quota refresh, filtering, previews, manual overrides, and an audit history. It reuses the Management Center's saved sign-in state and never asks for or persists a second copy of the management key.

## Features

- Four policies: quota bands, balanced rotation, reset-soon first, and manual primary/backup.
- Quota bands map `>=50%` to Primary, `20–49%` to Regular, `1–19%` to Backup, and `0%` to Paused.
- An Antigravity credential at 0% (or newly paused) records a durable rest deadline, defaulting to 16 hours; early upstream quota recovery does not release it.
- A failed quota probe keeps the last known result until the configured consecutive-failure threshold is reached, while preserving an active rest deadline.
- Paused credentials use CPA priority `-1` rather than writing `disabled=true`, so the credential can restore after its rest deadline and a healthy quota result.
- The usage plugin passively matches Antigravity HTTP 400 region failures and asynchronously invokes the configured egress command with five-minute event deduplication.
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
      rest_duration_hours: 16
      egress_command: /usr/local/bin/cpa-egress-cycle
      egress_target: to-2.5x
      geo400_debounce_minutes: 5
```

`rest_duration_hours` controls the Antigravity post-exhaustion hold. The usage plugin matches failed Antigravity records whose status is 400 and whose body contains both `FAILED_PRECONDITION` and `User location is not supported for the API use.` (case-insensitive), then starts `egress_command egress_target` with a 15-second timeout. The command is not run more than once for the same credential/body within `geo400_debounce_minutes`.

Start with `auto_apply: false`, open **Credential Tiers** in Management Center, refresh quota, and review the preview before enabling automatic writeback. The region-400 usage listener is passive and does not depend on the quota probe timer.

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
make package VERSION=0.1.2
```

The package target writes a platform zip and checksum into `dist/`.

## Security

- Management API requests require CPA's management key.
- The embedded page reads the Management Center's same-origin saved sign-in state and keeps the decoded key only in JavaScript memory.
- If the Management Center sign-in was not saved, the page asks the user to sign in there with **Remember password** enabled; it never displays a separate key field.
- Auth tokens are read through CPA host APIs and are not returned to the browser.
- The plugin rejects nested or unsafe auth filenames before writeback.
- State files are written with owner-only permissions.

Please report vulnerabilities privately through GitHub's security advisory feature rather than a public issue.

## License

MIT
