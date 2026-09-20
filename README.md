# Credential Tiers

Credential Tiers is a native [CLIProxyAPI](https://github.com/router-for-me/CLIProxyAPI) plugin that manages Codex and Antigravity credentials through four clear tiers: Primary, Regular, Backup, and Paused.

The embedded Management Center page uses named policies instead of numeric scores and provides quota refresh, filtering, previews, manual overrides, and an audit history. It reuses the Management Center's saved sign-in state and never asks for or persists a second copy of the management key.

## Features

- Four policies: quota bands, balanced rotation, reset-soon first, and manual primary/backup.
- With pool management disabled, quota bands map `>=50%` to Primary, `20–49%` to Regular, `1–19%` to Backup, and `0%` to Paused.
- An Antigravity credential at 0% (or newly paused) records a durable rest deadline, defaulting to 16 hours; early upstream quota recovery does not release it.
- A failed quota probe keeps the last known result until the configured consecutive-failure threshold is reached, while preserving an active rest deadline.
- `active_pool_size` (default 4) manages persistent Primary workers per provider. Incumbents keep their seats even at 1% quota or during transient probe failures; strategies rank only reserves filling actual vacancies. Zero disables pool management.
- Tiers and the hard-disable circuit breaker are independent: managed rest uses priority `-1`, while a Geo-400 burst also immediately writes `disabled=true` to break CPA affinity. The following inspection restores `disabled=false` while keeping priority `-1` until rest expires.
- The usage plugin passively matches Antigravity HTTP 400 region failures and rests only the affected credential for two hours by default. The plugin has no network-switching responsibility and never launches external commands.
- Credential updates preserve the complete auth document and change only managed scheduling fields.
- Nested backup files are excluded from scheduling and writeback.
- Credentials within the same tier remain available to CPA's normal round-robin selection.

## Installation

Install **Credential Tiers** from CLIProxyAPI's Plugin Store, or download the archive for your platform from the [latest release](https://github.com/Florious95/cpa-plugin-credential-tier-router/releases/latest).

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
      active_pool_size: 4 # per-provider admission target; zero disables pool management
      interval_minutes: 15
      provider_scope: codex|antigravity
      antigravity_group: gemini
      failure_threshold: 3
      rest_duration_hours: 16
      geo400_rest_hours: 2
```

`active_pool_size` sets the Primary admission target independently for each configured provider. `rest_duration_hours` controls the Antigravity post-exhaustion hold. The usage plugin matches failed Antigravity records whose status is 400 and whose body contains both `FAILED_PRECONDITION` and `User location is not supported for the API use.` (case-insensitive).

The first configuration card contains the original policies and base settings. The second, independent **在岗与休眠** card contains the worker target, exhaustion-rest duration, and Geo-400 rest duration.

### Managed rest lifecycle

1. A matching Geo-400 immediately writes `priority:-1` and `disabled:true` together and records a durable `geo400_rest_hours` deadline (default 2 hours). Reserve admission does not undo this hard-disable in the same event.
2. The next inspection (default 15 minutes) writes `disabled:false`, retaining `priority:-1`. This recovery runs before slow quota probes, and also runs when ordinary Auto Apply is off; in that case only existing managed-rest credentials are touched, without reserve admission.
3. During rest the page displays **强制休眠至…** with remaining time, even if the quota probe is unknown or retrying. Neither early quota recovery nor repeated errors extends/releases the existing rest.
4. At or after the deadline, priority alone returns to a usable tier (normally Backup 200, or Primary 400 if an enabled pool has a vacancy). Failed writeback keeps durable recovery ownership for the next attempt or restart. Genuine external disables without managed-rest ownership are never cleared.

Old network-control configuration fields are ignored on load and disappear on the next state save. There are no network commands, return timers, network management routes, or network controls.

Start with `auto_apply: false`, open **Credential Tiers** in Management Center, refresh quota, and review the preview before enabling automatic writeback. The region-400 usage listener is passive and does not depend on the quota probe timer.

## Persistent Primary workers (v0.4.5)

The state file (`credential-tier-router/state.json`, overridable with `CREDENTIAL_TIER_ROUTER_STATE_PATH`) stores each provider's ordered `active_pool.members` and membership `generation`. Membership, not the latest quota ranking or a temporary auth-file priority, is authoritative:

```text
next members = current members - terminal departures + vacancy admissions
vacancies = max(0, active_pool_size - surviving members)
```

- **Keep:** a worker with positive quota, unknown quota, no snapshot, or transient probe failures below `failure_threshold` retains priority 400. Idle 100% reserves never replace it. Unprobed/retrying reserves cannot acquire a seat.
- **Leave:** explicit quota `<=0`, an unexpired rest, external disable/manual pause, host unavailability/deletion, or consecutive probe failures reaching `failure_threshold` release the seat. The failure threshold is a configured circuit-breaker policy, not proof of upstream account death; even a widespread probe outage reaching it releases affected workers.
- **Replace:** only the resulting vacancies are filled. Recovered former workers wait in Backup (200) rather than reclaiming occupied seats. Geo-400 retirement and cache-based replacement do not wait for an in-flight quota probe.
- **Resize:** increasing the target fills additional vacancies; reducing it to a positive number never evicts surviving workers. For example, 4→2 retains all four until departures drain the excess. The target is therefore not a hard cap during shrink or migration. Setting it to **0 explicitly opts out**: policies directly assign tiers and the sticky-worker guarantee is disabled.
- **Migrate/restart:** only a missing provider entry bootstraps from existing priority-400 workers, including unprobed ones. An initialized empty pool stays initialized. A preserved state file survives restarts/hot reloads; existing membership repairs drifted auth priorities. Removing the state file discards this identity. No attempt is made to reconstruct past workers from cumulative call counts.
- **Commit/recover:** desired membership and quota/rest state are saved before auth writes. Departures are written before admissions; a failed write stops promotion. The next apply/reload replays the durable intent. A failed state save prevents promotion; unreadable/invalid state fails closed instead of overwriting it as a fresh installation. Successful complete inventory is required before treating an absent auth as deleted.
- **Preview:** quota observations are refreshed, but membership and auth files are not changed. Automatic replay requires `auto_apply: true`; explicit Apply is available otherwise. Passive geo-400 safety retirement remains independent of Auto Apply.

This prevents plugin-induced worker churn; it does not pin individual conversations inside CPA or guarantee an upstream provider's Prompt Cache lifetime. CPA still selects among the surviving Primary workers.

## Policies

With pool management enabled these policies order **vacancy candidates only**, except explicit pause remains a terminal exit. Manual Primary/Backup settings do not preempt existing workers. With `active_pool_size: 0`, they directly control tiers as below.

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
go test -race ./...
make package VERSION=0.4.5
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
