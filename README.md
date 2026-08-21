# DDNet Serverlist Mirror

Mirror of the official DDNet server list (`servers.json`) that reduces load on the
official `master1-4.ddnet.org` upstreams and can work around Cloudflare bot protection.

Two services live in this repo:

| Service | Entrypoint | Runs on | Responsibility |
| --- | --- | --- | --- |
| A — mirror | `ddnet-mirror` | any server | Serves cached `servers.json`, throttles upstream, fuses external sources |
| B — bypass | `ddnet-bypass` | machine with a GUI/browser | Runs `nodriver`, solves the Cloudflare JS challenge, returns cookie + UA |

Service B never carries the actual data: it only hands Service A a fresh
`cf_clearance` cookie, and Service A replays the request through `curl_cffi`.

## Install

```bash
uv sync                    # install service A deps
uv sync --extra bypass     # additionally install nodriver (for the GUI machine)
```

## Run

```bash
uv run ddnet-mirror --config config.yaml          # service A
uv run ddnet-bypass --port 9100 --token CHANGEME  # service B (different machine)
```

## HTTP endpoints (service A)

| Method | Path | Semantics | Upstream? |
| --- | --- | --- | --- |
| GET | `/` | same as `/api/v1/servers` | throttled |
| GET | `/health` | same as `/api/v1/health` | no (background probes) |
| GET | `/api/v1/servers` | immediate request, fresh data if older than the throttle window | throttled |
| GET | `/api/v1/servers/cache` | current cached copy, never touches upstream | no |
| GET | `/api/v1/health` | process / cache / upstream / bypass / config status | no (background probes) |

All data endpoints return the upstream body byte-for-byte with
`Content-Type: application/json`.

## Throttling & refresh model

- At most **one** upstream fetch every `throttle.min_interval_s` (3s). A request
  within 3s of the last cache write is answered from cache and an **async
  background refresh** is kicked off (the response is not blocked); otherwise it
  refreshes synchronously.
- Concurrent immediate requests are **coalesced** into a single upstream fetch
  (single-flight), so any burst of traffic costs one request.
- Background refresh runs every `background.base_interval_s` (1min). After
  `extend_after_idle_cycles` (5) consecutive cycles with no immediate request it
  backs off to `extended_interval_s` (5min). Any immediate request resets the
  counters and restores the base cadence. Sleeps are jittered by
  `background.jitter` (±10%) so upstreams cannot detect a fixed polling rhythm.
- Upstream load is round-robin across the four masters; a failed or
  challenge-gated endpoint falls through to the next one. Each background cycle
  polls **one** master (rotating), not all four.
- A master that fails `upstream.down_after_fails` (2) times **in a row** drops
  out of the rotation for `down_retry_after_s` (60s), then gets re-probed. This
  keeps a permanently dead master from costing a full `timeout_s` on every
  cycle. If every endpoint is down, the whole rotation is re-probed anyway.
- `/api/v1/health` shows this round-robin poll state — no separate probe loop,
  so checking health never adds load to the masters. Per master it reports
  `total` / `fails` / `consec_fails` / `down_until` plus the last latency and
  error. The bypass service status updates only when it is actually used to
  solve a challenge.
- If every upstream fails, the last cached copy is still served and
  `/api/v1/health` reports `status: degraded`.
- The final health snapshot is written to the log on shutdown.

## Cloudflare bypass flow

1. `curl_cffi` gets a 403/503 challenge from a master.
2. Service A asks Service B (`POST /solve`), request body `{"url", "host"}`.
3. Service B drives `nodriver` to the URL, waits for `cf_clearance`, returns
   `{"cookies", "user_agent", "expires"}`.
4. Service A retries the request with those cookies; cookies are cached per host
   until expiry (`bypass.cookie_ttl_s` or the cookie's own `expires`).

Set the same token on both sides (`bypass.auth_token` in config.yaml and
`--token` on Service B) to authenticate the request.

When Service B sits behind a Cloudflare Tunnel, you can additionally protect it
at the edge with a Cloudflare Access Service Token: set
`bypass.access_client_id` / `bypass.access_client_secret` in config.yaml and A
will send the `CF-Access-Client-Id` / `CF-Access-Client-Secret` headers on every
request to B.

## Configuration & hot reload

YAML config (`config.yaml`), validated with pydantic on load. Hot reload via
**SIGHUP** — under systemd that is `systemctl reload ddnet-mirror`, or
`kill -HUP <pid>`. A bad config on reload is rejected and the previous config
keeps serving; the failure is surfaced in `/api/v1/health`.

See [config.yaml](config.yaml) for every option and its default.

## Logging

One log file per run, named from `logging.filename` (`mirror_{stamp}.log`) with
`{stamp}` filled from `logging.timestamp_format` (`%Y-%m-%d_%H-%M-%S`) at
startup. A file is rotated once it passes `logging.max_bytes` (5 MiB) or
`logging.max_age_seconds` (7 days); rotated files keep the startup stamp and
gain a `_1`, `_2`, ... suffix.

## External data source fusion

Enable datasources in config; each has an isolated failure mode (a failing
source is logged and skipped, upstream serving is never blocked):

```yaml
datasources:
  - name: third-party
    type: http            # http | file | db (db is a stub for future extension)
    url: https://example/api/servers.json
    strategy: merge       # append | merge | override | additional
    key: address          # dedupe key used by `merge`
    target_key: external  # top-level key used by `additional`
    enabled: true
```

Fusion runs after the upstream fetch and before the cache write.

## Deploy

Example systemd units in [deploy/](deploy/):
`ddnet-mirror.service` (any machine) and `ddnet-bypass.service` (GUI machine).

## Tests

```bash
uv run --extra dev pytest
```