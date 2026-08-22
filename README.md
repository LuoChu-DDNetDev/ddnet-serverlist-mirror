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
| GET | `/metrics` | Prometheus metrics | no |
| GET | `/api/v1/servers` | immediate request, refreshes only when the throttle gate is open | throttled |
| GET | `/api/v1/servers/cache` | current cached copy, never touches upstream | no |
| GET | `/api/v1/health` | process / cache / upstream / bypass / config status | no (background probes) |

Data endpoints return the upstream body byte-for-byte with
`Content-Type: application/json` (the bytes are only re-encoded when an external
datasource actually contributed data).

### Caching headers

The cached body is held in memory with an `ETag`, plus a pre-compressed gzip
variant (compressed once per refresh, not per request):

- `ETag` + `Last-Modified` + `Cache-Control: public, max-age=<throttle.min_interval_s>`
- `If-None-Match` / `If-Modified-Since` are answered with `304` and an empty body
- `Accept-Encoding: gzip` gets ~214 KiB instead of ~1.2 MiB, with `Vary: Accept-Encoding`
  and a distinct ETag for the gzip variant

## Throttling & refresh model

- `throttle.min_interval_s` (3s) is a **hard floor between two upstream fetches**.
  The gate is keyed on the last upstream *attempt*, so a burst of traffic — or a
  failing upstream — cannot turn into one fetch per request. Gated requests are
  answered from cache and counted (`refresh.gated_requests`,
  `ddnet_mirror_throttle_gated_total`).
- Concurrent immediate requests are **coalesced** into a single upstream fetch
  (single-flight); a request arriving while a fetch is in flight joins it.
- An immediate request waits at most `throttle.immediate_budget_s` (5s) for a
  synchronous refresh; after that it serves the old cache while the refresh
  keeps running in the background.
- One whole fetch is capped by `upstream.total_budget_s` (25s) across every
  endpoint and retry, so a request cannot cost
  `endpoints * (1 + retries) * timeout_s`.
- Background refresh runs every `background.base_interval_s` (1min). After
  `extend_after_idle_cycles` (5) consecutive cycles with no immediate request it
  backs off to `extended_interval_s` (5min). Any immediate request resets the
  counters and restores the base cadence. Sleeps are jittered by
  `background.jitter` (±10%) so upstreams cannot detect a fixed polling rhythm.
  The background cycle also respects the throttle gate.
- Upstream load is round-robin across the four masters; a failed or
  challenge-gated endpoint falls through to the next one. Each background cycle
  polls **one** master (rotating), not all four.
- A master that fails `upstream.down_after_fails` (2) **fetches** in a row drops
  out of the rotation for `down_retry_after_s` (60s), then gets re-probed.
  Retries inside a single fetch count as one failure. If every endpoint is down,
  the whole rotation is re-probed anyway.
- If every upstream fails, the last cached copy is still served and
  `/api/v1/health` reports `status: degraded`.
- The final health snapshot is written to the log on shutdown.

## Health & metrics

`/api/v1/health` reports the round-robin poll state — no separate probe loop, so
checking health never adds load to the masters. Per master it reports `total` /
`fails` / `consec_fails` / `down_until` / `challenges` / `challenges_persisted`
plus the last latency and error. Bypass peers are probed directly (that costs
nothing upstream), TTL-limited by `health.probe_ttl_s`.

Without `health.auth_token` set, `/health` only exposes status, cache freshness
and uptime. With `Authorization: Bearer <token>` it returns the full snapshot
(pid, config path, per-master errors, bypass peer detail).

`/metrics` exposes request counts and durations, upstream fetch results and
latency histograms, challenge counters, bypass solves per peer, gated requests,
datasource failures, cache age/size and per-endpoint up gauges. It requires
`metrics.auth_token` (falling back to `health.auth_token`) when either is set.

## Client limits

`limits` applies a per-client token bucket (`rps`, `burst`) and a global
in-flight cap (`max_concurrency`, shedding with `503` + `Retry-After`). Client
identity is the peer address, or the left-most `X-Forwarded-For` entry when
`trust_forwarded_for` is enabled — only turn that on behind a trusted proxy.
`exempt_paths` (`/health`, `/metrics`) bypass the limiter.

## Cloudflare bypass flow

1. `curl_cffi` gets a 403/503 challenge from a master.
2. Service A asks a bypass peer (`POST /solve`), request body `{"url", "host"}`.
3. That peer drives `nodriver` to the URL, waits for `cf_clearance`, returns
   `{"cookies", "user_agent", "expires"}`.
4. Service A replays the request with those cookies **through that peer's
   `proxy_url`**; cookies are cached per host until expiry
   (`cookie_ttl_s` or the cookie's own `expires`) and are attached to the *first*
   request of later fetches, so a challenge round-trip is not paid every time.

### Egress must match, and multiple peers

`cf_clearance` is bound to the egress IP that solved it. A cookie solved on
another machine and replayed from Service A's own IP will be challenged again —
that is what `challenges_persisted` counts. So each peer declares the egress its
cookies are valid for:

```yaml
bypass:
  down_after_fails: 2
  down_retry_after_s: 60
  peers:                                  # tried in order, first success wins
    - name: gui-box-1
      base_url: http://10.0.0.5:9100
      proxy_url: http://10.0.0.5:8888     # HTTP/SOCKS forwarder on that machine
      auth_token: CHANGE_ME               # must match service B's --token
    - name: gui-box-2
      base_url: http://10.0.0.9:9100
      proxy_url: socks5h://10.0.0.9:1080
```

Only the bypass path uses the proxy; normal fetches leave directly from Service A.
A peer that fails `down_after_fails` times in a row is skipped for
`down_retry_after_s`; when cookies get rejected the peer is blamed and the next
attempt solves via the next peer. A peer without `proxy_url` only works if it
shares Service A's egress IP — `/health` flags that.

Set the same token on both sides (`auth_token` in config.yaml and `--token` on
Service B) to authenticate the request. When a peer sits behind a Cloudflare
Tunnel, you can additionally protect it at the edge with a Cloudflare Access
Service Token: set `access_client_id` / `access_client_secret` and A will send
the `CF-Access-Client-Id` / `CF-Access-Client-Secret` headers.

### When do you actually need `proxy_url`?

Only when Service A and the peer leave the internet through **different** public
IPs. Check both machines:

```bash
curl -s https://ifconfig.me; echo
```

- **Same IP** (typical when both boxes sit behind one home/office router): leave
  `proxy_url` unset. A replays the cookie from an IP Cloudflare already trusts,
  nothing extra runs on the peer. `/health` still prints a `warn` for the unset
  field — treat it as a reminder to verify, not as an error.
- **Different IPs** (peer in another network, VPS, or a phone hotspot): the
  replay has to physically originate from the peer, so that machine needs a
  forwarder for A to dial, reachable **only** from Service A. Minimal tinyproxy:

  ```ini
  Port 8888
  Listen 10.0.0.5        # peer's LAN address
  Allow 10.0.0.7         # Service A, and nothing else
  ```

  `squid` or an `ssh -D` SOCKS tunnel work the same way; point `proxy_url` at
  whichever you run (`socks5h://` for SOCKS, so DNS resolves on the peer).
  Because the proxy tunnels A's own TLS connection, the `impersonate`
  fingerprint stays A's — the forwarder only moves bytes.

The proxy carries the bypass path only. Uncookied fetches always leave Service A
directly, so a peer with no active cookie sees zero traffic.

### Bandwidth on a bypass peer

Measured payload: 1,237,556 B raw, **219,594 B on the wire** (upstream serves
gzip). Only fetches that carry a clearance cookie traverse the peer:

| Situation | Peer uplink | Per day |
| --- | --- | --- |
| No challenge active (cookie-free) | 0 | 0 |
| Cookie valid, background cadence 60s | ~3.6 KiB/s | ~301 MiB |
| Cookie valid, immediate traffic at the 3s floor | ~71 KiB/s | ~6 GiB |

## Configuration & hot reload

YAML config (`config.yaml`), validated with pydantic on load. **Unknown keys are
rejected**, so a typo fails the load instead of silently falling back to the
default. Hot reload via **SIGHUP** — under systemd that is
`systemctl reload ddnet-mirror`, or `kill -HUP <pid>`. A bad config on reload is
rejected and the previous config keeps serving; the failure is surfaced in
`/api/v1/health`. Changing `upstream.impersonate` / `timeout_s` or a peer's
`timeout_s` rebuilds the corresponding HTTP session on the next use.

See [config.example.yaml](config.example.yaml) for every option and its default.

## Logging

One log file per run, named from `logging.filename` (`mirror_{stamp}.log`) with
`{stamp}` filled from `logging.timestamp_format` (`%Y-%m-%d_%H-%M-%S`) at
startup. A file is rotated once it passes `logging.max_bytes` (5 MiB) or
`logging.max_age_seconds` (7 days); rotated files keep the startup stamp and
gain a `_1`, `_2`, ... suffix.

## External data source fusion

Enable datasources in config; each has an isolated failure mode (a failing
source is logged, counted and skipped — upstream serving is never blocked, and
if every source fails the upstream bytes are stored unchanged):

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

`type` and `strategy` are validated on load. Fusion runs after the upstream
fetch and before the cache write.

## Deploy

Example systemd units in [deploy/](deploy/):
`ddnet-mirror.service` (any machine) and `ddnet-bypass.service` (GUI machine).

## Tests

```bash
uv run --extra dev pytest
uv run --extra dev ruff check .
```
