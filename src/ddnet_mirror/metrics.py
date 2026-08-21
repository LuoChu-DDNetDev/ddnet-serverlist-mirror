"""Prometheus metrics for the mirror service.

Everything here is process-local: the service runs as a single uvicorn process,
so the default registry is enough. Instrumentation is deliberately cheap — a
counter increment per request, gauges filled on scrape from state we already keep.
"""

from __future__ import annotations

import time

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry(auto_describe=True)

CONTENT_TYPE = "text/plain; version=0.0.4; charset=utf-8"

requests_total = Counter(
    "ddnet_mirror_requests_total",
    "Client requests served",
    ["endpoint", "code"],
    registry=REGISTRY,
)
request_duration = Histogram(
    "ddnet_mirror_request_duration_seconds",
    "Time spent serving a client request",
    ["endpoint"],
    buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0),
    registry=REGISTRY,
)
response_bytes = Counter(
    "ddnet_mirror_response_bytes_total",
    "Bytes written to clients (post-compression)",
    ["endpoint", "encoding"],
    registry=REGISTRY,
)
cache_response_total = Counter(
    "ddnet_mirror_cache_response_total",
    "Data responses by kind",
    ["kind"],  # raw | gzip | not_modified
    registry=REGISTRY,
)
upstream_fetch_total = Counter(
    "ddnet_mirror_upstream_fetch_total",
    "Upstream fetch attempts",
    ["host", "result"],  # ok | fail
    registry=REGISTRY,
)
upstream_latency = Histogram(
    "ddnet_mirror_upstream_latency_seconds",
    "Successful upstream fetch latency",
    ["host"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 20.0),
    registry=REGISTRY,
)
upstream_challenge_total = Counter(
    "ddnet_mirror_upstream_challenge_total",
    "Cloudflare challenges seen on upstream",
    ["host", "result"],  # seen | persisted
    registry=REGISTRY,
)
bypass_solve_total = Counter(
    "ddnet_mirror_bypass_solve_total",
    "Bypass solve attempts per peer",
    ["peer", "result"],  # ok | fail
    registry=REGISTRY,
)
datasource_failure_total = Counter(
    "ddnet_mirror_datasource_failure_total",
    "External datasource failures (skipped, never fatal)",
    ["name"],
    registry=REGISTRY,
)
throttle_gated_total = Counter(
    "ddnet_mirror_throttle_gated_total",
    "Immediate requests answered from cache because the throttle gate was closed",
    registry=REGISTRY,
)
refresh_total = Counter(
    "ddnet_mirror_refresh_total",
    "Cache refresh runs",
    ["result"],  # ok | fail
    registry=REGISTRY,
)
rate_limited_total = Counter(
    "ddnet_mirror_rate_limited_total",
    "Requests rejected by client limits",
    ["reason"],  # rps | concurrency
    registry=REGISTRY,
)

cache_age_seconds = Gauge(
    "ddnet_mirror_cache_age_seconds", "Age of the cached server list", registry=REGISTRY
)
cache_size_bytes = Gauge(
    "ddnet_mirror_cache_size_bytes", "Size of the cached server list", registry=REGISTRY
)
endpoint_up = Gauge(
    "ddnet_mirror_endpoint_up", "1 if the last fetch of this master succeeded", ["host"],
    registry=REGISTRY,
)
endpoint_down_until = Gauge(
    "ddnet_mirror_endpoint_down_until_seconds",
    "Epoch until which this master is skipped (0 if in rotation)",
    ["host"],
    registry=REGISTRY,
)
bypass_peer_up = Gauge(
    "ddnet_mirror_bypass_peer_up", "1 if this bypass peer last solved successfully", ["peer"],
    registry=REGISTRY,
)
config_ok = Gauge(
    "ddnet_mirror_config_ok", "1 if the last config load/reload succeeded", registry=REGISTRY
)
uptime_seconds = Gauge(
    "ddnet_mirror_uptime_seconds", "Process uptime", registry=REGISTRY
)


def render(snapshot: dict) -> bytes:
    """Fill gauges from a health snapshot, then serialise the registry."""
    cache = snapshot.get("cache") or {}
    updated_at = cache.get("updated_at")
    if updated_at is not None:
        cache_age_seconds.set(max(0.0, time.time() - updated_at))
    cache_size_bytes.set(cache.get("size_bytes") or 0)
    uptime_seconds.set((snapshot.get("process") or {}).get("uptime_seconds") or 0)
    config_ok.set(1 if (snapshot.get("config") or {}).get("status") == "ok" else 0)
    for host, st in (snapshot.get("upstream") or {}).items():
        endpoint_up.labels(host=host).set(1 if st.get("ok") else 0)
        endpoint_down_until.labels(host=host).set(st.get("down_until") or 0)
    for peer, st in ((snapshot.get("bypass") or {}).get("peers") or {}).items():
        bypass_peer_up.labels(peer=peer).set(1 if st.get("ok") else 0)
    return generate_latest(REGISTRY)
