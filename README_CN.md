# DDNet 服务器列表镜像

官方 DDNet 服务器列表（`servers.json`）的镜像，可减轻对官方 `master1-4.ddnet.org` 上游的负载，并能绕过 Cloudflare 机器人防护。

本仓库包含两个服务：

| 服务 | 入口点 | 运行位置 | 职责 |
| --- | --- | --- | --- |
| A — 镜像 | `ddnet-mirror` | 任意服务器 | 提供缓存的 `servers.json`，对上游进行节流，融合外部数据源 |
| B — 绕过 | `ddnet-bypass` | 具有 GUI/浏览器的机器 | 运行 `nodriver`，解决 Cloudflare JS 挑战，返回 cookie + UA |

服务 B 从不承载实际数据：它仅向服务 A 提供一个新鲜的 `cf_clearance` cookie，然后服务 A 通过 `curl_cffi` 重放请求。

## 安装

```bash
uv sync                    # 安装服务 A 的依赖
uv sync --extra bypass     # 额外安装 nodriver（用于 GUI 机器）
```

## 运行

```bash
uv run ddnet-mirror --config config.yaml          # 服务 A
uv run ddnet-bypass --port 9100 --token CHANGEME  # 服务 B（不同的机器）
```

## HTTP 端点（服务 A）

| 方法 | 路径 | 语义 | 访问上游？ |
| --- | --- | --- | --- |
| GET | `/` | 等同于 `/api/v1/servers` | 受节流 |
| GET | `/health` | 等同于 `/api/v1/health` | 否（后台探测） |
| GET | `/metrics` | Prometheus 指标 | 否 |
| GET | `/api/v1/servers` | 立即请求，仅在节流门打开时刷新 | 受节流 |
| GET | `/api/v1/servers/cache` | 当前缓存副本，从不访问上游 | 否 |
| GET | `/api/v1/health` | 进程/缓存/上游/绕过/配置状态 | 否（后台探测） |

数据端点原封不动地返回上游响应体，并带有 `Content-Type: application/json`（只有当外部数据源实际贡献了数据时，才会重新编码字节）。

### 缓存头

缓存体保存在内存中，带有一个 `ETag`，以及一个预压缩的 gzip 变体（每次刷新压缩一次，而非每个请求压缩一次）：

- `ETag` + `Last-Modified` + `Cache-Control: public, max-age=<throttle.min_interval_s>`
- 对 `If-None-Match` / `If-Modified-Since` 的响应为 `304` 和空响应体
- `Accept-Encoding: gzip` 可获得约 214 KiB 而不是约 1.2 MiB，并带有 `Vary: Accept-Encoding` 以及针对 gzip 变体的独立 ETag

## 节流与刷新模型

- `throttle.min_interval_s`（3 秒）是**两次上游获取之间的硬性下限**。该闸门以最后一次上游*尝试*为键，因此突发流量——或上游故障——无法变成每个请求都触发一次获取。被闸门限制的请求将从缓存应答并计数（`refresh.gated_requests`、`ddnet_mirror_throttle_gated_total`）。
- 并发的立即请求会被**合并**为单次上游获取（单飞模式）；在获取进行中到达的请求会加入其中。
- 立即请求最多等待 `throttle.immediate_budget_s`（5 秒）进行同步刷新；之后它将提供旧缓存，同时刷新在后台继续运行。
- 一次完整获取在整个端点和重试上受到 `upstream.total_budget_s`（25 秒）的上限约束，因此一个请求不可能消耗 `endpoints * (1 + retries) * timeout_s`。
- 后台刷新按 `background.base_interval_s`（1 分钟）运行。在连续 `extend_after_idle_cycles`（5）个周期没有立即请求后，它会退避到 `extended_interval_s`（5 分钟）。任何立即请求都会重置计数器并恢复基本节奏。休眠时间由 `background.jitter`（±10%）抖动，因此上游无法检测到固定的轮询节奏。后台周期同样遵守节流闸门。
- 上游负载在四个主服务器之间轮询；失败或受挑战门控的端点会落到下一个。每个后台周期轮询**一个**主服务器（旋转），而非全部四个。
- 连续失败 `upstream.down_after_fails`（2）次**获取**的主服务器会从轮换中剔除，持续 `down_retry_after_s`（60 秒），然后重新探测。单次获取内部的多次重试算作一次失败。如果所有端点都宕机，无论如何都会重新探测整个轮换。
- 如果所有上游都失败，仍会提供最后一份缓存副本，且 `/api/v1/health` 报告 `status: degraded`。
- 最终健康快照在关闭时写入日志。

## 健康与指标

`/api/v1/health` 报告轮询状态——没有单独的探测循环，因此检查健康永远不会给主服务器增加负载。每个主服务器报告 `total` / `fails` / `consec_fails` / `down_until` / `challenges` / `challenges_persisted` 以及最后的延迟和错误。绕过对等点直接被探测（这不会给上游带来任何成本），受 `health.probe_ttl_s` 的 TTL 限制。

未设置 `health.auth_token` 时，`/health` 仅暴露状态、缓存新鲜度和运行时间。使用 `Authorization: Bearer <token>` 时，它返回完整快照（pid、配置路径、每个主服务器的错误、绕过对等点详细信息）。

`/metrics` 暴露请求计数和持续时间、上游获取结果和延迟直方图、挑战计数器、每个对等点的绕过解决次数、被节流的请求、数据源失败、缓存年龄/大小以及每个端点的 up 指标。当设置了任一令牌时，它要求 `metrics.auth_token`（回退到 `health.auth_token`）。

## 客户端限制

`limits` 对每个客户端应用令牌桶（`rps`、`burst`）以及全局并发上限（`max_concurrency`，超限时以 `503` + `Retry-After` 丢弃请求）。客户端标识为对等地址，或者当启用 `trust_forwarded_for` 时为最左侧的 `X-Forwarded-For` 条目——仅在可信代理之后才启用该选项。`exempt_paths`（`/health`、`/metrics`）绕过限制器。

## Cloudflare 绕过流程

1. `curl_cffi` 从主服务器收到 403/503 挑战。
2. 服务 A 请求一个绕过对等点（`POST /solve`），请求体为 `{"url", "host"}`。
3. 该对等点驱动 `nodriver` 访问该 URL，等待 `cf_clearance`，返回 `{"cookies", "user_agent", "expires"}`。
4. 服务 A **通过该对等点的 `proxy_url`** 使用这些 cookie 重放请求；cookie 按主机缓存直到过期（`cookie_ttl_s` 或 cookie 自身的 `expires`），并附加到后续获取的*第一个*请求上，因此不需要每次都付出挑战往返的代价。

### 出口必须匹配，以及多个对等点

`cf_clearance` 绑定到解决它的出口 IP。在另一台机器上解决的 cookie 从服务 A 自己的 IP 重放时会再次受到挑战——这正是 `challenges_persisted` 计数的内容。因此每个对等点声明其 cookie 有效的出口：

```yaml
bypass:
  down_after_fails: 2
  down_retry_after_s: 60
  peers:                                  # 按顺序尝试，首次成功即采用
    - name: gui-box-1
      base_url: http://10.0.0.5:9100
      proxy_url: http://10.0.0.5:8888     # 该机器上的 HTTP/SOCKS 转发器
      auth_token: CHANGE_ME               # 必须与服务 B 的 --token 匹配
    - name: gui-box-2
      base_url: http://10.0.0.9:9100
      proxy_url: socks5h://10.0.0.9:1080
```

只有绕过路径使用代理；正常获取直接从服务 A 发出。连续失败 `down_after_fails` 次的对等点会被跳过，持续 `down_retry_after_s`；当 cookie 被拒绝时，该对等点会被归咎，下一次尝试会通过下一个对等点解决。没有 `proxy_url` 的对等点只有在与服务 A 共享出口 IP 时才有效——`/health` 会标记这种情况。

在两侧设置相同的令牌（config.yaml 中的 `auth_token` 和服务 B 的 `--token`）以验证请求。当对等点位于 Cloudflare Tunnel 后面时，你还可以在边缘使用 Cloudflare Access 服务令牌额外保护它：设置 `access_client_id` / `access_client_secret`，服务 A 将发送 `CF-Access-Client-Id` / `CF-Access-Client-Secret` 头。

### 什么时候真正需要 `proxy_url`？

只有当服务 A 和对等点通过**不同的**公共 IP 访问互联网时才需要。检查两台机器：

```bash
curl -s https://ifconfig.me; echo
```

- **相同 IP**（典型情况是两台机器都在同一个家庭/办公室路由器后面）：不设置 `proxy_url`。服务 A 从 Cloudflare 已信任的 IP 重放 cookie，对等点上不需要运行任何额外服务。`/health` 仍会为该未设置字段打印一条 `warn`——将其视为提醒你进行验证，而非错误。
- **不同 IP**（对等点在另一个网络、VPS 或手机热点中）：重放必须物理上源自对等点，因此该机器需要一个供服务 A 拨号的转发器，并且**仅**服务 A 可达。最简 tinyproxy 配置：

  ```ini
  Port 8888
  Listen 10.0.0.5        # 对等点的局域网地址
  Allow 10.0.0.7         # 服务 A，仅此而已
  ```

  `squid` 或 `ssh -D` SOCKS 隧道的工作方式相同；将 `proxy_url` 指向你运行的那个（SOCKS 使用 `socks5h://`，以便 DNS 在对等点解析）。由于代理隧道传输的是服务 A 自身的 TLS 连接，`impersonate` 指纹仍然是服务 A 的——转发器只移动字节。

代理仅承载绕过路径。无 cookie 的获取始终直接从服务 A 发出，因此没有活动 cookie 的对等点看到零流量。

### 绕过对等点的带宽

实测有效载荷：原始 1,237,556 字节，**线上 219,594 字节**（上游提供 gzip）。只有携带 clearance cookie 的获取才会经过对等点：

| 情况 | 对等点上行 | 每天 |
| --- | --- | --- |
| 无挑战活动（无 cookie） | 0 | 0 |
| Cookie 有效，后台节奏 60 秒 | 约 3.6 KiB/s | 约 301 MiB |
| Cookie 有效，立即流量处于 3 秒下限 | 约 71 KiB/s | 约 6 GiB |

## 配置与热重载

YAML 配置（`config.yaml`）在加载时通过 pydantic 验证。**未知键会被拒绝**，因此拼写错误会导致加载失败，而不是静默回退到默认值。通过 **SIGHUP** 进行热重载——在 systemd 下即 `systemctl reload ddnet-mirror`，或 `kill -HUP <pid>`。重载时的错误配置会被拒绝，之前的配置继续提供服务；失败会在 `/api/v1/health` 中显示。更改 `upstream.impersonate` / `timeout_s` 或某个对等点的 `timeout_s` 会在下次使用时重建相应的 HTTP 会话。

有关每个选项及其默认值，请参见 [config.example.yaml](config.example.yaml)。

## 日志

每次运行一个日志文件，命名基于 `logging.filename`（`mirror_{stamp}.log`），其中 `{stamp}` 在启动时由 `logging.timestamp_format`（`%Y-%m-%d_%H-%M-%S`）填充。文件一旦超过 `logging.max_bytes`（5 MiB）或 `logging.max_age_seconds`（7 天）就会轮转；轮转后的文件保留启动时间戳，并增加 `_1`、`_2`、……后缀。

## 外部数据源融合

在配置中启用数据源；每个数据源具有隔离的故障模式（失败的源会被记录、计数并跳过——上游服务永远不会被阻塞，如果所有源都失败，上游字节将原样存储）：

```yaml
datasources:
  - name: third-party
    type: http            # http | file | db（db 是为未来扩展预留的桩）
    url: https://example/api/servers.json
    strategy: merge       # append | merge | override | additional
    key: address          # `merge` 使用的去重键
    target_key: external  # `additional` 使用的顶层键
    enabled: true
```

`type` 和 `strategy` 在加载时验证。融合在上游获取之后、缓存写入之前运行。

## 部署

[deploy/](deploy/) 中有示例 systemd 单元文件：`ddnet-mirror.service`（任意机器）和 `ddnet-bypass.service`（GUI 机器）。

## 测试

```bash
uv run --extra dev pytest
uv run --extra dev ruff check .
```
