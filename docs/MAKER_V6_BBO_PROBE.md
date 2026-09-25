# BBO定向取证：v6-bbo-probe-r1

基线 main `7da7215` / Maker I/O `v6-io-r4`。此变更新增独立取证入口，不修改 MakerRuntime、BookCache、SnapshotBookCache、MakerConfig、价格、模型、风险、模拟成交或结算；不是宣布BBO根因已经修好，也不恢复收益实验。

## 为什么不是重复跑旧900秒

旧运行器在拒绝后退出该连接；旧导出缺少冲突后的同连接连续消息，也没有原始应用消息帧边界。重复导出不能补回从未记录的消息。本工具只采集BTC5m公共盘口，不等180秒参考行情预热、不请求私钥、不创建/读取/修改SQLite数据库，也不实例化模拟或实盘交易引擎。

严格校验器出现 `bbo_delta_mismatch` 时，失败前/候选状态照常保存，缓存仍失效；但独立取证连接不立刻关闭。它保留同一个 `ws.recv()` 载荷里的后续消息，以及随后2秒的连续载荷。后续数据仅保存，不重新应用到失效缓存，更不放行报价。`book`、`price_change`、`last_trade_price`、`tick_size_change`、`best_bid_ask` 的公共字段及相对次序均保留。

## 输出的含义和边界

- 每次连接有独立session，载荷有递增frame_id；列表载荷保留数组边界和message_index。边界是`ws.recv()`应用载荷，不是TCP分包或WebSocket底层分片。
- 记录接收wall/perf_counter_ns、原载荷字节数和SHA256；内容使用公共字段白名单投影，不保存任意HTTP错误体、鉴权头或未知附加字段。不是原始字节逐字转储。
- 最多保留前32帧且不超过1MiB；省略量明确。触发时完整校验器before/candidate作为回放种子；前32帧不一定含有建立初始盘口的全部历史。
- 收齐指定数量的完整BBO样例后提前停止，默认3个；最多180秒观察，最多20次连接；完整与不完整样例合计最多3个。默认每例冲突后观察2秒。关闭连接的等待不算入这2秒。
- 每帧最大1MiB；每例最多6MiB序列化证据；最终输出上限24MiB。触发大小限制、断线、市场切换、运行结束或中断时明确标记不完整，不将其当成完整样例。
- `following`只定位后续首个book/BBO和部分成交消息及接收间隔。它不宣布跨消息原子性、不推断缺失深度、不验证恢复成功，也不把公开成交变成自己的成交。
- `summary.root_cause_verified`和`profit_experiment_enabled`始终为false。零冲突、没有成功连接或样例不足不是BBO修复验收通过。检查`termination`和每个session的`end_reason`。
- 默认输出即为需要提交分析的压缩文件，不需要再运行diagnose、context或stats。原数据库、旧导出及配置保持不变。

## PowerShell

使用之前能正常运行uv的PowerShell窗口。旧交易/采集进程仍运行时，先正常停止，以免本机并行负载污染取证。

```powershell
& {
    $ErrorActionPreference = "Stop"
    Set-Location -LiteralPath "C:\Users\wy331\Documents\jevymarket-direct"
    if (-not (Get-Command uv -CommandType Application -ErrorAction SilentlyContinue)) {
        throw "当前终端找不到uv，请使用之前能运行机器人的窗口。"
    }
    git pull --ff-only
    if ($LASTEXITCODE -ne 0) { throw "Git更新失败；不要强制覆盖本地改动。" }
    uv run --frozen python -m pytest tests/test_maker_bbo_probe.py -q
    if ($LASTEXITCODE -ne 0) { throw "新取证工具测试失败，停止。" }
    uv run --frozen python -m jevymarket.maker_bbo_probe --observe-only --seconds 180 --episodes 3
    if ($LASTEXITCODE -ne 0) { throw "取证工具异常，请保留错误输出。" }
}
```

启动显示 `v6-bbo-probe-r1`。原Maker I/O版本仍为`v6-io-r4`，不要因其没有变成r5而认定更新失败。正常结束显示`已导出`及`runs/v6_bbo_probe_*.json.gz`完整路径，直接提供该文件。中断时会尽力保存不完整取证；不要强制结束进程。已有同名输出在联网前即拒绝覆盖。

## 验证与下一步

离线测试覆盖严格BBO触发、同帧尾部、跨帧继续读取、失效缓存不恢复、不完整尾部、关闭等待不冒充观察、公共字段白名单、畸形/非有限数据、尺寸限制、目标停止、文件独占、市场范围与CLI边界。所有网络测试使用替身；CI通过不等于真实网络取证已完成。

将新样例中的触发源时间、前后快照、BBO事件、成交与接收边界逐项对齐，再决定是否需要隔离等待完整快照等恢复逻辑。不能先剪掉残留价位或关闭校验来制造健康盘口。仅当修复能由实际样例及回归验证，并完成网络盘口验收，才另开更早入场Maker的收益实验。

官方依据（2026-09-24核对）：

```text
https://docs.polymarket.com/api-reference/wss/market
https://github.com/Polymarket/agent-skills/blob/main/websocket.md
```

官方区分成交引起的完整book与挂单/撤单price_change；这支持检查跨消息的更新衔接，但没有据此认定当前七次BBO冲突的共同根因。
