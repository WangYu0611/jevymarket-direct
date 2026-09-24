# v6 首次连接修复：v6-io-r2

本次基于 f512100。只修复消息边界处理、时钟诊断/重试和运行可见性；不改变 Maker 概率模型、2秒记录、最后10秒至T-2秒入场、post-only、金额上限、结算和成交估计。旧 v4 与所有数据库不删除、不迁移。仍没有实盘功能。

## 已确认的代码缺口

原来只跳过完全等于字符串 `PING` / `PONG` 的消息，其余一律 JSON 解析。空帧、带空白或小写心跳、bytes 心跳会进入 JSON 解析；异常处理随后清空该路参考历史并重连。官方 RTDS TypeScript 客户端也对空字符串作了过滤。用户日志只显示 JSONDecodeError，没有原始帧，因此能确认这段代码的处理缺口，但不能宣称所有错误已经证实都来自空帧。

修复为共同的帧解析器：忽略空/全空白帧；识别字符串及UTF-8 bytes形式的大小写心跳；按收到的PING大小写回复PONG；接受JSON对象和对象数组。非空乱码、坏JSON、订阅错误、非法消息结构仍明确拒绝并按原逻辑重连。心跳和空帧不刷新“最后有效BTC报价”的20秒计时，不刷新价格，不生成成交；真断线仍清空该路历史，保护不放宽。

## 时钟

原先RTT过长和时差异常只有一条泛化提示，失败后固定等30秒。本次保留原有 RTT<1秒及时间偏差门槛，不用服务器时间改写行情源时间，也不复用失败样本。

RTT改用单调时钟测量，并检查请求期间系统时钟跳变。输出区分 `clock_rtt_exceeded`、`clock_offset_exceeded`、`clock_wall_jump`、`clock_request_failed`，同时保存RTT及秒级时间戳的偏差不确定区间。请求加 no-cache 头。失败后依次2/4/8/16/30秒重试，成功恢复30秒巡检。单个失败样本仍立即阻止报价。

这些区间不是NTP测时结果；网络拥堵、缓存和本机时间偏差不能仅由旧泛化日志区别。不关闭时钟检查。

## 每2秒的独立数据状态

即使没进入最后10秒，也输出 raw样本数/跨度/年龄、TWAP30与60年龄、当前市场边界目标是否已捕获、两侧盘口新鲜数、参考模型检查原因、时钟检查状态。`数据就绪` 与是否进入交易时段分开；`账本风险暂停=False` 不再被误解成数据已齐。

`BookGap`会显示异常链中白名单原因，例如`bbo_delta_mismatch`、`stale_or_future_book_message`或`delta_without_snapshot_or_out_of_order`，不输出原始响应、任意异常文本或凭证。本次不猜测用户盘口报错的具体根因，也不取消盘口一致性和年龄检查。

## 更新及验收

先Ctrl+C停止采集，保留旧文件。然后：

```powershell
cd C:\Users\wy331\Documents\jevymarket-direct
git pull --ff-only
uv sync --frozen
uv run python -m pytest tests/test_maker_v6.py tests/test_maker_protocol.py -q
uv run python -m jevymarket.maker run --dry-run --loop 2
```

任何一步出错都停止，不强制还原本地uv.lock，不删数据库。命令和数据库名称不变。启动行增加`v6-io-r2`；每条新观察及runtime事件记录`io_revision`，修复前记录保留并可区分。

中途启动无法补造已过去的市场边界目标。至少积累180秒完整raw历史并实际捕获新的精确边界后，才有机会报价。零报价可能来自有效规则过滤，但反复解析错误不是正常预热。数据状态应随接收而推进，而不是永远缺失。

测试覆盖空帧、心跳、bytes、JSON批次、坏帧拒绝、有效报价静默期限、参考历史保留、盘口流、时钟门槛与重试、非入场时段的数据可见性。离线/CI通过不等于在用户网络上的长期连接验收。

## 核对依据

- RTDS与market heartbeat、原始接口格式：https://docs.polymarket.com/market-data/realtime-data
- TWAP源时间及订阅无回放：https://docs.polymarket.com/market-data/chainlink-twap
- 官方RTDS客户端空消息处理：https://github.com/Polymarket/real-time-data-client/blob/main/src/client.ts
