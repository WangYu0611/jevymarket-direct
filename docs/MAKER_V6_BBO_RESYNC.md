# BBO 同连接恢复：快照与分拆增量

基于 `43dd021`。本次修改运行时盘口读取与只读诊断，不改 MakerConfig、原 BookCache 验证器、SnapshotBookCache、模型、价格、仓位、结算或依赖。没有实盘接口。基础 IO_REVISION 仍为 v6-io-r4；版本号文件未修改，验收以提交及新增 resync_latest_run 为准。

## 证据与结论边界

新 probe 保存了三个完整的同连接冲突后片段。三个触发事件之后，两 token 的完整 book 快照与触发 price_change 的 timestamp、逐 token hash 和最终 BBO 一致。两例还在下一条消息中明确给出缺少的零数量删除。全量回放保留的 5,056 条后续消息，共出现 8 次 BBO 隔离：7 次由匹配完整快照恢复，1 次由收齐同版本增量后整批严格校验恢复。三个初始触发的双快照到齐间隔分别为 0.8312、0.8894、0.7407ms；这只是原记录里的本地消息间隔，非新的运行时性能或网络/交易所回执测量。

这确认了这些片段中“分拆更新先到、最终盘口状态随后补齐”与旧版过早重连的冲突，不证明交易所所有消息都有此保证，也不能替代历史 73/149 次失败逐例验证。样本属于同一个 BTC5m 市场；前史有明确截断，原 probe 仍保持 root_cause_verified=false，未改写原始文件。原始数据、钱包、凭证、用户运行日志均不提交仓库。

## 恢复路径

1. 原 apply_book_message 仍先严格拒绝不一致更新并保存 book_reject。
2. 立即使交易缓存失效、发起既有模拟撤单/未知成交风险处理，然后才继续等同连接数据。旧风险暂停不会因行情恢复而解除。
3. 只接纳两个 token、合法参数、同 timestamp 且逐 token hash/BBO 一致的恢复片段。两 token 完整快照在独立缓存中按原规则校验，全部合格才无 await 原子发布。
4. 同版本分拆增量必须等待明确的新 price_change 版本边界；从失败前状态重放明确收到的全部同版本行，调用原验证器整批检查 BBO、交叉盘、价格数量及源时间。成功后边界消息必须重新处理一次，不丢弃，也不重复处理。
5. 不根据 best_bid/best_ask 猜删残留价位；hash 只比较同版本，未声称重新计算/密码学验证。
6. 隔离最多 100ms、256 条消息/累计增量行，有快照档位限制。缺快照、缺增量、版本冲突、畸形数据、tick/其他不支持的插入、断线、取消、跨市场或超时仍失败关闭并走原重连。100ms 是工程上限，不是实盘撤单保证。
7. 保留原5秒接收有效性与独立1秒报价新鲜度：一致但已超过1秒的盘口仍不许报价；不在恢复或边界上刷新源/接收时间。单边盘口策略仍不放行。

## 诊断

新增 book_resync（started/recovered/aborted/ineligible）及 book_resync_input（只使用原公共字段白名单）。maker diagnose 的 summary.resync_latest_run 在原只读 SQLite 事务内统计，包含恢复方法、最大本地等待、恢复但不够报价新鲜的次数。book_reject 保留真实拒绝数量，不因为后来恢复而删减。不要用“book_reject 必须为零”作为新验收条件。

基础 maker_protocol 的错误白名单未修改，因此某些新增恢复拒绝在 source_error 中可显示 unclassified；resync_latest_run.aborted 会明确计数。此限制不放宽检查或允许运行时忽略错误。

## 本地验收

不要再次运行旧 probe；它故意使用旧的严格触发检测器，不能用来验收新运行时。也不清库、不重跑旧收益试验。先停止其他 Maker 采集，保留本地修改，然后更新及全仓库测试，每步失败就停止：

```powershell
git pull --ff-only
uv run --frozen python -m pytest -q
```

使用全新文件名执行 300 秒普通 Maker 的仅观察验收，再调用既有小型导出：

```powershell
$Stamp = Get-Date -Format yyyyMMdd_HHmmss_fff
$Db = "jevymarket.maker-bbo-resync-check_$Stamp.db"
uv run --frozen python -m jevymarket.maker run --dry-run --observe-only --seconds 300 --loop 2 --db $Db
uv run --frozen python -m jevymarket.maker diagnose --db $Db --out "v6_bbo_resync_check_$Stamp.json.gz"
```

空白库下应无新订单。验收重点是出现真实 BBO 触发时，能否在同连接恢复、隔离中是否始终无可用报价、未恢复是否仍失败关闭，以及状态与新鲜度统计。没有触发只说明该段未覆盖恢复场景，不证明盈利或长期稳定。验收通过后才另行设计更早 Maker 入场试验，不能把工程修复与降低交易门槛混为一谈。

## 外部资料

核对日期：2026-09-24。官方协议定义完整 book、price_change、best_bid_ask 等消息；本文的跨消息衔接结论来自实际片段回放，不把官方字段描述当成原子顺序保证。
https://docs.polymarket.com/api-reference/wss/market
