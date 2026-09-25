# 快照交接边界与公共HTTP检查

基于 main `ac30601`。不改 MakerConfig、模型、报价价格、入场时间、仓位、结算、依赖或真实交易能力；不修改历史数据库。基础 IO_REVISION 仍为 `v6-io-r4`，不能仅用这个字符串判断是否包含本补丁。

## 快照交接修复

新证据包含同一增量相对于两个token快照分别为“更早”和“同时间”的情况。同时间不等于一定被快照包含，因此仅在以下条件全部成立时丢弃整个消息：

- 各受影响token仍保持其完整快照基线，尚无后续增量，源时间和本地接收时间检查合格。
- 至少一行严格早于该token快照；没有任何一行新于对应快照。
- 每一条同时间行的价格档位数量、最优买价和最优卖价都与快照完全相同，应用它确实不会改变状态。

严格更早的行不应用；同时间的重复行也不更新新鲜度。价格、数量、源时间、本地接收时间、缓存代次和对象身份全部保留，另记 book_discard / equal_noop_tokens。全部同时间消息仍走原始apply；同时间数量变化、BBO变化、缺失字段、新旧混合、已接受新增量后的乱序仍走严格检查。不是扩大乱序容忍毫秒数，也不根据BBO推测删除价位。

已在所给两个失败前状态上复现旧拒绝，修订后丢弃且全状态不变。其余提供的BBO和过期失败样例仍被原检查拒绝。本修复不宣称解释所有历史乱序。

## 公共HTTP检查

当独立联网验收已记录连接错误，而原诊断只有 ConnectError/unclassified 时，不能从错误类别直接推断DNS、代理、TLS、服务端故障，也不能因为WebSocket能收到行情就认为HTTP正常。

运行：

```powershell
uv run --frozen python -m jevymarket.maker_transport_probe --observe-only --rounds 6
```

工具不启动Maker、不连接WebSocket、不访问数据库、不导入订单引擎或凭证配置。最多6轮，每轮依次GET公开CLOB `/time` 和Gamma当前BTC5m市场；每次请求有5秒asyncio截止时间，底层HTTPX保留4秒超时。成功响应体只读取至64KiB限制且不输出；不跟随重定向。HTTP异常也是检查结果，不代表诊断程序运行失败。

继承原HTTPX默认代理、环境证书和TLS验证，不关闭SSL验证、不清空代理、不改变NO_PROXY、不选择替代主机。导出只保留固定异常类名、数字OS错误码、状态码、实测请求耗时、设置是否存在的布尔值、Python版本和时钟分辨率；不导出代理地址/账号、证书路径、异常正文、请求/响应头、响应正文、API Key或.env。

自动生成新的 `runs/v6_transport_check_*.json.gz`，已有输出拒绝覆盖。检查结果只能说明这次公共HTTP请求的情况，不会事后补回旧33次连接错误的原因，也不证明WS无积压、BBO长期稳定或策略盈利。无嵌套错误原因就明确保留未知。

先更新和运行测试，测试失败不得继续；不必重复300秒或900秒采集：

```powershell
git pull --ff-only
uv run --frozen python -m pytest -q
uv run --frozen python -m jevymarket.maker_transport_probe --observe-only --rounds 6
```

每行失败都应停止，保留原始错误输出。不要启用收益实验或手动放宽1秒盘口门槛。本次HTTP工具与快照补丁都没有改变收益实验状态。

## 依据与验证

HTTPX官方异常说明：https://www.python-httpx.org/exceptions/
HTTPX官方环境配置说明：https://www.python-httpx.org/environment_variables/

本地运行新增边界/HTTP测试及原resync测试共88项通过。完整仓库的实际导入、Ruff和Windows/Linux测试仍须由CI确认；不把合成测试当成本机网络验收。新增测试不访问公共网络，既有代码和用户提供数据未上传为测试fixture。
