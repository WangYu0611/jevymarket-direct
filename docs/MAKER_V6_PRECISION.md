# v6-perf-counter-r1：修正本地反应计时，主运行保留HTTP异常链

基于 `bcf7aec`。不修改 MakerConfig、概率、入场时间、挂价、仓位、盘口恢复、成交或结算规则，不新增真实订单能力，不改依赖或 maker_protocol。基础 I/O 标记仍为 v6-io-r4；新计时单独标记 v6-perf-counter-r1。

## 为什么改

部分 Windows Python 3.12 环境中，monotonic 的报告分辨率是15.625ms，perf_counter的报告分辨率则细得多。原本地反应计时用前者，0.1ms直方图桶不能弥补底层时钟分辨率。旧统计中的0ms不证明零延迟。时钟粒度本身也不能解释数秒的源行情过期；当前短HTTP检查成功不补回过去的连接故障原因。

Python 3.12依据：https://docs.python.org/3.12/library/time.html#time.perf_counter_ns

## 修改范围

- 本地合并信号到动作以及服务器时间HTTP请求往返耗时使用perf_counter_ns差值。首个合并信号的起点保留，起点0不当成无信号；无订单与定时检查仍采样。100ms本地反应预算数值不变，使用更精确的测量进行判断。记录不等于网络、完整WS解析链路或真实撤单回执。
- 缓存和模拟订单生命周期继续使用原monotonic秒，不把两种时钟的绝对读数相减，不重写源时间、目标价或历史样本。未修改asyncio时钟、系统时间、代理、TLS配置或行情新鲜度要求。
- runtime保存measurement信息；diagnose输出measurement_latest_run。旧运行没有记录时钟信息时明确标成legacy_or_unrecorded，不用当前机器的时钟冒充历史运行时钟。直方图桶上界不等于测量准确度。
- metadata/server_clock/settlement出错时复用现有safe_error，仅保留固定异常类型和数字OS错误码；不保留异常正文、URL、HTTP头、代理值或证书路径。下一次主程序出错时即可留证，不需要另跑探针期待复现。
- diagnose在原只读事务内汇总http_failures_latest_run，最多16种异常模式，省略事件明确计数；旧记录没有异常链时仍算未记录，不推断DNS或证书原因。它只统计HTTP阶段错误，没有成功请求分母，不是错误率。

## 验收

更新并通过回归测试后，使用新的诊断数据库运行一次300秒 `maker run --dry-run --observe-only --loop 2 --seconds 300 --db <new.db>`，再用同一个db执行 `maker diagnose --out <new.json.gz>`。无需重复HTTP独立探针，无需收益长跑，不删除旧库。

启动应显示 `v6-perf-counter-r1 / perf_counter_ns`。检查measurement_latest_run、http_failures_latest_run、local_reaction_all_evaluations、resync_latest_run及data_health。零订单是只观察模式预期结果。没有复现某错误不能证明其根因已消失；计时修正不是网络加速，不保证更高收益或解决数秒行情滞后。

新增确定性测试覆盖亚tick耗时、0/None起点、100ms边界、无订单和定时评估、时钟域隔离、倒退保护、HTTP安全异常链、分组有界、最近运行范围及只读/不覆盖，并用主运行离线替身验证接入。不得跳过失败测试启用采集；CI通过不是用户网络下的性能或盈利验证。
