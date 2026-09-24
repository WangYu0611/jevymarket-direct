"""Explicit, experimental BTC 5m maker policy; no secrets or live-trading flag."""
import math
from dataclasses import asdict, dataclass

VERSION = "v6-btc5m-late-maker-r1"


@dataclass(frozen=True)
class MakerConfig:
    interval_seconds: float = 2.0
    watchdog_seconds: float = 0.05
    reaction_budget_seconds: float = 0.10
    entry_seconds: float = 10.0
    cancel_before_end_seconds: float = 2.0
    quote_ttl_seconds: float = 2.0
    min_requote_seconds: float = 0.20
    paper_submit_latency_seconds: float = 0.10
    paper_cancel_latency_seconds: float = 0.10
    max_book_age_seconds: float = 1.0
    max_reference_age_seconds: float = 5.0
    max_model_age_seconds: float = 2.5
    min_history_seconds: float = 180.0
    max_history_gap_seconds: float = 5.0
    min_confidence: float = 0.92
    uncertainty_buffer: float = 0.005
    min_edge: float = 0.005
    max_model_market_gap: float = 0.10
    min_price: float = 0.85
    max_price: float = 0.98
    max_spread: float = 0.04
    reference_noise_usd: float = 2.0
    sigma_floor_usd_sqrt_second: float = 0.5
    capital_usd: float = 500.0
    risk_fraction: float = 0.01
    max_order_usd: float = 5.0
    max_market_usd: float = 5.0
    max_open_usd: float = 50.0
    max_market_profit_usd: float = 0.50
    daily_loss_limit_usd: float = 10.0
    max_quotes_per_market: int = 8
    concentration_target: float = 0.02

    def __post_init__(self):
        for name, value in asdict(self).items():
            if isinstance(value, bool) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"参数必须为有限正数: {name}")
        if not 0 < self.risk_fraction <= 1 or not 0 < self.concentration_target < 1:
            raise ValueError("比例参数无效")
        if not 0.5 < self.min_confidence < 1:
            raise ValueError("确定性阈值必须在0.5与1之间")
        if not 0 < self.min_price < self.max_price < 1:
            raise ValueError("挂价区间无效")
        if not self.cancel_before_end_seconds < self.entry_seconds <= 30:
            raise ValueError("本实验只支持结束前最多30秒入场，必须留出撤单时间")
        if self.watchdog_seconds > self.reaction_budget_seconds:
            raise ValueError("看门狗间隔不能大于本地反应预算")
        if not self.paper_cancel_latency_seconds < self.cancel_before_end_seconds:
            raise ValueError("模拟撤单延迟必须短于结束保护窗口")
        if self.max_order_usd > self.max_market_usd or self.max_market_usd > self.max_open_usd:
            raise ValueError("单笔/单市场/总敞口上限顺序错误")
        if self.max_open_usd > self.capital_usd:
            raise ValueError("不支持杠杆")
        if type(self.max_quotes_per_market) is not int:
            raise ValueError("最大报价次数必须为整数")
