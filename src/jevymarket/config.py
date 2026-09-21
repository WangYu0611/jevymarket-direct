"""Runtime settings, loaded from environment / .env."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- keys -------------------------------------------------------------
    typesafe_api_key: str = ""
    deepseek_api_key: str = ""
    polymarket_private_key: str = ""
    # Optional: the Polymarket proxy / deposit wallet address if you trade via the
    # website's wallet rather than the raw EOA. Leave empty to let the SDK resolve it.
    polymarket_wallet: str | None = None

    # --- Jev (TypeSafe official API) --------------------------------------
    jev_model: str = "jev-latest"
    typesafe_base_url: str = "https://api.typesafe.ai/v1"
    jev_timeout_seconds: float = 30.0
    jev_max_retries: int = 2

    # --- researcher (DeepSeek official API + native web search) -----------
    research_enabled: bool = True
    research_model: str = "deepseek-v4-pro"
    deepseek_base_url: str = "https://api.deepseek.com/anthropic/v1"
    deepseek_json_base_url: str = "https://api.deepseek.com"
    research_ttl_hours: float = 6.0
    max_research_per_run: int = 20
    research_max_searches: int = 5
    research_max_chars: int = 2500
    research_exclude_domains: list[str] | None = None

    # --- signal thresholds -------------------------------------------------
    min_edge: float = 0.08
    min_answerable: float = 0.70
    min_clarity: int = 2
    min_trade_price: float = 0.10
    max_trade_price: float = 0.90

    # --- market filter -----------------------------------------------------
    # Comma-separated crypto asset whitelist. Empty string disables the asset filter.
    allowed_assets: str = "BTC"
    allowed_timeframes: str = "5m,15m,1h"
    short_term_min_liquidity_usd: float = 0.0
    short_term_min_volume_usd: float = 0.0
    anchor_capture_grace_seconds: float = 3.0
    short_term_min_history_seconds: int = 60
    short_term_max_sample_age_seconds: float = 5.0
    min_liquidity_usd: float = 5_000
    min_volume_usd: float = 10_000
    max_days_to_resolution: int = 60
    max_spread: float = 0.06
    min_market_price: float = 0.03
    max_market_price: float = 0.97
    description_max_chars: int = 1_500

    # --- hard caps (enforced in executor) ----------------------------------
    max_usd_per_trade: float = 5.0
    max_open_exposure_usd: float = 50.0
    max_trades_per_run: int = 3
    kelly_fraction: float = 0.25

    # --- misc -------------------------------------------------------------
    dry_run: bool = False
    db_path: str = "jevymarket.db"
    log_level: str = Field(default="INFO")


def load_settings(**overrides) -> Settings:
    return Settings(**overrides)
