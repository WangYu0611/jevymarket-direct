"""Runtime settings, loaded from environment / .env."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    # --- keys -------------------------------------------------------------
    openrouter_api_key: str = ""
    polymarket_private_key: str = ""
    # Optional: the Polymarket proxy / deposit wallet address if you trade via the
    # website's wallet rather than the raw EOA. Leave empty to let the SDK resolve it.
    polymarket_wallet: str | None = None

    # --- Jev --------------------------------------------------------------
    jev_model: str = "typesafe/jev-1.13"
    openrouter_base_url: str = "https://openrouter.ai/api"

    # --- researcher (generative model + web search via OpenRouter) ---------------
    research_enabled: bool = True
    research_model: str = "deepseek/deepseek-v4-pro-0813"
    research_ttl_hours: float = 6.0        # reuse a cached brief for this long
    max_research_per_run: int = 20         # hard cap on researcher calls per `run` pass
    research_max_results: int = 5          # web search results per brief
    research_max_chars: int = 2500         # evidence block size in the Jev state
    research_exclude_domains: list[str] | None = None  # None -> research.DEFAULT_EXCLUDE_DOMAINS

    # --- signal thresholds -------------------------------------------------
    min_edge: float = 0.08          # |P_jev - best ask| required to trade
    min_answerable: float = 0.70    # Jev's belief that the question is judgeable from the state
    min_clarity: int = 2            # 0..4 score of how unambiguous the resolution criteria are
    # Only buy contracts priced inside this band. LLMs tend to be under-confident at the extremes,
    # so "edge" on 5-cent longshots is usually the model, not the market, being wrong.
    min_trade_price: float = 0.10
    max_trade_price: float = 0.90

    # --- market filter -----------------------------------------------------
    min_liquidity_usd: float = 5_000
    min_volume_usd: float = 10_000
    max_days_to_resolution: int = 60
    max_spread: float = 0.06
    # Skip markets already priced at the extremes: no room for edge, wasted Jev calls.
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
