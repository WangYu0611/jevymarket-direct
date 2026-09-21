"""Bounded retries for market-data reads, never for order submission."""

from __future__ import annotations

import asyncio
import logging
import math
import random
import ssl
from collections.abc import Awaitable, Callable

import httpx
from polymarket.errors import RateLimitError, RequestRejectedError
from polymarket.errors import TransportError as SDKTransportError

log = logging.getLogger(__name__)

# Labels identify read stages, not market slugs; keep API rate-limit cooldowns
# across calls/rounds without delaying unrelated stages or stopping RTDS tasks.
_cooldowns: dict[str, float] = {}


class ReadUnavailable(RuntimeError):
    """A known read failure: discard this observation, never use a stale value."""


def _fatal_transport_error(exc: BaseException) -> bool:
    """Do not hide certificate/configuration errors inside SDK wrappers."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, (
            ssl.SSLCertVerificationError, httpx.LocalProtocolError, httpx.UnsupportedProtocol,
        )):
            return True
        current = current.__cause__ or current.__context__
    return False


async def read_with_retry[T](
    operation: Callable[[], Awaitable[T]],
    *,
    label: str,
    attempts: int = 3,
    timeout_seconds: float = 12.0,
) -> T:
    """Retry a read with a total time budget; preserve cancellation/fatal errors.

    Only pass idempotent data reads here. In particular, do NOT wrap ex.place,
    order submission, approvals, or an entire trading iteration in this helper.
    The callable creates a new coroutine and fetches new data on every attempt.
    """
    if attempts < 1 or not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("读取重试次数和时间预算必须为正数")
    clock = asyncio.get_running_loop().time
    if _cooldowns.get(label, 0.0) > clock():
        raise ReadUnavailable(f"{label}：接口限流冷却中，本次不请求")
    _cooldowns.pop(label, None)
    deadline = clock() + timeout_seconds
    for attempt in range(1, attempts + 1):
        try:
            async with asyncio.timeout(max(0.0, deadline - clock())):
                return await operation()
        except RateLimitError as exc:
            delay = exc.retry_after
            if delay is None or not math.isfinite(delay) or delay < 0:
                delay = 30.0
            _cooldowns[label] = clock() + delay
            raise ReadUnavailable(f"{label}：接口限流，冷却 {delay:.1f}s") from exc
        except (httpx.TransportError, ssl.SSLError, SDKTransportError,
                TimeoutError, RequestRejectedError) as exc:
            if _fatal_transport_error(exc):
                raise
            if isinstance(exc, RequestRejectedError):
                if exc.status == 404:
                    raise ReadUnavailable(f"{label}：接口返回 404，本次无有效数据") from exc
                if exc.status not in (408, 429) and not 500 <= exc.status < 600:
                    raise
                if exc.status == 429:
                    delay = exc.retry_after
                    if delay is None or not math.isfinite(delay) or delay < 0:
                        delay = 30.0
                    _cooldowns[label] = clock() + delay
                    raise ReadUnavailable(f"{label}：接口限流，冷却 {delay:.1f}s") from exc
            delay = 0.5 * 2 ** (attempt - 1) * (1.0 + random.random() * 0.2)
            if attempt == attempts or clock() + delay >= deadline:
                raise ReadUnavailable(
                    f"{label}：读取失败（{type(exc).__name__}），"
                    f"已尝试 {attempt} 次；不使用旧数据"
                ) from exc
            log.warning(
                "%s：%s；%.1fs 后重试（下一次 %d/%d）",
                label, type(exc).__name__, delay, attempt + 1, attempts,
            )
            await asyncio.sleep(delay)
    raise AssertionError("unreachable")
