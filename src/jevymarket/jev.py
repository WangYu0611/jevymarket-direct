"""Client for Jev (TypeSafe AI System One model) through TypeSafe's official API.

Jev is not a chat model. You hand it a JSON ``state`` and a map of typed
``questions``; it returns a typed decision per question with calibrated
probabilities.

Official endpoint::

    POST https://api.typesafe.ai/v1/systemone
    {"model": "jev-latest", "state": {...}, "questions": {...}}
"""

from __future__ import annotations

import asyncio
import logging
import random
from typing import Any

import httpx
from pydantic import BaseModel, ConfigDict

log = logging.getLogger(__name__)

Question = dict[str, Any]


def noul(instructions: str) -> Question:
    return {"type": "noul", "instructions": instructions}


def choice(instructions: str, criteria: dict[str, str]) -> Question:
    if not 2 <= len(criteria) <= 255:
        raise ValueError("choice needs between 2 and 255 options")
    return {"type": "choice", "instructions": instructions, "criteria": dict(criteria)}


def score(instructions: str, levels: list[str]) -> Question:
    if not 2 <= len(levels) <= 10:
        raise ValueError("score needs between 2 and 10 levels")
    return {"type": "score", "instructions": instructions, "criteria": list(levels)}


class Answer(BaseModel):
    """One typed answer. Permissive so minor API additions do not break the bot."""

    model_config = ConfigDict(extra="allow")

    @property
    def raw(self) -> dict[str, Any]:
        return self.model_dump()

    def _get(self, *names: str) -> Any:
        d = self.raw
        for n in names:
            if n in d and d[n] is not None:
                return d[n]
        return None

    @property
    def noul(self) -> float | None:
        v = self._get("noul", "probability", "p", "value")
        if isinstance(v, bool):
            return 1.0 if v else 0.0
        if isinstance(v, (int, float)):
            return float(v)
        return None

    @property
    def choice(self) -> str | None:
        v = self._get("choice", "value")
        return str(v) if v is not None else None

    @property
    def score_mean(self) -> float | None:
        v = self._get("score", "value")
        return float(v) if isinstance(v, (int, float)) else None

    @property
    def score(self) -> int | None:
        v = self.score_mean
        return int(round(v)) if v is not None else None

    @property
    def probabilities(self) -> dict[str, float] | list[float] | None:
        return self._get("probabilities", "distribution")

    @property
    def confidence(self) -> float | None:
        v = self._get("confidence")
        return float(v) if isinstance(v, (int, float)) else None


class Usage(BaseModel):
    model_config = ConfigDict(extra="allow")
    input_tokens: int = 0
    output_tokens: int = 0
    # TypeSafe's direct API currently reports tokens, not request cost.
    cost: float = 0.0


class Decision(BaseModel):
    model_config = ConfigDict(extra="allow")
    id: str | None = None
    model: str | None = None
    provider: str | None = None
    answers: dict[str, Answer]
    usage: Usage = Usage()


class JevError(RuntimeError):
    def __init__(self, status: int, message: str, body: Any = None):
        super().__init__(f"Jev/TypeSafe {status}: {message}")
        self.status = status
        self.message = message
        self.body = body


class JevClient:
    def __init__(
        self,
        api_key: str,
        model: str = "jev-latest",
        base_url: str = "https://api.typesafe.ai/v1",
        timeout: float = 30.0,
        max_retries: int = 4,
        client: httpx.AsyncClient | None = None,
    ):
        if not api_key:
            raise ValueError("TYPESAFE_API_KEY is required to call Jev")
        self.model = model
        self.max_retries = max_retries
        self._url = base_url.rstrip("/") + "/systemone"
        self._own_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        self.total_cost = 0.0
        self.total_input_tokens = 0
        self.total_output_tokens = 0
        self.calls = 0

    async def aclose(self) -> None:
        if self._own_client:
            await self._client.aclose()

    async def __aenter__(self) -> JevClient:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    async def decide_raw(self, state: Any, questions: dict[str, Question]) -> dict[str, Any]:
        body = {"model": self.model, "state": state, "questions": questions}
        delay = 0.5
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = await self._client.post(
                    self._url,
                    json=body,
                    headers=self._headers,
                )
            except httpx.TransportError as exc:
                msg = f"{type(exc).__name__}: {exc or 'network transport error'}"
                if attempt < self.max_retries:
                    sleep = delay * (1 + random.random())
                    log.warning(
                        "Jev 网络错误（%s）；重试 %d/%d，%.1fs 后继续",
                        msg,
                        attempt,
                        self.max_retries,
                        sleep,
                    )
                    await asyncio.sleep(sleep)
                    delay *= 2
                    continue
                raise JevError(
                    0,
                    f"网络请求失败，已重试 {self.max_retries} 次：{msg}",
                    {"exception": type(exc).__name__},
                ) from exc

            if resp.status_code < 400:
                return resp.json()
            retryable = resp.status_code == 429 or resp.status_code >= 500
            msg = _error_message(resp)
            if retryable and attempt < self.max_retries:
                sleep = delay * (1 + random.random())
                log.warning(
                    "Jev %s (%s)；重试 %d/%d，%.1fs 后继续",
                    resp.status_code,
                    msg,
                    attempt,
                    self.max_retries,
                    sleep,
                )
                await asyncio.sleep(sleep)
                delay *= 2
                continue
            raise JevError(resp.status_code, msg, _safe_json(resp))
        raise AssertionError("unreachable")

    async def decide(self, state: Any, questions: dict[str, Question]) -> Decision:
        data = await self.decide_raw(state, questions)
        d = Decision.model_validate(data)
        self.calls += 1
        self.total_cost += d.usage.cost
        self.total_input_tokens += d.usage.input_tokens
        self.total_output_tokens += d.usage.output_tokens
        return d


def _safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return resp.text


def _error_message(resp: httpx.Response) -> str:
    body = _safe_json(resp)
    if isinstance(body, dict):
        err = body.get("error")
        if isinstance(err, dict):
            return str(err.get("message") or err)
        if err:
            return str(err)
        if "detail" in body:
            return str(body["detail"])
        if "message" in body:
            return str(body["message"])
    return (resp.text or resp.reason_phrase or "")[:300]
