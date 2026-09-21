"""Client for Jev (TypeSafe AI System One model) through OpenRouter's Decisions API.

Jev is not a chat model. You hand it a JSON `state` and a map of typed `questions`; it
returns a typed decision per question with calibrated probabilities. On OpenRouter this
lives on a separate beta endpoint:

    POST https://openrouter.ai/api/alpha/decisions
    { "model": "typesafe/jev-1.13", "state": {...}, "questions": { name: {...} } }

Three question primitives:
  noul   -> yes/no, answered as P(yes) in [0, 1]
  choice -> one key out of `criteria` {key: description}, plus per-key probabilities
  score  -> position on an ordinal rubric `criteria` [level0, level1, ...], plus probabilities
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


# --- question builders -----------------------------------------------------


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


# --- response models --------------------------------------------------------


class Answer(BaseModel):
    """One typed answer. Permissive on purpose: the endpoint is in beta."""

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
        """P(yes) for a noul question."""
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
        """Raw score as returned. The endpoint returns the expected value (e.g. 2.92), not a level."""
        v = self._get("score", "value")
        return float(v) if isinstance(v, (int, float)) else None

    @property
    def score(self) -> int | None:
        """Nearest rubric level."""
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
        super().__init__(f"Jev/OpenRouter {status}: {message}")
        self.status = status
        self.message = message
        self.body = body


# --- client -------------------------------------------------------------------


class JevClient:
    def __init__(
        self,
        api_key: str,
        model: str = "typesafe/jev-1.13",
        base_url: str = "https://openrouter.ai/api",
        timeout: float = 30.0,
        max_retries: int = 4,
        client: httpx.AsyncClient | None = None,
    ):
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY is required to call Jev")
        self.model = model
        self.max_retries = max_retries
        self._url = base_url.rstrip("/") + "/alpha/decisions"
        self._own_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://github.com/markusbug/jevymarket",
            "X-Title": "jevymarket",
        }
        self.total_cost = 0.0
        self.total_input_tokens = 0
        self.calls = 0

    async def aclose(self) -> None:
        if self._own_client:
            await self._client.aclose()

    async def __aenter__(self) -> JevClient:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    async def decide_raw(self, state: Any, questions: dict[str, Question]) -> dict[str, Any]:
        """POST and return the raw JSON body (for schema inspection)."""
        body = {"model": self.model, "state": state, "questions": questions}
        delay = 0.5
        for attempt in range(1, self.max_retries + 1):
            resp = await self._client.post(self._url, json=body, headers=self._headers)
            if resp.status_code < 400:
                return resp.json()
            retryable = resp.status_code == 429 or resp.status_code >= 500
            msg = _error_message(resp)
            if retryable and attempt < self.max_retries:
                sleep = delay * (1 + random.random())
                log.warning("Jev %s (%s); retry %d/%d in %.1fs", resp.status_code, msg, attempt, self.max_retries, sleep)
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
    return (resp.text or resp.reason_phrase or "")[:300]
