"""Researcher step: a generative model with web search writes a dated, sourced evidence brief.

Jev has no browsing and a training cutoff, so on news-driven markets it (correctly) reports that
the question is not answerable from what it was given. This module fills that gap: it asks a
chat model through OpenRouter's web-search plugin to gather the *current* facts relevant to
resolution, and returns them as a compact `Brief` that goes into the Jev state. The researcher
never estimates a probability — that stays with Jev.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

log = logging.getLogger(__name__)

REPO_URL = "https://github.com/markusbug/jevymarket"

# Odds sites and their mirrors: they only reflect market prices, which Jev must not see.
DEFAULT_EXCLUDE_DOMAINS = [
    "polymarket.com", "kalshi.com", "polyspotter.com", "polyveritas.com", "hkimarket.com",
    "manifold.markets", "metaculus.com", "predictit.org", "polymarketanalytics.com",
    "polymarket.us", "betfair.com", "oddschecker.com",
]

SYSTEM_PROMPT = """You are a neutral research analyst supporting a prediction-market pricing model.
Your only job is to find and report the CURRENT facts that bear on how the given market question
will resolve. Search the web. Prefer primary and reputable sources.

Rules:
- Source from news outlets, wire services, official government/organisation statements, regulators,
  league/federation sites, company filings, and reference data. Prediction-market sites and their
  mirrors (polymarket, kalshi, polyspotter, polyveritas, hkimarket, manifold, metaculus, etc.) are
  NOT sources: they only reflect odds, and this analysis must not see odds.
- Every fact must carry a date (YYYY-MM-DD). If you cannot date it, say "undated".
- Do not restate the market's resolution rules as facts; the reader already has them. Report what
  has actually happened in the world.
- Quote resolution-relevant numbers, names, deadlines and official statements exactly.
- Report the most recent development you can find and its date.
- Give considerations for and against a YES resolution as short factual bullets — not opinions.
- Do NOT estimate a probability, do NOT say what you would bet, do NOT summarize market odds.
- If you find nothing relevant, say so plainly in `summary`.
- Output strictly one JSON object, no prose around it, with exactly these keys:
  {
    "as_of": "YYYY-MM-DD (date of the newest fact you found)",
    "summary": "2-4 sentences: current status relevant to resolution",
    "key_facts": ["YYYY-MM-DD: fact", ...],           // 3-8 items
    "latest_development": "YYYY-MM-DD: what happened most recently",
    "for_yes": ["short factual point", ...],           // 0-5 items
    "against_yes": ["short factual point", ...],       // 0-5 items
    "sources": ["https://...", ...]
  }"""


class ResearchError(RuntimeError):
    def __init__(self, status: int, message: str, body: Any = None):
        super().__init__(f"Researcher/OpenRouter {status}: {message}")
        self.status = status
        self.message = message
        self.body = body


@dataclass
class Brief:
    summary: str = ""
    key_facts: list[str] = field(default_factory=list)
    latest_development: str = ""
    as_of: str = ""
    for_yes: list[str] = field(default_factory=list)
    against_yes: list[str] = field(default_factory=list)
    sources: list[str] = field(default_factory=list)
    model: str = ""
    cost: float = 0.0
    raw: dict = field(default_factory=dict)

    @classmethod
    def from_json(cls, data: dict, model: str = "", cost: float = 0.0, raw: dict | None = None) -> Brief:
        def _list(x) -> list[str]:
            if isinstance(x, list):
                return [str(i).strip() for i in x if str(i).strip()]
            if isinstance(x, str) and x.strip():
                return [x.strip()]
            return []

        return cls(
            summary=str(data.get("summary") or "").strip(),
            key_facts=_list(data.get("key_facts")),
            latest_development=str(data.get("latest_development") or "").strip(),
            as_of=str(data.get("as_of") or "").strip(),
            for_yes=_list(data.get("for_yes")),
            against_yes=_list(data.get("against_yes")),
            sources=_list(data.get("sources")),
            model=model,
            cost=cost,
            raw=raw or {},
        )

    def to_dict(self) -> dict:
        return {
            "summary": self.summary,
            "key_facts": self.key_facts,
            "latest_development": self.latest_development,
            "as_of": self.as_of,
            "for_yes": self.for_yes,
            "against_yes": self.against_yes,
            "sources": self.sources,
            "model": self.model,
            "cost": self.cost,
        }

    def to_state(self, max_chars: int = 2500) -> dict:
        """Compact evidence block for the Jev state. Sources/model/cost are noise to Jev."""
        ev: dict = {
            "as_of": self.as_of,
            "summary": self.summary,
            "key_facts": list(self.key_facts),
            "latest_development": self.latest_development,
            "considerations_for_yes": list(self.for_yes),
            "considerations_against_yes": list(self.against_yes),
        }
        # Trim list fields from the tail until the block fits.
        while len(json.dumps(ev)) > max_chars:
            for key in ("considerations_against_yes", "considerations_for_yes", "key_facts"):
                if ev[key]:
                    ev[key].pop()
                    break
            else:
                ev["summary"] = ev["summary"][: max(0, max_chars - 400)]
                break
        return ev


class Researcher:
    def __init__(
        self,
        api_key: str,
        model: str = "deepseek/deepseek-v4-pro-0813",
        base_url: str = "https://openrouter.ai/api",
        max_results: int = 5,
        timeout: float = 120.0,
        max_retries: int = 3,
        max_calls: int | None = None,
        exclude_domains: list[str] | None = None,
        client: httpx.AsyncClient | None = None,
    ):
        if not api_key:
            raise ValueError("OPENROUTER_API_KEY is required for the researcher")
        self.model = model
        self.max_results = max_results
        self.max_retries = max_retries
        self.max_calls = max_calls
        self.exclude_domains = DEFAULT_EXCLUDE_DOMAINS if exclude_domains is None else exclude_domains
        self._url = base_url.rstrip("/") + "/v1/chat/completions"
        self._own_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "HTTP-Referer": REPO_URL,
            "X-Title": "jevymarket",
        }
        self.calls = 0
        self.total_cost = 0.0

    async def aclose(self) -> None:
        if self._own_client:
            await self._client.aclose()

    async def __aenter__(self) -> Researcher:
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    @property
    def budget_left(self) -> bool:
        return self.max_calls is None or self.calls < self.max_calls

    def _user_prompt(self, question: str, description: str, resolution_source: str | None,
                     end_date: str | None, today: str) -> str:
        parts = [
            f"Latest news and official statements relevant to: {question}",
            f"Today is {today}.",
            f"Resolution rules / description:\n{description or '(none given)'}",
        ]
        if resolution_source:
            parts.append(f"Stated resolution source: {resolution_source}")
        if end_date:
            parts.append(f"Market end date: {end_date}")
        parts.append("Research the current status and return the JSON object.")
        return "\n\n".join(parts)

    async def brief(self, question: str, description: str, resolution_source: str | None,
                    end_date: str | None, today: str) -> Brief:
        if not self.budget_left:
            raise ResearchError(0, f"research budget of {self.max_calls} calls for this run exhausted")
        body = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": self._user_prompt(question, description, resolution_source, end_date, today)},
            ],
            "plugins": [{
                "id": "web",
                "engine": "exa",
                "max_results": self.max_results,
                **({"exclude_domains": self.exclude_domains} if self.exclude_domains else {}),
                "search_prompt": (
                    f"Web search results as of {today}. Use only news, official and reference "
                    "sources to establish the current, dated facts relevant to the market question; "
                    "ignore prediction-market pages and odds. Cite sources by URL."
                ),
            }],
            "response_format": {"type": "json_object"},
            "temperature": 0.2,
            "usage": {"include": True},
        }
        data = await self._post(body)
        self.calls += 1
        try:
            msg = data["choices"][0]["message"]
        except (KeyError, IndexError, TypeError) as e:
            raise ResearchError(200, f"unexpected response shape: {e}", data) from e
        parsed = parse_json_object(msg.get("content") or "")
        if parsed is None:
            raise ResearchError(200, "researcher did not return a JSON object", data)
        cost = float((data.get("usage") or {}).get("cost") or 0.0)
        self.total_cost += cost
        b = Brief.from_json(parsed, model=str(data.get("model") or self.model), cost=cost, raw=data)
        for ann in msg.get("annotations") or []:
            cite = ann.get("url_citation") if isinstance(ann, dict) else None
            url = (cite or {}).get("url")
            if url and url not in b.sources:
                b.sources.append(url)
        return b

    async def _post(self, body: dict) -> dict:
        delay = 1.0
        for attempt in range(1, self.max_retries + 1):
            resp = await self._client.post(self._url, json=body, headers=self._headers)
            if resp.status_code < 400:
                return resp.json()
            retryable = resp.status_code == 429 or resp.status_code >= 500
            msg = _error_message(resp)
            if retryable and attempt < self.max_retries:
                sleep = delay * (1 + random.random())
                log.warning("researcher %s (%s); retry %d/%d in %.1fs", resp.status_code, msg, attempt, self.max_retries, sleep)
                await asyncio.sleep(sleep)
                delay *= 2
                continue
            raise ResearchError(resp.status_code, msg, _safe_json(resp))
        raise AssertionError("unreachable")


_FENCE = re.compile(r"```(?:json)?\s*(.*?)```", re.S)


def parse_json_object(text: str) -> dict | None:
    """Parse a JSON object from model output, tolerating code fences and surrounding prose."""
    text = text.strip()
    if not text:
        return None
    candidates = [text]
    candidates += [m.strip() for m in _FENCE.findall(text)]
    start, end = text.find("{"), text.rfind("}")
    if start != -1 and end > start:
        candidates.append(text[start:end + 1])
    for c in candidates:
        try:
            obj = json.loads(c)
        except json.JSONDecodeError:
            continue
        if isinstance(obj, dict):
            return obj
    return None


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
    return (resp.text or resp.reason_phrase or "")[:300]
