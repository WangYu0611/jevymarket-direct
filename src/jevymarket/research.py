"""Research current facts with DeepSeek's official API and native web search.

The researcher uses DeepSeek's Anthropic-compatible Messages endpoint with the
``web_search_20250305`` server tool. DeepSeek performs the search server-side;
this module then parses the final JSON brief and passes only compact evidence to
Jev. The researcher never estimates market probability.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlparse

import httpx

log = logging.getLogger(__name__)

DEFAULT_EXCLUDE_DOMAINS = [
    "polymarket.com", "kalshi.com", "polyspotter.com", "polyveritas.com", "hkimarket.com",
    "manifold.markets", "metaculus.com", "predictit.org", "polymarketanalytics.com",
    "polymarket.us", "betfair.com", "oddschecker.com",
]

SYSTEM_PROMPT = """You are a neutral research analyst supporting a prediction-market pricing model.
Your only job is to find and report the CURRENT facts that bear on how the given market question
will resolve. You MUST use the web_search tool. Prefer primary and reputable sources.

Rules:
- Source from news outlets, wire services, official government/organisation statements, regulators,
  league/federation sites, company filings, and reference data. Prediction-market sites and their
  mirrors are NOT sources: they only reflect odds, and this analysis must not see odds.
- Every fact must carry a date (YYYY-MM-DD). If you cannot date it, say \"undated\".
- Do not restate the market's resolution rules as facts; the reader already has them.
- Quote resolution-relevant numbers, names, deadlines and official statements exactly when available.
- Report the most recent development you can find and its date.
- Give considerations for and against a YES resolution as short factual bullets, not opinions.
- Do NOT estimate a probability, do NOT say what you would bet, do NOT summarize market odds.
- If you find nothing relevant, say so plainly in `summary`.
- Your FINAL text block must be strictly one JSON object, no prose around it, with exactly these keys:
  {
    \"as_of\": \"YYYY-MM-DD (date of the newest fact you found)\",
    \"summary\": \"2-4 sentences: current status relevant to resolution\",
    \"key_facts\": [\"YYYY-MM-DD: fact\", ...],
    \"latest_development\": \"YYYY-MM-DD: what happened most recently\",
    \"for_yes\": [\"short factual point\", ...],
    \"against_yes\": [\"short factual point\", ...],
    \"sources\": [\"https://...\", ...]
  }"""


class ResearchError(RuntimeError):
    def __init__(self, status: int, message: str, body: Any = None):
        super().__init__(f"Researcher/DeepSeek {status}: {message}")
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
    def from_json(cls, data: dict, model: str = "", cost: float = 0.0, raw: dict | None = None) -> "Brief":
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
        ev: dict = {
            "as_of": self.as_of,
            "summary": self.summary,
            "key_facts": list(self.key_facts),
            "latest_development": self.latest_development,
            "considerations_for_yes": list(self.for_yes),
            "considerations_against_yes": list(self.against_yes),
        }
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
        model: str = "deepseek-v4-pro",
        base_url: str = "https://api.deepseek.com/anthropic/v1",
        max_searches: int = 5,
        max_tokens: int = 4096,
        timeout: float = 120.0,
        max_retries: int = 3,
        max_calls: int | None = None,
        exclude_domains: list[str] | None = None,
        client: httpx.AsyncClient | None = None,
    ):
        if not api_key:
            raise ValueError("DEEPSEEK_API_KEY is required for the researcher")
        self.model = model
        self.max_searches = max_searches
        self.max_tokens = max_tokens
        self.max_retries = max_retries
        self.max_calls = max_calls
        self.exclude_domains = DEFAULT_EXCLUDE_DOMAINS if exclude_domains is None else exclude_domains
        self._url = base_url.rstrip("/") + "/messages"
        self._own_client = client is None
        self._client = client or httpx.AsyncClient(timeout=timeout)
        self._headers = {
            "x-api-key": api_key,
            "Authorization": f"Bearer {api_key}",
            "anthropic-version": "2023-06-01",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self.calls = 0
        self.total_cost = 0.0
        self.total_input_tokens = 0
        self.total_output_tokens = 0

    async def aclose(self) -> None:
        if self._own_client:
            await self._client.aclose()

    async def __aenter__(self) -> "Researcher":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    @property
    def budget_left(self) -> bool:
        return self.max_calls is None or self.calls < self.max_calls

    def _user_prompt(self, question: str, description: str, resolution_source: str | None,
                     end_date: str | None, today: str) -> str:
        parts = [
            f"Research the latest news and official statements relevant to: {question}",
            f"Today is {today}.",
            f"Resolution rules / description:\n{description or '(none given)'}",
        ]
        if resolution_source:
            parts.append(f"Stated resolution source: {resolution_source}")
        if end_date:
            parts.append(f"Market end date: {end_date}")
        parts.append("Use web_search before answering, then return the required JSON object.")
        return "\n\n".join(parts)

    async def brief(self, question: str, description: str, resolution_source: str | None,
                    end_date: str | None, today: str) -> Brief:
        if not self.budget_left:
            raise ResearchError(0, f"research budget of {self.max_calls} calls for this run exhausted")

        web_tool: dict[str, Any] = {
            "type": "web_search_20250305",
            "name": "web_search",
            "max_uses": self.max_searches,
        }
        if self.exclude_domains:
            web_tool["blocked_domains"] = self.exclude_domains

        body = {
            "model": self.model,
            "max_tokens": self.max_tokens,
            "system": SYSTEM_PROMPT,
            "messages": [{
                "role": "user",
                "content": [{
                    "type": "text",
                    "text": self._user_prompt(question, description, resolution_source, end_date, today),
                }],
            }],
            "tools": [web_tool],
        }
        data = await self._post(body)
        self.calls += 1

        blocks = data.get("content") or []
        if not isinstance(blocks, list):
            raise ResearchError(200, "unexpected response shape: content is not a list", data)
        if not any(isinstance(b, dict) and b.get("type") == "web_search_tool_result" for b in blocks):
            raise ResearchError(200, "DeepSeek did not return web_search_tool_result; current facts were not verified", data)

        # Safety invariant from the original bot: Jev must not receive evidence derived
        # from prediction-market / odds sites. DeepSeek is asked to block them server-side;
        # if one still leaks through, reject the entire brief rather than silently use it.
        leaked = excluded_urls(extract_search_urls(blocks), self.exclude_domains)
        if leaked:
            raise ResearchError(200, f"excluded-domain search results leaked into research: {leaked}", data)

        text = "\n".join(
            str(b.get("text") or "")
            for b in blocks
            if isinstance(b, dict) and b.get("type") == "text" and b.get("text")
        )
        parsed = parse_json_object(text)
        if parsed is None:
            raise ResearchError(200, "researcher did not return a JSON object", data)

        usage = data.get("usage") or {}
        self.total_input_tokens += int(usage.get("input_tokens") or 0)
        self.total_output_tokens += int(usage.get("output_tokens") or 0)

        b = Brief.from_json(parsed, model=str(data.get("model") or self.model), cost=0.0, raw=data)
        leaked = excluded_urls(b.sources, self.exclude_domains)
        if leaked:
            raise ResearchError(200, f"excluded-domain URLs appeared in researcher JSON: {leaked}", data)
        for url in extract_search_urls(blocks):
            if url not in b.sources:
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


def extract_search_urls(blocks: list[Any]) -> list[str]:
    """Collect unique URLs from native search results and citation metadata."""
    out: list[str] = []
    seen: set[str] = set()

    def add(url: Any) -> None:
        if isinstance(url, str) and url and url not in seen:
            seen.add(url)
            out.append(url)

    for block in blocks:
        if not isinstance(block, dict):
            continue
        if block.get("type") == "web_search_tool_result" and isinstance(block.get("content"), list):
            for item in block["content"]:
                if isinstance(item, dict) and item.get("type") == "web_search_result":
                    add(item.get("url"))
        if block.get("type") == "text":
            for cite in block.get("citations") or []:
                if isinstance(cite, dict):
                    add(cite.get("url"))
    return out


def excluded_urls(urls: list[str], blocked_domains: list[str]) -> list[str]:
    """Return URLs whose hostname is a blocked domain or one of its subdomains."""
    blocked = {d.lower().strip(".") for d in blocked_domains if d}
    out: list[str] = []
    for url in urls:
        try:
            host = (urlparse(url).hostname or "").lower().strip(".")
        except ValueError:
            continue
        if host and any(host == d or host.endswith("." + d) for d in blocked):
            out.append(url)
    return out


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
        if body.get("message"):
            return str(body["message"])
    return (resp.text or resp.reason_phrase or "")[:300]
