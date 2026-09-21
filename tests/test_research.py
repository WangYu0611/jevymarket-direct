import json

import httpx
import pytest
import respx

from jevymarket.research import Brief, Researcher, ResearchError, parse_json_object

URL = "https://openrouter.ai/api/v1/chat/completions"

BRIEF = {
    "as_of": "2026-09-20",
    "summary": "Talks continue; no strike reported.",
    "key_facts": ["2026-09-19: Iran sent conditions via Qatar.", "2026-09-17: Trump cited a big decision."],
    "latest_development": "2026-09-20: security alerts issued.",
    "for_yes": ["No strike reported."],
    "against_yes": ["Threat level elevated."],
    "sources": ["https://example.com/a"],
}


def _resp(content: str, annotations=None, cost=0.0123, model="deepseek/deepseek-v4-pro-0813"):
    msg = {"role": "assistant", "content": content}
    if annotations is not None:
        msg["annotations"] = annotations
    return {"id": "gen-1", "model": model, "choices": [{"message": msg}], "usage": {"cost": cost}}


def test_parse_json_object_variants():
    assert parse_json_object(json.dumps(BRIEF))["as_of"] == "2026-09-20"
    fenced = "Here you go:\n```json\n" + json.dumps(BRIEF) + "\n```\nDone."
    assert parse_json_object(fenced)["summary"] == BRIEF["summary"]
    prose = "Sure. " + json.dumps(BRIEF) + " That's all."
    assert parse_json_object(prose)["as_of"] == "2026-09-20"
    assert parse_json_object("no json here") is None
    assert parse_json_object("[1,2]") is None


@respx.mock
async def test_brief_happy_path_merges_annotations():
    ann = [{"type": "url_citation", "url_citation": {"url": "https://example.com/b", "title": "B"}},
           {"type": "url_citation", "url_citation": {"url": "https://example.com/a", "title": "A"}}]
    route = respx.post(URL).mock(return_value=httpx.Response(200, json=_resp(json.dumps(BRIEF), ann)))
    async with Researcher(api_key="k", max_calls=2) as r:
        b = await r.brief("Q?", "desc", None, "2026-09-30", "2026-09-20")
    body = json.loads(route.calls[0].request.content)
    assert body["plugins"][0]["id"] == "web"
    assert "polymarket.com" in body["plugins"][0]["exclude_domains"]
    assert body["response_format"] == {"type": "json_object"}
    assert b.as_of == "2026-09-20"
    assert b.key_facts[0].startswith("2026-09-19")
    assert b.sources == ["https://example.com/a", "https://example.com/b"]
    assert b.cost == pytest.approx(0.0123)
    assert r.calls == 1 and r.total_cost == pytest.approx(0.0123)
    assert r.budget_left


@respx.mock
async def test_budget_exhausted_raises():
    respx.post(URL).mock(return_value=httpx.Response(200, json=_resp(json.dumps(BRIEF))))
    async with Researcher(api_key="k", max_calls=1) as r:
        await r.brief("Q?", "d", None, None, "2026-09-20")
        assert not r.budget_left
        with pytest.raises(ResearchError):
            await r.brief("Q?", "d", None, None, "2026-09-20")


@respx.mock
async def test_non_json_content_raises():
    respx.post(URL).mock(return_value=httpx.Response(200, json=_resp("I could not find anything.")))
    async with Researcher(api_key="k") as r:
        with pytest.raises(ResearchError):
            await r.brief("Q?", "d", None, None, "2026-09-20")


@respx.mock
async def test_402_raises_with_message():
    respx.post(URL).mock(return_value=httpx.Response(402, json={"error": {"message": "Insufficient credits"}}))
    async with Researcher(api_key="k") as r:
        with pytest.raises(ResearchError) as ei:
            await r.brief("Q?", "d", None, None, "2026-09-20")
    assert ei.value.status == 402 and "Insufficient credits" in str(ei.value)


def test_to_state_trims_to_max_chars():
    b = Brief.from_json({**BRIEF, "key_facts": [f"2026-09-{i:02d}: " + "x" * 200 for i in range(1, 20)]})
    ev = b.to_state(max_chars=1200)
    assert len(json.dumps(ev)) <= 1200
    assert "sources" not in ev and "as_of" in ev
    assert ev["considerations_for_yes"] == [] or len(ev["key_facts"]) < 19


def test_roundtrip_dict():
    b = Brief.from_json(BRIEF, model="m", cost=0.01)
    b2 = Brief.from_json(b.to_dict(), model=b.model, cost=b.cost)
    assert b2.to_dict() == b.to_dict()
