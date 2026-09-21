import json

import httpx
import pytest
import respx

from jevymarket.research import Brief, Researcher, ResearchError, parse_json_object

URL = "https://api.deepseek.com/anthropic/v1/messages"

BRIEF = {
    "as_of": "2026-09-20",
    "summary": "Talks continue; no strike reported.",
    "key_facts": ["2026-09-19: A relevant official statement was issued."],
    "latest_development": "2026-09-20: A new official update was published.",
    "for_yes": ["A dated fact supporting YES."],
    "against_yes": ["A dated fact supporting NO."],
    "sources": ["https://example.com/a"],
}


def _resp(content=None):
    return {
        "id": "msg-1",
        "model": "deepseek-v4-pro",
        "content": content or [
            {"type": "server_tool_use", "id": "srv-1", "name": "web_search", "input": {"query": "q"}},
            {"type": "web_search_tool_result", "tool_use_id": "srv-1", "content": [
                {"type": "web_search_result", "url": "https://example.com/a", "title": "A", "page_age": "2026-09-20"}
            ]},
            {"type": "text", "text": json.dumps(BRIEF), "citations": [
                {"type": "web_search_result_location", "url": "https://example.com/b", "title": "B", "cited_text": "excerpt"}
            ]},
        ],
        "usage": {"input_tokens": 1000, "output_tokens": 300},
    }


def test_parse_json_object_variants():
    assert parse_json_object(json.dumps(BRIEF))["as_of"] == "2026-09-20"
    fenced = "Here:\n```json\n" + json.dumps(BRIEF) + "\n```"
    assert parse_json_object(fenced)["summary"] == BRIEF["summary"]
    assert parse_json_object("no json") is None


@respx.mock
async def test_brief_native_web_search():
    route = respx.post(URL).mock(return_value=httpx.Response(200, json=_resp()))
    async with Researcher(api_key="k", max_calls=2) as r:
        b = await r.brief("Q?", "desc", None, "2026-09-30", "2026-09-20")
    body = json.loads(route.calls[0].request.content)
    assert body["tools"][0]["type"] == "web_search_20250305"
    assert body["tools"][0]["max_uses"] == 5
    assert "polymarket.com" in body["tools"][0]["blocked_domains"]
    assert b.sources == ["https://example.com/a", "https://example.com/b"]
    assert r.total_input_tokens == 1000 and r.total_output_tokens == 300


@respx.mock
async def test_no_native_search_result_is_rejected():
    content = [{"type": "text", "text": json.dumps(BRIEF)}]
    respx.post(URL).mock(return_value=httpx.Response(200, json=_resp(content)))
    async with Researcher(api_key="k") as r:
        with pytest.raises(ResearchError, match="web_search_tool_result"):
            await r.brief("Q?", "d", None, None, "2026-09-20")


@respx.mock
async def test_excluded_domain_leak_is_rejected():
    bad = _resp()
    bad["content"][1]["content"][0]["url"] = "https://polymarket.com/event/x"
    respx.post(URL).mock(return_value=httpx.Response(200, json=bad))
    async with Researcher(api_key="k") as r:
        with pytest.raises(ResearchError, match="excluded-domain"):
            await r.brief("Q?", "d", None, None, "2026-09-20")


def test_to_state_trims_to_max_chars():
    b = Brief.from_json({**BRIEF, "key_facts": [f"2026-09-{i:02d}: " + "x" * 200 for i in range(1, 20)]})
    ev = b.to_state(max_chars=1200)
    assert len(json.dumps(ev)) <= 1200
    assert "sources" not in ev
