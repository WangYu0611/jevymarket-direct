import json

import httpx
import pytest
import respx

from jevymarket.research import Brief, Researcher, ResearchError, extract_final_answer, parse_json_object

SEARCH_URL = "https://api.deepseek.com/anthropic/v1/messages"
JSON_URL = "https://api.deepseek.com/chat/completions"

BRIEF = {
    "as_of": "2026-09-20",
    "summary": "Talks continue; no strike reported.",
    "key_facts": ["2026-09-19: A relevant official statement was issued."],
    "latest_development": "2026-09-20: A new official update was published.",
    "for_yes": ["A dated fact supporting YES."],
    "against_yes": ["A dated fact supporting NO."],
    "sources": ["https://example.com/a"],
}


def _search_resp():
    return {
        "id": "msg-1",
        "model": "deepseek-v4-pro",
        "content": [
            {"type": "text", "text": "I will search."},
            {"type": "server_tool_use", "id": "srv-1", "name": "web_search", "input": {"query": "q"}},
            {"type": "web_search_tool_result", "tool_use_id": "srv-1", "content": [
                {"type": "web_search_result", "url": "https://example.com/a", "title": "A", "page_age": "2026-09-20"},
                {"type": "web_search_result", "url": "https://example.com/b", "title": "B", "page_age": "2026-09-19"},
            ]},
            {"type": "text", "text": "2026-09-20: verified factual memo after search."},
        ],
        "usage": {"input_tokens": 1000, "output_tokens": 300},
    }


def _json_resp():
    return {
        "id": "chat-1",
        "model": "deepseek-v4-pro",
        "choices": [{"message": {"role": "assistant", "content": json.dumps(BRIEF)}}],
        "usage": {"prompt_tokens": 400, "completion_tokens": 120},
    }


def test_parse_json_object_variants():
    assert parse_json_object(json.dumps(BRIEF))["as_of"] == "2026-09-20"
    fenced = "Here:\n\`\`\`json\n" + json.dumps(BRIEF) + "\n\`\`\`"
    assert parse_json_object(fenced)["summary"] == BRIEF["summary"]
    assert parse_json_object("no json") is None


def test_extract_final_answer_only_after_last_search():
    blocks = _search_resp()["content"]
    assert extract_final_answer(blocks) == "2026-09-20: verified factual memo after search."


@respx.mock
async def test_brief_search_then_json_mode():
    search = respx.post(SEARCH_URL).mock(return_value=httpx.Response(200, json=_search_resp()))
    formatter = respx.post(JSON_URL).mock(return_value=httpx.Response(200, json=_json_resp()))
    async with Researcher(api_key="k", max_calls=2) as r:
        b = await r.brief("Q?", "desc", None, "2026-09-30", "2026-09-20")

    search_body = json.loads(search.calls[0].request.content)
    assert search_body["tools"][0]["type"] == "web_search_20250305"
    assert "polymarket.com" in search_body["tools"][0]["blocked_domains"]

    format_body = json.loads(formatter.calls[0].request.content)
    assert format_body["response_format"] == {"type": "json_object"}
    assert format_body["thinking"] == {"type": "disabled"}

    assert b.summary == BRIEF["summary"]
    assert b.sources == ["https://example.com/a", "https://example.com/b"]
    assert r.total_input_tokens == 1400
    assert r.total_output_tokens == 420


@respx.mock
async def test_no_native_search_result_is_rejected():
    response = _search_resp()
    response["content"] = [{"type": "text", "text": "no search"}]
    respx.post(SEARCH_URL).mock(return_value=httpx.Response(200, json=response))
    async with Researcher(api_key="k") as r:
        with pytest.raises(ResearchError, match="web_search_tool_result"):
            await r.brief("Q?", "d", None, None, "2026-09-20")


@respx.mock
async def test_excluded_domain_leak_is_rejected():
    bad = _search_resp()
    bad["content"][2]["content"][0]["url"] = "https://polymarket.com/event/x"
    respx.post(SEARCH_URL).mock(return_value=httpx.Response(200, json=bad))
    async with Researcher(api_key="k") as r:
        with pytest.raises(ResearchError, match="excluded-domain"):
            await r.brief("Q?", "d", None, None, "2026-09-20")


def test_to_state_trims_to_max_chars():
    b = Brief.from_json({**BRIEF, "key_facts": [f"2026-09-{i:02d}: " + "x" * 200 for i in range(1, 20)]})
    ev = b.to_state(max_chars=1200)
    assert len(json.dumps(ev)) <= 1200
    assert "sources" not in ev
