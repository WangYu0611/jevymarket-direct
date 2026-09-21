import httpx
import pytest
import respx

from jevymarket.jev import JevClient, JevError, choice, noul, score

URL = "https://openrouter.ai/api/alpha/decisions"

SAMPLE = {
    "id": "gen-dec-1",
    "model": "typesafe/jev-1.13-20260917",
    "provider": "TypeSafe",
    "answers": {
        "urgent": {"noul": 0.83},
        "queue": {"choice": "billing", "probabilities": {"billing": 0.9, "technical": 0.1}, "confidence": 0.88},
        "anger": {"score": 2.92, "legend": ["calm", "annoyed", "angry", "furious"],
                  "probabilities": [0.05, 0.1, 0.25, 0.6], "confidence": 0.7},
    },
    "usage": {"input_tokens": 476, "output_tokens": 0, "cost": 0.000019992},
}


def test_builders():
    assert noul("x") == {"type": "noul", "instructions": "x"}
    assert choice("x", {"a": "A", "b": "B"})["criteria"] == {"a": "A", "b": "B"}
    assert score("x", ["l0", "l1"])["criteria"] == ["l0", "l1"]
    with pytest.raises(ValueError):
        choice("x", {"a": "A"})


@respx.mock
async def test_decide_parses_answers():
    route = respx.post(URL).mock(return_value=httpx.Response(200, json=SAMPLE))
    async with JevClient(api_key="k") as jev:
        d = await jev.decide({"t": "hi"}, {"urgent": noul("?")})
    assert route.called
    body = route.calls[0].request
    assert body.headers["Authorization"] == "Bearer k"
    assert d.answers["urgent"].noul == 0.83
    assert d.answers["queue"].choice == "billing"
    assert d.answers["queue"].confidence == 0.88
    assert d.answers["anger"].score == 3
    assert d.answers["anger"].score_mean == 2.92
    assert d.usage.cost == pytest.approx(0.000019992)
    assert jev.total_cost == pytest.approx(0.000019992)


@respx.mock
async def test_boolean_noul_is_coerced():
    respx.post(URL).mock(return_value=httpx.Response(200, json={"answers": {"q": {"noul": True}}}))
    async with JevClient(api_key="k") as jev:
        d = await jev.decide({}, {"q": noul("?")})
    assert d.answers["q"].noul == 1.0


@respx.mock
async def test_4xx_raises_with_message():
    respx.post(URL).mock(return_value=httpx.Response(402, json={"error": {"code": 402, "message": "Insufficient credits"}}))
    async with JevClient(api_key="k") as jev:
        with pytest.raises(JevError) as ei:
            await jev.decide({}, {"q": noul("?")})
    assert ei.value.status == 402 and "Insufficient credits" in str(ei.value)


@respx.mock
async def test_retries_on_429(monkeypatch):
    import jevymarket.jev as m

    async def no_sleep(_):
        return None

    monkeypatch.setattr(m.asyncio, "sleep", no_sleep)
    route = respx.post(URL).mock(side_effect=[
        httpx.Response(429, json={"error": {"message": "slow down"}}),
        httpx.Response(200, json=SAMPLE),
    ])
    async with JevClient(api_key="k", max_retries=3) as jev:
        d = await jev.decide({}, {"q": noul("?")})
    assert route.call_count == 2
    assert d.answers["urgent"].noul == 0.83
