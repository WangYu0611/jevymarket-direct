import ssl

import httpx
import pytest
import respx

from jevymarket.jev import JevClient, JevError, choice, noul, score

URL = "https://api.typesafe.ai/v1/systemone"

SAMPLE = {
    "id": "dec-1",
    "model": "jev-1.13.0",
    "provider": "TypeSafe",
    "answers": {
        "urgent": {"type": "noul", "noul": 0.83},
        "queue": {"type": "choice", "choice": "billing", "probabilities": {"billing": 0.9, "technical": 0.1}, "confidence": 0.88},
        "anger": {"type": "score", "score": 2.92, "probabilities": [0.05, 0.1, 0.25, 0.6], "confidence": 0.7},
    },
    "usage": {"input_tokens": 476, "output_tokens": 65},
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
    req = route.calls[0].request
    assert req.headers["Authorization"] == "Bearer k"
    assert d.answers["urgent"].noul == 0.83
    assert d.answers["queue"].choice == "billing"
    assert d.answers["anger"].score == 3
    assert d.usage.input_tokens == 476
    assert jev.total_output_tokens == 65


@respx.mock
async def test_boolean_noul_is_coerced():
    respx.post(URL).mock(return_value=httpx.Response(200, json={"answers": {"q": {"noul": True}}}))
    async with JevClient(api_key="k") as jev:
        d = await jev.decide({}, {"q": noul("?")})
    assert d.answers["q"].noul == 1.0


@respx.mock
async def test_4xx_raises_with_message():
    respx.post(URL).mock(return_value=httpx.Response(401, json={"error": {"message": "bad key"}}))
    async with JevClient(api_key="k") as jev:
        with pytest.raises(JevError) as ei:
            await jev.decide({}, {"q": noul("?")})
    assert ei.value.status == 401 and "bad key" in str(ei.value)


@respx.mock
async def test_transport_timeout_retries_then_succeeds(monkeypatch):
    calls = 0

    async def no_sleep(_seconds):
        return None

    async def handler(_request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadTimeout("temporary timeout")
        return httpx.Response(200, json=SAMPLE)

    monkeypatch.setattr("jevymarket.jev.asyncio.sleep", no_sleep)
    respx.post(URL).mock(side_effect=handler)

    async with JevClient(api_key="k", max_retries=2) as jev:
        d = await jev.decide({}, {"urgent": noul("?")})

    assert calls == 2
    assert d.answers["urgent"].noul == 0.83


@respx.mock
async def test_transport_timeout_exhaustion_becomes_jev_error():
    respx.post(URL).mock(side_effect=httpx.ReadTimeout("temporary timeout"))

    async with JevClient(api_key="k", max_retries=1) as jev:
        with pytest.raises(JevError) as ei:
            await jev.decide({}, {"q": noul("?")})

    assert ei.value.status == 0
    assert "ReadTimeout" in str(ei.value)


@respx.mock
async def test_ssl_error_retries_then_succeeds(monkeypatch):
    calls = 0

    async def no_sleep(_seconds):
        return None

    async def handler(_request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ssl.SSLError("record layer failure")
        return httpx.Response(200, json=SAMPLE)

    monkeypatch.setattr("jevymarket.jev.asyncio.sleep", no_sleep)
    respx.post(URL).mock(side_effect=handler)

    async with JevClient(api_key="k", max_retries=2) as jev:
        d = await jev.decide({}, {"urgent": noul("?")})

    assert calls == 2
    assert d.answers["urgent"].noul == 0.83
