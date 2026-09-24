"""Public HTTP probe tests use MockTransport only, never public network access."""
import gzip
import inspect
import json
import socket
import ssl

import httpx
import pytest

from jevymarket import maker_transport_probe as probe


async def no_pause(_):
    return None


def test_safe_chain_keeps_dns_code_not_exception_or_url():
    inner = socket.gaierror(-2, "SECRET_HOST")
    outer = httpx.ConnectError("SECRET_PROXY")
    outer.__cause__ = inner
    result = probe.safe_error(outer)
    assert result["classes"] == ["ConnectError", "gaierror"]
    assert result["errno_codes"] == [-2] and "SECRET" not in json.dumps(result)


def test_tls_cert_failure_distinguished_from_dns():
    result = probe.safe_error(ssl.SSLCertVerificationError(1, "SECRET_CERT_PATH"))
    assert result["classes"][0] == "SSLCertVerificationError"
    assert "SECRET" not in json.dumps(result)


def test_unknown_class_name_is_not_exported_and_cycles_are_bounded():
    class SECRET(Exception):
        pass
    error = SECRET("SECRET_BODY")
    error.__cause__ = error
    result = probe.safe_error(error)
    assert result["classes"] == ["other_exception"]
    assert "SECRET" not in json.dumps(result)


def test_error_group_retains_nested_known_failures():
    group = ExceptionGroup("SECRET", [ConnectionRefusedError(111, "SECRET"), socket.gaierror(-2, "SECRET")])
    result = probe.safe_error(group)
    assert result["errno_codes"] == [-2, 111] and "SECRET" not in json.dumps(result)


def test_environment_contains_booleans_not_proxy_or_cert_values(monkeypatch):
    monkeypatch.setenv("HTTPS_PROXY", "https://SECRET:SECRET@SECRET")
    monkeypatch.setenv("SSL_CERT_FILE", "/SECRET/cert.pem")
    monkeypatch.setattr(probe, "getproxies", lambda: {"https": "SECRET", "custom_SECRET": "SECRET"})
    result = probe.environment_receipt()
    assert result["environment_present"]["HTTPS_PROXY"]
    assert result["system_proxy_entries_present"]["https"]
    assert "SECRET" not in json.dumps(result)
    assert result["tls_verification"] and result["trust_env"]


def test_endpoint_allowlist():
    assert probe.endpoint("clob_time", 1800000000) == "https://clob.polymarket.com/time"
    assert probe.endpoint("gamma_market", 1800000000).endswith("btc-updown-5m-1800000000")
    with pytest.raises(ValueError):
        probe.endpoint("order", 1800000000)


@pytest.mark.asyncio
async def test_all_requests_bounded_public_only_no_sensitive_output(monkeypatch):
    calls, options = [], []
    monkeypatch.setattr(probe, "environment_receipt", lambda: {})
    def handler(request):
        calls.append(request)
        return httpx.Response(200, text="SECRET_RESPONSE", headers={"X-SECRET": "SECRET"})
    def factory(**kwargs):
        options.append(kwargs)
        return httpx.AsyncClient(transport=httpx.MockTransport(handler), **kwargs)
    result = await probe.collect(6, client_factory=factory, pause=no_pause)
    assert len(calls) == result["summary"]["successes"] == 12
    assert options == [{"timeout": 4}]
    assert all(r.method == "GET" and r.url.host in {"clob.polymarket.com", "gamma-api.polymarket.com"} for r in calls)
    assert all("authorization" not in r.headers for r in calls)
    assert "SECRET" not in json.dumps(result)
    assert not result["trading_enabled"] and not result["summary"]["profit_experiment_enabled"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 429, 500, 302])
async def test_http_status_retained_not_response_body(status):
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(status, text="SECRET"))) as client:
        row = await probe.sample(client, "clob_time")
    assert not row["ok"] and row["status_code"] == status
    assert row["error"]["classes"] == ["HTTPStatusError"]
    assert "SECRET" not in json.dumps(row)


@pytest.mark.asyncio
async def test_connect_error_and_dns_chain_preserved():
    def handler(request):
        try:
            raise socket.gaierror(-2, "SECRET_HOST")
        except socket.gaierror as exc:
            raise httpx.ConnectError("SECRET_PROXY", request=request) from exc
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        row = await probe.sample(client, "gamma_market")
    assert not row["ok"] and row["status_code"] is None
    assert row["error"]["classes"] == ["ConnectError", "gaierror"]
    assert "SECRET" not in json.dumps(row)


@pytest.mark.asyncio
async def test_response_limit_is_explicit(monkeypatch):
    monkeypatch.setattr(probe, "BODY_LIMIT", 8)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, text="0123456789"))) as client:
        row = await probe.sample(client, "clob_time")
    assert row["failure"] == "response_size_limit" and not row["ok"]


@pytest.mark.asyncio
async def test_setup_failure_yields_report_without_secret(monkeypatch):
    monkeypatch.setattr(probe, "environment_receipt", lambda: {})
    def factory(**kwargs):
        raise ValueError("SECRET_PROXY_OR_CERT_PATH")
    result = await probe.collect(1, client_factory=factory)
    assert result["summary"]["requests"] == 0 and result["summary"]["setup_failed"]
    assert "SECRET" not in json.dumps(result)


@pytest.mark.parametrize("rounds", [0, 7, True, .5])
@pytest.mark.asyncio
async def test_programmatic_limits_before_network(rounds):
    with pytest.raises(ValueError):
        await probe.collect(rounds, client_factory=lambda **kw: pytest.fail("network"))


def test_existing_output_stops_before_collect(tmp_path, monkeypatch):
    path = tmp_path / "report.json.gz"
    path.write_bytes(b"KEEP")
    monkeypatch.setattr(probe, "collect", lambda *a: pytest.fail("must not collect"))
    with pytest.raises(FileExistsError):
        probe.main(["--observe-only", "--out", str(path)])
    assert path.read_bytes() == b"KEEP"


def test_export_roundtrip(tmp_path, monkeypatch):
    async def fake(*args):
        return {"summary": {"requests": 12}, "trading_enabled": False}
    monkeypatch.setattr(probe, "collect", fake)
    out = tmp_path / "report.json.gz"
    probe.main(["--observe-only", "--out", str(out)])
    with gzip.open(out, "rt") as f:
        assert json.load(f)["trading_enabled"] is False
    source = inspect.getsource(probe)
    assert all(s not in source for s in ("import sqlite3", "maker_engine import", "load_settings", "verify=False", "trust_env=False"))


@pytest.mark.parametrize("args", [[], ["--observe-only", "--rounds", "7"], ["--observe-only", "--out", "bad.json"]])
def test_cli_bounds(args):
    with pytest.raises(SystemExit):
        probe.main(args)
