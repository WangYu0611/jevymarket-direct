from types import SimpleNamespace

import pytest

import jevymarket.executor as executor


class Handle:
    def __init__(self):
        self.waited = False

    async def wait(self):
        self.waited = True


class State:
    is_fully_approved = True
    missing = SimpleNamespace(erc20=(), erc1155=())


class FakeSecureClient:
    create_calls = []
    created_clients = []

    def __init__(self, *, wallet_type="DEPOSIT_WALLET", wallet="0xdeposit", ephemeral=False):
        self.wallet_type = wallet_type
        self.wallet = wallet
        self.ephemeral = ephemeral
        self.builder_created = False
        self.setup_calls = 0
        self.revoked = False
        self.closed = False

    @classmethod
    async def create(cls, **kwargs):
        cls.create_calls.append(kwargs)
        client = cls(
            wallet_type="DEPOSIT_WALLET",
            wallet=kwargs["wallet"],
            ephemeral=True,
        )
        cls.created_clients.append(client)
        return client

    async def create_builder_api_key(self):
        self.builder_created = True
        return object()

    async def setup_trading_approvals(self):
        self.setup_calls += 1
        return Handle()

    async def get_trading_approvals_state(self, *, wallet=None):
        assert wallet == self.wallet
        return State()

    async def revoke_builder_api_key(self):
        self.revoked = True

    async def close(self):
        self.closed = True


@pytest.fixture(autouse=True)
def reset_fake():
    FakeSecureClient.create_calls.clear()
    FakeSecureClient.created_clients.clear()


def settings():
    return SimpleNamespace(
        polymarket_private_key="0x" + "1" * 64,
        polymarket_wallet=None,
    )


@pytest.mark.asyncio
async def test_gasless_deposit_wallet_uses_ephemeral_builder_key(monkeypatch):
    monkeypatch.setattr(executor, "AsyncSecureClient", FakeSecureClient)
    original = FakeSecureClient(wallet_type="DEPOSIT_WALLET", wallet="0xdeposit")
    ex = executor.Executor(original, "0xdeposit", settings(), SimpleNamespace(), False)

    result = await ex.setup_approvals(attempts=1)

    assert result == "fully approved"
    assert original.builder_created is True
    assert original.setup_calls == 0
    assert len(FakeSecureClient.create_calls) == 1
    call = FakeSecureClient.create_calls[0]
    assert call["private_key"] == settings().polymarket_private_key
    assert call["wallet"] == "0xdeposit"
    assert call["api_key"] is not None

    ephemeral = FakeSecureClient.created_clients[0]
    assert ephemeral.setup_calls == 1
    assert ephemeral.revoked is True
    assert ephemeral.closed is True


@pytest.mark.asyncio
async def test_eoa_approval_does_not_create_builder_key(monkeypatch):
    monkeypatch.setattr(executor, "AsyncSecureClient", FakeSecureClient)
    original = FakeSecureClient(wallet_type="EOA", wallet="0xeoa")

    async def must_not_create_key():
        raise AssertionError("EOA must not create a builder key")

    original.create_builder_api_key = must_not_create_key
    ex = executor.Executor(original, "0xeoa", settings(), SimpleNamespace(), False)

    result = await ex.setup_approvals(attempts=1)

    assert result == "fully approved"
    assert original.setup_calls == 1
    assert FakeSecureClient.create_calls == []


@pytest.mark.asyncio
async def test_gasless_temp_client_wallet_mismatch_fails_closed(monkeypatch):
    monkeypatch.setattr(executor, "AsyncSecureClient", FakeSecureClient)
    original = FakeSecureClient(wallet_type="DEPOSIT_WALLET", wallet="0xdeposit")
    ex = executor.Executor(original, "0xdeposit", settings(), SimpleNamespace(), False)

    async def mismatched_create(**kwargs):
        return FakeSecureClient(
            wallet_type="DEPOSIT_WALLET",
            wallet="0xdifferent",
            ephemeral=True,
        )

    monkeypatch.setattr(FakeSecureClient, "create", mismatched_create)

    with pytest.raises(RuntimeError, match="different wallet"):
        await ex.setup_approvals(attempts=1)
