"""start() must return quickly; the Bambuddy round-trips run in the background.

FilaMan 2.2.2 gives a driver 10s to start. With several printers on one
Bambuddy the inventory sync and slot recovery took longer than that, so the
later printers were cancelled and left without a driver.
"""

import asyncio
import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

_PKG_DIR = Path(__file__).resolve().parents[1] / "bambuddy"
if "bambuddy" not in sys.modules:
    _pkg = types.ModuleType("bambuddy")
    _pkg.__path__ = [str(_PKG_DIR)]
    _pkg.__package__ = "bambuddy"
    sys.modules["bambuddy"] = _pkg

from bambuddy import driver as driver_mod  # noqa: E402
from bambuddy.driver import Driver  # noqa: E402


class _FakeClient:
    def __init__(self, *args, **kwargs):
        pass

    async def get(self, *args, **kwargs):
        return types.SimpleNamespace(status_code=404, json=lambda: {})

    async def aclose(self):
        pass


def _no_db():
    raise RuntimeError("no database in this test")


@pytest.fixture
def make_driver(monkeypatch):
    monkeypatch.setattr(driver_mod.httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(driver_mod, "async_session_maker", _no_db)
    made = []

    def make(url: str, calls: list[str], status_gate: asyncio.Event | None = None, sync_error=None):
        d = Driver(
            printer_id=len(made) + 900,
            config={"bambuddy_url": url, "api_key": "k", "printer_id": 1, "sync_enabled": "enabled"},
            emitter=lambda _payload: None,
        )

        async def status():
            calls.append("status")
            if status_gate is not None:
                await status_gate.wait()

        async def sync():
            calls.append("sync")
            if sync_error:
                raise sync_error

        async def recover():
            calls.append("recover")

        async def forever(name):
            calls.append(name)
            await asyncio.Event().wait()

        d._fetch_and_emit_status = status
        d._sync_all_spools = sync
        d._recover_slot_links = recover
        d._ws_loop = lambda: forever("ws")
        d._sync_inventory_loop = lambda: forever("sync_loop")
        d._sanitize_pfus_stored_as_bambu_idx = AsyncMock()
        d._sanitize_mismatched_bambu_idx = AsyncMock()
        d._legacy_fanout_mirrored_profiles = AsyncMock()
        made.append(d)
        return d

    yield make
    for d in made:
        Driver._url_instances.pop(d._bambuddy_url, None)
        Driver._url_startup_locks.pop(d._bambuddy_url, None)


@pytest.mark.asyncio
async def test_start_returns_before_bambuddy_answers(make_driver):
    calls: list[str] = []
    gate = asyncio.Event()
    d = make_driver("http://bb-slow", calls, status_gate=gate)

    await asyncio.wait_for(d.start(), timeout=1)
    await asyncio.sleep(0)

    assert calls == ["status"]
    assert d._ws_task is None
    assert d in Driver._url_instances["http://bb-slow"]

    await asyncio.wait_for(d.stop(), timeout=1)
    gate.set()
    await asyncio.sleep(0)
    assert d._ws_task is None
    assert calls == ["status"]


@pytest.mark.asyncio
async def test_background_startup_keeps_the_old_order(make_driver):
    calls: list[str] = []
    d = make_driver("http://bb-order", calls)

    await d.start()
    await asyncio.wait_for(d._startup_task, timeout=1)
    await asyncio.sleep(0)

    assert calls[:3] == ["status", "sync", "recover"]
    assert set(calls[3:]) == {"ws", "sync_loop"}
    await d.stop()


@pytest.mark.asyncio
async def test_websocket_still_starts_when_startup_sync_fails(make_driver):
    calls: list[str] = []
    d = make_driver("http://bb-fail", calls, sync_error=RuntimeError("bambuddy 500"))

    await d.start()
    await asyncio.wait_for(d._startup_task, timeout=1)
    await asyncio.sleep(0)

    assert "recover" not in calls
    assert d._ws_task is not None and not d._ws_task.done()
    await d.stop()


@pytest.mark.asyncio
async def test_second_driver_on_same_url_is_not_coordinator(make_driver):
    calls_a: list[str] = []
    calls_b: list[str] = []
    a = make_driver("http://bb-shared", calls_a)
    b = make_driver("http://bb-shared", calls_b)

    await a.start()
    await b.start()
    await asyncio.wait_for(asyncio.gather(a._startup_task, b._startup_task), timeout=1)
    await asyncio.sleep(0)

    assert "sync_loop" in calls_a
    assert "sync_loop" not in calls_b
    await b.stop()
    await a.stop()


@pytest.mark.asyncio
async def test_startups_on_one_bambuddy_run_one_at_a_time(make_driver):
    calls_a: list[str] = []
    calls_b: list[str] = []
    gate = asyncio.Event()
    a = make_driver("http://bb-queue", calls_a, status_gate=gate)
    b = make_driver("http://bb-queue", calls_b)

    await a.start()
    await b.start()
    for _ in range(5):
        await asyncio.sleep(0)
    # A is stuck fetching status; B must not start its status/sync/recover yet.
    assert calls_a == ["status"]
    assert calls_b == []

    gate.set()
    await asyncio.wait_for(asyncio.gather(a._startup_task, b._startup_task), timeout=1)
    assert calls_b[:3] == ["status", "sync", "recover"]
    await b.stop()
    await a.stop()


@pytest.mark.asyncio
async def test_stopping_a_queued_driver_skips_its_startup(make_driver):
    calls_a: list[str] = []
    calls_b: list[str] = []
    gate = asyncio.Event()
    a = make_driver("http://bb-cancel", calls_a, status_gate=gate)
    b = make_driver("http://bb-cancel", calls_b)

    await a.start()
    await b.start()
    await asyncio.sleep(0)
    await b.stop()
    gate.set()
    await asyncio.wait_for(a._startup_task, timeout=1)
    await asyncio.sleep(0)

    assert calls_b == []
    assert "ws" in calls_a
    await a.stop()
