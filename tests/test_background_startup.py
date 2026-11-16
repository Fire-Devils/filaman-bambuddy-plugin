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


@pytest.mark.asyncio
async def test_stopping_coordinator_hands_sync_loop_to_queued_peer(make_driver):
    calls_a: list[str] = []
    calls_b: list[str] = []
    gate = asyncio.Event()
    a = make_driver("http://bb-handoff", calls_a, status_gate=gate)
    b = make_driver("http://bb-handoff", calls_b)

    await a.start()
    await b.start()
    # Health reports healthy + starting while queued on the URL startup lock.
    assert a.health()["tasks_healthy"] is True
    assert a.health()["task_status"]["starting"] is True
    assert b.health()["tasks_healthy"] is True
    assert b.health()["task_status"]["starting"] is True

    for _ in range(5):
        await asyncio.sleep(0)
    await a.stop()
    gate.set()

    await asyncio.wait_for(b._startup_task, timeout=1)
    await asyncio.sleep(0)

    assert calls_b[:3] == ["status", "sync", "recover"]
    assert calls_b.count("sync_loop") == 1
    assert b.health()["tasks_healthy"] is True
    assert b.health()["task_status"]["starting"] is False
    await b.stop()


@pytest.mark.asyncio
async def test_startup_reemits_cached_status_after_slot_recovery(monkeypatch):
    monkeypatch.setattr(driver_mod.httpx, "AsyncClient", _FakeClient)
    monkeypatch.setattr(driver_mod, "async_session_maker", _no_db)
    emitted: list[dict] = []
    status_payload = {
        "connected": True,
        "ams": [{"id": 0, "tray": [{"id": 1, "tray_type": "PLA", "tray_color": "112233FF"}]}],
    }

    d = Driver(
        printer_id=950,
        config={"bambuddy_url": "http://bb-reemit", "api_key": "k", "printer_id": 1, "sync_enabled": "enabled"},
        emitter=emitted.append,
    )

    async def fake_status():
        d._last_bambuddy_status = dict(status_payload)
        d._process_slots(status_payload)

    async def fake_recover():
        d._slot_to_filaman_spool["0-1"] = 42
        d._slot_cache_restored = True

    async def forever():
        await asyncio.Event().wait()

    d._fetch_and_emit_status = fake_status
    d._sync_all_spools = AsyncMock()
    d._recover_slot_links = fake_recover
    d._ws_loop = forever
    d._sync_inventory_loop = forever
    d._sanitize_pfus_stored_as_bambu_idx = AsyncMock()
    d._sanitize_mismatched_bambu_idx = AsyncMock()
    d._legacy_fanout_mirrored_profiles = AsyncMock()

    try:
        await d.start()
        await asyncio.wait_for(d._startup_task, timeout=1)
        await asyncio.sleep(0)

        assert len(emitted) == 2
        # First emit before recovery strips spool_id; second emit right after recovery includes it.
        assert "spool_id" not in emitted[0]["slots"][0]
        assert emitted[1]["slots"][0]["spool_id"] == 42
    finally:
        await d.stop()
        Driver._url_instances.pop("http://bb-reemit", None)
        Driver._url_startup_locks.pop("http://bb-reemit", None)


@pytest.mark.asyncio
async def test_restore_slot_cache_keeps_filaman_ams_owners_and_tolerates_bad_params(monkeypatch):
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
    from sqlalchemy.pool import StaticPool

    from app.models import Base, Filament, Location, Manufacturer, Printer, Spool, SpoolPrinterParam, SpoolStatus

    engine = create_async_engine(
        "sqlite+aiosqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    session_maker = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(driver_mod, "async_session_maker", session_maker)

    async with session_maker() as db:
        active = SpoolStatus(key="new", label="New", sort_order=1)
        archived = SpoolStatus(key="archived", label="Archived", sort_order=99)
        mfg = Manufacturer(name="Maker")
        db.add_all([active, archived, mfg])
        await db.flush()
        fil = Filament(manufacturer_id=mfg.id, designation="PLA", material_type="PLA", diameter_mm=1.75)
        printer = Printer(id=77, name="P1S", driver_key="bambuddy")
        slot_keep = Location(name="AMS 1 - Slot 1", identifier="bambuddy_77_0_0", custom_fields={"managed_by": "bambuddy_plugin"})
        slot_cleared = Location(name="Shelf", identifier=None)
        slot_archived = Location(name="AMS 1 - Slot 3", identifier="bambuddy_77_0_2", custom_fields={"managed_by": "bambuddy_plugin"})
        db.add_all([fil, printer, slot_keep, slot_cleared, slot_archived])
        await db.flush()

        spool_keep = Spool(filament_id=fil.id, status_id=active.id, location_id=slot_keep.id)
        spool_cleared = Spool(filament_id=fil.id, status_id=active.id, location_id=slot_cleared.id)
        spool_archived = Spool(filament_id=fil.id, status_id=archived.id, location_id=slot_archived.id)
        db.add_all([spool_keep, spool_cleared, spool_archived])
        await db.flush()

        db.add_all([
            # Corrupt non-numeric param rows must not abort recovery.
            SpoolPrinterParam(spool_id=spool_keep.id, printer_id=77, param_key="bambuddy_spool_id", param_value="not-a-number"),
            SpoolPrinterParam(spool_id=spool_keep.id, printer_id=77, param_key="original_location_id", param_value="bogus"),
            # Stale Bambuddy assignment whose FilaMan location moved to Shelf.
            SpoolPrinterParam(spool_id=spool_cleared.id, printer_id=77, param_key="bambuddy_spool_id", param_value="502"),
        ])
        await db.commit()
        keep_id = spool_keep.id

    emitted: list[dict] = []
    d = Driver(
        printer_id=77,
        config={"bambuddy_url": "http://bb-db", "api_key": "k", "printer_id": 1, "sync_enabled": "enabled"},
        emitter=emitted.append,
    )
    d._client = _FakeClient()  # type: ignore[assignment]
    # Bambuddy returns only the stale assignment (missing slot 0-0), and backfill POST fails.
    d._bb_get = AsyncMock(return_value=[{"ams_id": 0, "tray_id": 1, "spool_id": 502}])
    d._bb_post = AsyncMock(side_effect=RuntimeError("POST 500"))

    try:
        await d._recover_slot_links()
        assert d._slot_cache_restored is True
        # Slot 0-0 kept from FilaMan DB despite missing Bambuddy assignment + failed backfill;
        # slot 0-1 dropped because spool moved to Shelf; slot 0-2 ignored because archived.
        assert d._slot_to_filaman_spool == {"0-0": keep_id}

        d._process_slots({
            "connected": True,
            "ams": [{"id": 0, "tray": [
                {"id": 0, "tray_type": "PLA", "tray_color": "FFFFFFFF"},
                {"id": 1, "tray_type": "PLA", "tray_color": "000000FF"},
            ]}],
        })
        slots_by_idx = {s["slot_index"]: s for s in emitted[-1]["slots"]}
        assert slots_by_idx["0-0"]["spool_id"] == keep_id
        assert slots_by_idx["0-1"]["spool_id"] is None
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_recover_slot_links_if_needed_swallows_errors_and_missing_printer_id():
    d_no_pid = Driver(
        printer_id=960,
        config={"bambuddy_url": "http://bb-nopid", "api_key": "k", "sync_enabled": "enabled"},
        emitter=lambda _p: None,
    )
    d_no_pid._client = _FakeClient()  # type: ignore[assignment]
    await d_no_pid._restore_slot_cache_from_assignments()
    assert d_no_pid._slot_cache_restored is True

    d_err = Driver(
        printer_id=961,
        config={"bambuddy_url": "http://bb-err", "api_key": "k", "printer_id": 1, "sync_enabled": "enabled"},
        emitter=lambda _p: None,
    )
    d_err._running = True
    d_err._fetch_and_emit_status = AsyncMock(side_effect=RuntimeError("boom"))
    # Must not raise into the WebSocket handler.
    await d_err._recover_slot_links_if_needed()
    assert d_err._slot_cache_restored is False
    Driver._url_startup_locks.pop("http://bb-err", None)

