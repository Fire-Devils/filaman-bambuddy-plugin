import pytest

from bambuddy.driver import Driver


@pytest.mark.asyncio
async def test_spool_usage_logged_maps_bambuddy_id_and_builds_idempotency_key():
    driver = object.__new__(Driver)
    driver._bambuddy_printer_id = 3
    driver.printer_id = 7
    driver._spoolman_enabled = False
    driver._spoolman_enabled = False
    driver._modern_usage_event_ids = set()
    driver._legacy_consumption_tasks = {}
    driver._slot_to_filaman_spool = {}
    calls = []
    driver._resolve_bambuddy_spool_id = lambda spool_id: _resolved(spool_id, 42)
    driver._report_consumption = lambda spool, grams, **kw: _record(calls, spool, grams, kw)

    await driver._handle_spool_usage_logged({
        "type": "spool_usage_logged",
        "printer_id": 3,
        "event_id": "bambuddy:3:job-1",
        "usage": [{"spool_id": 17, "weight_used": 12.4, "ams_id": 0, "tray_id": 2}],
    })

    assert calls == [(42, 12.4, {"source_event_key": "bambuddy:3:job-1:17:0:2"})]


@pytest.mark.asyncio
async def test_spool_usage_logged_ignores_invalid_weight_and_unsafe_mapping():
    driver = object.__new__(Driver)
    driver._bambuddy_printer_id = 3
    driver._spoolman_enabled = False
    driver._modern_usage_event_ids = set()
    driver._legacy_consumption_tasks = {}
    driver._slot_to_filaman_spool = {}
    driver._resolve_bambuddy_spool_id = lambda _: _resolved(None, None)
    calls = []
    driver._report_consumption = lambda *args, **kw: _record(calls, *args, **kw)

    await driver._handle_spool_usage_logged({
        "printer_id": 3,
        "event_id": "event",
        "usage": [
            {"spool_id": 1, "weight_used": 0, "ams_id": 0, "tray_id": 0},
            {"spool_id": 2, "weight_used": "NaN", "ams_id": 0, "tray_id": 1},
            {"spool_id": 3, "weight_used": 5, "ams_id": 1, "tray_id": 1},
        ],
    })

    assert calls == []


async def _resolved(value, _expected):
    return value


async def _record(calls, spool, grams, kw):
    calls.append((spool, grams, kw))
