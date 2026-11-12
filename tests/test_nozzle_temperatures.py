import json
from contextlib import asynccontextmanager

import httpx
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from app.models import Base, Filament, Manufacturer, Printer, Spool, SpoolStatus
from app.models.printer_params import FilamentPrinterParam, SpoolPrinterParam
from app.plugins.bambuddy import driver as bambuddy

MIN = "nozzle_temp_min"
MAX = "nozzle_temp_max"
BAMBU_MIN = "bambu_nozzle_temp_min"
BAMBU_MAX = "bambu_nozzle_temp_max"


@asynccontextmanager
async def connected_driver(handler, *, printer_id=1, mode="full"):
    driver = bambuddy.Driver(
        printer_id,
        {
            "bambuddy_url": "http://bambuddy.test",
            "printer_id": 7,
            "per_model_profiles": "disabled",
            "printer_write_mode": mode,
        },
        lambda _: None,
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        driver._client = client
        yield driver


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("temperatures", "live_material", "expected"),
    [
        ({BAMBU_MIN: "216.75", BAMBU_MAX: "236.25", MIN: 190, MAX: 230}, "PLA", (217, 236)),
        ({BAMBU_MIN: "218", BAMBU_MAX: 234}, "PLA", (218, 234)),
        ({BAMBU_MIN: "216.75"}, "PLA", (217, 240)),
        ({BAMBU_MIN: "216.75"}, "PETG", (217, 230)),
        ({BAMBU_MIN: "nan", BAMBU_MAX: "inf"}, "PETG", (190, 230)),
        pytest.param({BAMBU_MAX: "1e100"}, "PETG", (190, 230), id="oversized-temperature"),
        ({BAMBU_MIN: "-5", BAMBU_MAX: "-10"}, "PETG", (190, 230)),
        ({BAMBU_MIN: "250", BAMBU_MAX: "215", MIN: 205, MAX: 225}, "PLA", (205, 225)),
        ({BAMBU_MIN: "250"}, "PETG", (190, 230)),
        pytest.param(
            {BAMBU_MIN: "0", BAMBU_MAX: "0", MIN: 211}, "PLA", (211, 240),
            id="stored-zero-uses-caller-or-live",
        ),
        pytest.param(
            {BAMBU_MIN: "0", BAMBU_MAX: "0", MIN: 0, MAX: "0"}, "PLA", (190, 230),
            id="caller-zero-uses-required-defaults",
        ),
    ],
)
async def test_configure_sends_rounded_nozzle_temperatures(temperatures, live_material, expected):
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={})

    async with connected_driver(respond) as driver:
        driver._current_slots = [
            {"slot_index": "0-1", "tray_type": live_material, MIN: 210, MAX: 240}
        ]
        await driver._send_assignment(
            0, 1, {"material_type": "PLA", "color": "123456", **temperatures}
        )

    configure, = [
        req for req in requests
        if req.method == "POST" and req.url.path == "/api/v1/printers/7/slots/0/1/configure"
    ]
    assert (configure.url.params[MIN], configure.url.params[MAX]) == tuple(map(str, expected))


@pytest.mark.asyncio
async def test_inventory_only_skips_slot_configure():
    requests = []

    def respond(request):
        requests.append(request)
        return httpx.Response(200, json={})

    async with connected_driver(respond, mode="inventory_only") as driver:
        await driver._send_assignment(0, 1, {BAMBU_MIN: "216.75", BAMBU_MAX: "236.25"})

    assert requests == []


@asynccontextmanager
async def inventory_sessions(monkeypatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(bambuddy, "async_session_maker", sessions)
    monkeypatch.setattr(bambuddy.Driver, "_url_last_payloads", {})
    monkeypatch.setattr(bambuddy.Driver, "_url_bb_model_presets", {})
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        yield sessions
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_inventory_sync_resolves_nozzle_temperatures(monkeypatch):
    native_supported = hasattr(Filament, "printer_initializations")
    expected = {}
    posts = set()
    remote = {}
    async with inventory_sessions(monkeypatch) as sessions:
        async with sessions() as db:
            manufacturer = Manufacturer(name="Test")
            status = SpoolStatus(key="active", label="Active")
            printer = Printer(name="Bambuddy", driver_key="bambuddy")
            peer = Printer(name="Other", driver_key="bambuddy")
            db.add_all([manufacturer, status, printer, peer])
            await db.flush()

            async def add_spool(
                designation, want, *, filament_params=(), spool_params=(),
                native=None, bed=None, initialized=False, peer_initialized=False, remote_temps=None,
            ):
                filament = Filament(
                    manufacturer_id=manufacturer.id,
                    designation=designation,
                    material_type="PLA",
                    diameter_mm=1.75,
                )
                if native_supported:
                    from app.models.printer_params import FilamentPrinterInitialization

                    filament.extruder_temp_range_c = native
                    filament.bed_temp_range_c = bed
                    for target, marked in ((printer, initialized), (peer, peer_initialized)):
                        if marked:
                            filament.printer_initializations.append(
                                FilamentPrinterInitialization(printer_id=target.id)
                            )
                filament.printer_params = [
                    FilamentPrinterParam(printer_id=target.id, param_key=key, param_value=value)
                    for target, key, value in filament_params
                ]
                spool = Spool(filament=filament, status_id=status.id)
                spool.printer_params = [
                    SpoolPrinterParam(printer_id=target.id, param_key=key, param_value=value)
                    for target, key, value in spool_params
                ]
                db.add(spool)
                await db.flush()
                note = f"filaman:{spool.id}"
                expected[note] = want
                if remote_temps is not None:
                    bb_id = len(remote) + 1
                    minimum, maximum = remote_temps
                    remote[bb_id] = {"id": bb_id, "note": note, MIN: minimum, MAX: maximum}

            await add_spool(
                "Calibrated", (217, 236), initialized=True, remote_temps=(210, 230),
                filament_params=((printer, BAMBU_MIN, "217"), (printer, BAMBU_MAX, "236")),
                spool_params=((peer, BAMBU_MIN, "190"),),
            )
            await add_spool(
                "Override", (219, 236), initialized=True,
                filament_params=((printer, BAMBU_MIN, "217"), (printer, BAMBU_MAX, "236")),
                spool_params=((printer, BAMBU_MIN, "219.25"),),
            )
            await add_spool(
                "Blank override", (None, 236), initialized=True, remote_temps=(217, 240),
                filament_params=((printer, BAMBU_MIN, "217"), (printer, BAMBU_MAX, "236")),
                spool_params=((printer, BAMBU_MIN, ""),),
            )
            await add_spool(
                "Blank filament bound", (210, None), remote_temps=(210, 236),
                filament_params=((printer, BAMBU_MAX, None),),
            )
            await add_spool(
                "Remote-only temperatures", (204, 241), remote_temps=(204, 241),
            )
            await add_spool(
                "Explicit inventory zero", (0, 236), initialized=True,
                spool_params=((printer, BAMBU_MIN, "0"), (printer, BAMBU_MAX, "236")),
            )
            await add_spool(
                "Oversized temperature", (217, None),
                spool_params=((printer, BAMBU_MIN, "217"), (printer, BAMBU_MAX, "1e100")),
            )
            if native_supported:
                await add_spool(
                    "Uninitialized native", (217, 236), peer_initialized=True,
                    native={"min": 216.75, "max": 236.25},
                    filament_params=((peer, BAMBU_MAX, "220"),),
                )
                await add_spool(
                    "Cleared", (None, None), initialized=True, remote_temps=(217, 236),
                    native={"min": 216.75, "max": 236.25},
                )
                await add_spool(
                    "One-sided", (217, None), native={"min": 216.75, "max": None},
                )
                await add_spool(
                    "Compatible native partial update", (217, 243), remote_temps=(205, 243),
                    native={"min": 216.75, "max": None},
                )
                await add_spool(
                    "Incompatible native partial update", (205, 213), remote_temps=(205, 213),
                    native={"min": 216.75, "max": None},
                )
                await add_spool(
                    "Initialized one-sided update", (217, None), initialized=True,
                    native={"min": 216.75, "max": 236.25},
                    remote_temps=(205, 213), filament_params=((printer, BAMBU_MIN, "217"),),
                )
                await add_spool(
                    "Bed-only initialization", (204, 241), initialized=True,
                    bed={"min": 55, "max": 60}, remote_temps=(204, 241),
                )
                await add_spool(
                    "Initialized partial native ownership", (None, 243), initialized=True,
                    native={"min": 216.75, "max": None}, remote_temps=(205, 243),
                )
                await add_spool(
                    "Inverted native range", (None, None), native={"min": 250, "max": 215},
                )
                await add_spool(
                    "Native max below override", (240, None),
                    native={"min": 216.75, "max": 236.25},
                    spool_params=((printer, BAMBU_MIN, "240"),),
                )
                await add_spool(
                    "Oversized override preserves opposite native bound", (205, 236),
                    native={"min": 216.75, "max": 236.25},
                    spool_params=((printer, BAMBU_MIN, "1e100"),),
                    remote_temps=(205, 240),
                )
                await add_spool(
                    "Blank override masks native", (None, 236),
                    native={"min": 216.75, "max": 236.25},
                    spool_params=((printer, BAMBU_MIN, None),),
                )
            await db.commit()

        def respond(request):
            path = request.url.path
            if path == "/api/v1/inventory/spools":
                if request.method == "GET":
                    return httpx.Response(200, json=list(remote.values()))
                assert request.method == "POST"
                body = json.loads(request.content)
                spool_id = len(remote) + 1
                posts.add(body["note"])
                remote[spool_id] = {"id": spool_id, **body}
                return httpx.Response(200, json=remote[spool_id])
            if request.method == "PATCH" and path.startswith("/api/v1/inventory/spools/"):
                body = json.loads(request.content)
                spool_id = int(path.rsplit("/", 1)[1])
                # SpoolUpdate distinguishes an omitted field from explicit null.
                remote[spool_id].update(body)
                return httpx.Response(200, json=remote[spool_id])
            return httpx.Response(200, json={})

        existing_notes = {spool["note"] for spool in remote.values()}
        async with connected_driver(respond, printer_id=printer.id, mode="inventory_only") as driver:
            await driver._do_sync_inner()

        assert posts == expected.keys() - existing_notes
        assert {
            spool["note"]: (spool.get(MIN), spool.get(MAX)) for spool in remote.values()
        } == expected


@pytest.mark.asyncio
async def test_inventory_sync_retries_owned_temperature_on_the_same_remote_spool(monkeypatch):
    async with inventory_sessions(monkeypatch) as sessions:
        async with sessions() as db:
            printer = Printer(name="Bambuddy", driver_key="bambuddy")
            filament = Filament(
                manufacturer=Manufacturer(name="Test"), designation="PLA", material_type="PLA",
                diameter_mm=1.75,
            )
            spool = Spool(filament=filament, status=SpoolStatus(key="active", label="Active"))
            db.add(printer)
            await db.flush()
            spool.printer_params = [
                SpoolPrinterParam(printer_id=printer.id, param_key=BAMBU_MIN, param_value="216.75")
            ]
            db.add(spool)
            await db.commit()

        remote = {
            101: {"id": 101, "note": f"filaman:{spool.id}", MIN: 205, MAX: 240},
            102: {"id": 102, "note": f"filaman:{spool.id + 1}"},
        }
        patches = []

        def respond(request):
            path = request.url.path
            if path == "/api/v1/inventory/spools":
                assert request.method == "GET"
                return httpx.Response(200, json=list(remote.values()))
            if path.startswith("/api/v1/inventory/spools/"):
                remote_id = int(path.rsplit("/", 1)[1])
                if request.method == "DELETE":
                    del remote[remote_id]
                    return httpx.Response(200, json={})
                assert request.method == "PATCH"
                payload = json.loads(request.content)
                assert MAX not in payload
                patches.append(payload)
                if len(patches) == 1:
                    return httpx.Response(503)
                remote[remote_id].update(payload)
                return httpx.Response(200, json=remote[remote_id])
            return httpx.Response(200, json={})

        async with connected_driver(respond, printer_id=printer.id, mode="inventory_only") as driver:
            await driver._do_sync_inner()
            assert set(remote) == {101}
            assert remote[101][MIN] == 205
            assert spool.id not in driver._url_last_payloads.get("http://bambuddy.test", {})

            await driver._do_sync_inner()
            assert remote[101][MIN] == 217
            assert len(patches) == 2

            remote[101].update({MIN: 207, MAX: 245})
            await driver._do_sync_inner()
            assert (remote[101][MIN], remote[101][MAX]) == (217, 245)
            assert len(patches) == 3

            await driver._do_sync_inner()
            assert len(patches) == 3
