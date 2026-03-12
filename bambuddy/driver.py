"""Bambuddy driver: bidirektionale FilaMan↔Bambuddy Synchronisation.

Flows:
1. Inventory Sync (FilaMan → Bambuddy):
   - Auf Start + periodisch: FilaMan-Spulen → Bambuddy-Inventory (create/update/delete)
   - Nach CREATE: Bambuddy-Spool-ID direkt als SpoolPrinterParam in FilaMan-DB speichern
     (param_key="bambuddy_spool_id") — kein HTTP-Umweg, da Plugin intern läuft

2. Tray-Konfiguration (FilaMan → Bambuddy):
   - Primär: POST /api/v1/inventory/assignments (wenn bambuddy_spool_id bekannt)
   - Fallback: POST /slots/{a}/{t}/configure (bei erstem Start vor Sync)

3. Verbrauchsmeldung (Bambuddy → FilaMan):
   - WebSocket-Verbindung zu Bambuddy
   - print_complete-Event → SpoolService.record_consumption() direkt in FilaMan-DB
"""
import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any, Callable

import httpx
from sqlalchemy import delete, event, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.core.database import async_session_maker
from app.models.filament import Filament, FilamentColor
from app.models.printer_params import SpoolPrinterParam
from app.models.spool import Spool, SpoolStatus
from app.services.spool_service import SpoolService

try:
    import websockets
    import websockets.exceptions
except ImportError:  # pragma: no cover
    websockets = None  # type: ignore[assignment]

from app.plugins.base import BaseDriver

logger = logging.getLogger(__name__)

# Generische Bambu-Slicer-IDs für gängige Materialien (Fallback wenn kein bambu_idx gesetzt)
_GENERIC_SLICER_IDS: dict[str, str] = {
    "PLA":   "GFL99",
    "PETG":  "GFG99",
    "ABS":   "GFB99",
    "ASA":   "GFB98",
    "TPU":   "GFU99",
    "NYLON": "GFN99",
    "PA":    "GFN99",
    "PVA":   "GFS99",
    "HIPS":  "GFS98",
    "PC":    "GFC99",
    "PP":    "GFP97",
}


def _int_or_none(v: Any) -> int | None:
    """Konvertiert einen Wert zu int, gibt None zurück bei leerem/ungültigem Wert."""
    try:
        return int(v) if v not in (None, "") else None
    except (ValueError, TypeError):
        return None


def _float_or_none(v: Any) -> float | None:
    """Konvertiert einen Wert zu float, gibt None zurück bei leerem/ungültigem Wert."""
    try:
        return float(v) if v not in (None, "") else None
    except (ValueError, TypeError):
        return None


def _extract_bambu_idx(preset_id: str) -> str:
    """Extrahiert bambu_idx aus Bambuddy preset_id.

    Bambuddy liefert z.B. 'builtin_GFA01' → extrahiert 'GFA01' (Teil nach erstem '_').
    Enthält preset_id kein Unterstriche, wird der gesamte Wert zurückgegeben.
    """
    if not preset_id:
        return ""
    return preset_id.split("_", 1)[1] if "_" in preset_id else preset_id


class Driver(BaseDriver):
    """Bambuddy-Driver mit bidirektionaler FilaMan↔Bambuddy Synchronisation.

    FilaMan ist die Quelle der Wahrheit für Spulen und Filamente.
    Der Driver synchronisiert Spulen automatisch in das Bambuddy-Inventory
    und empfängt Verbrauchsdaten nach Druckabschluss via WebSocket.
    """

    driver_key = "bambuddy"

    def __init__(
        self,
        printer_id: int,
        config: dict[str, Any],
        emitter: Callable[[dict[str, Any]], None],
    ):
        super().__init__(printer_id, config, emitter)

        # -- Bambuddy-Verbindung --
        self._bambuddy_url = config.get("bambuddy_url", "").rstrip("/")
        self._api_key = config.get("api_key", "")
        self._bambuddy_printer_id = config.get("printer_id")
        self._headers = {"X-API-Key": self._api_key}
        self._client: httpx.AsyncClient | None = None

        # -- Sync/Reconnect-Einstellungen --
        self._sync_interval: int = int(config.get("sync_interval_seconds", 300))
        self._reconnect_interval: int = int(config.get("reconnect_interval_seconds", 30))

        # -- Background Tasks --
        self._ws_task: asyncio.Task | None = None
        self._sync_task: asyncio.Task | None = None

        # -- Verbindungs-Status --
        self._connected: bool = False

        # -- Status-Cache --
        self._current_slots: list[dict[str, Any]] = []
        self._current_ams_units: list[dict[str, Any]] = []
        # Cache für Bambu-Parameter (nozzle temps, k_value etc.) pro Slot
        self._slot_params_cache: dict[str, dict] = {}
        # Slot-Key ("ams_id-tray_id") → FilaMan-Spool-ID (für Verbrauchsmeldungen)
        self._slot_to_filaman_spool: dict[str, int] = {}
        # Letzte Sync-Statistik
        self._last_sync_count: int = 0
        self._last_sync_error: str | None = None

        # Sofortiger Push: Guard gegen Endlosschleife + Debounce-Task
        self._syncing: bool = False
        self._debounce_task: asyncio.Task | None = None

    # -- Lifecycle ------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._client = httpx.AsyncClient(headers=self._headers, timeout=15.0)

        # Initialen AMS-Status laden
        await self._fetch_and_emit_status()

        # Inventory-Sync: FilaMan → Bambuddy
        await self._sync_all_spools()

        # Background-Tasks starten
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._sync_task = asyncio.create_task(self._sync_inventory_loop())

        # DB-Event-Listener für sofortigen Push registrieren
        event.listen(AsyncSession, "after_commit", self._on_session_commit)

        logger.info(
            f"Bambuddy driver started for FilaMan printer {self.printer_id} "
            f"(Bambuddy printer_id={self._bambuddy_printer_id})"
        )

    async def stop(self) -> None:
        # DB-Event-Listener entfernen
        try:
            event.remove(AsyncSession, "after_commit", self._on_session_commit)
        except Exception:
            pass
        self._running = False
        for task in (self._ws_task, self._sync_task):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._ws_task = self._sync_task = None
        self._connected = False
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info(f"Bambuddy driver stopped for printer {self.printer_id}")

    # -- Sofortiger Push (SQLAlchemy Event-Listener) -------------------------

    def _on_session_commit(self, session: AsyncSession) -> None:
        """Reagiert auf jeden DB-Commit im Prozess (synchron, im Event-Loop-Thread).

        Ignoriert eigene Commits während eines laufenden Syncs (_syncing-Guard).
        Debounct mehrfache Commits innerhalb von 3 Sekunden zu einem einzelnen Sync.
        """
        if not self._running or not self._connected or self._syncing:
            return
        if self._debounce_task and not self._debounce_task.done():
            self._debounce_task.cancel()
        self._debounce_task = asyncio.create_task(self._debounced_sync())

    async def _debounced_sync(self) -> None:
        """Wartet 3 Sekunden auf weitere Commits, dann Inventory-Sync."""
        await asyncio.sleep(3)
        if self._running and self._connected and not self._syncing:
            logger.debug(f"Data change detected, triggering inventory sync for printer {self.printer_id}")
            await self._sync_all_spools()

    # -- Bambuddy HTTP-Helfer ------------------------------------------------

    async def _bb_get(self, path: str, **kwargs: Any) -> Any:
        assert self._client
        r = await self._client.get(f"{self._bambuddy_url}{path}", **kwargs)
        r.raise_for_status()
        return r.json()

    async def _bb_post(self, path: str, json_body: dict) -> Any:
        assert self._client
        r = await self._client.post(f"{self._bambuddy_url}{path}", json=json_body)
        r.raise_for_status()
        return r.json()

    async def _bb_patch(self, path: str, json_body: dict) -> Any:
        assert self._client
        r = await self._client.patch(f"{self._bambuddy_url}{path}", json=json_body)
        r.raise_for_status()
        return r.json()

    async def _bb_delete(self, path: str) -> None:
        assert self._client
        r = await self._client.delete(f"{self._bambuddy_url}{path}")
        r.raise_for_status()

    # -- FilaMan DB-Helfer (direkte SQLAlchemy-Zugriffe, kein HTTP) ----------

    async def _fetch_fm_spools(self) -> list[Spool]:
        """Holt alle nicht-archivierten FilaMan-Spulen aus der DB."""
        async with async_session_maker() as db:
            result = await db.execute(
                select(Spool)
                .join(SpoolStatus)
                .where(SpoolStatus.key != "archived")
                .options(
                    selectinload(Spool.filament).selectinload(Filament.manufacturer),
                    selectinload(Spool.filament).selectinload(
                        Filament.filament_colors
                    ).selectinload(FilamentColor.color),
                    selectinload(Spool.printer_params),
                )
            )
            return list(result.scalars().all())

    async def _store_bambuddy_id_db(
        self, filaman_spool_id: int, bambuddy_spool_id: int
    ) -> None:
        """Speichert Bambuddy-Spool-ID als SpoolPrinterParam direkt in FilaMan-DB.

        Danach liefert enrich_filament_data() automatisch bambuddy_spool_id
        in filament_data["printer_params"], sodass send_filament_to_tray()
        nur die ID an die Bambuddy-Assignment-API schicken muss.
        """
        async with async_session_maker() as db:
            result = await db.execute(
                select(SpoolPrinterParam).where(
                    SpoolPrinterParam.spool_id == filaman_spool_id,
                    SpoolPrinterParam.printer_id == self.printer_id,
                    SpoolPrinterParam.param_key == "bambuddy_spool_id",
                )
            )
            existing = result.scalar_one_or_none()
            if existing:
                existing.param_value = str(bambuddy_spool_id)
            else:
                db.add(SpoolPrinterParam(
                    spool_id=filaman_spool_id,
                    printer_id=self.printer_id,
                    param_key="bambuddy_spool_id",
                    param_value=str(bambuddy_spool_id),
                ))
            await db.commit()

    async def _report_consumption_db(
        self, filaman_spool_id: int, delta_g: float
    ) -> None:
        """Meldet Verbrauch direkt über SpoolService in FilaMan-DB."""
        async with async_session_maker() as db:
            spool = await db.get(Spool, filaman_spool_id)
            if not spool:
                logger.warning(
                    f"FilaMan spool {filaman_spool_id} not found for consumption report"
                )
                return
            service = SpoolService(db)
            _, remaining = await service.record_consumption(
                spool=spool,
                delta_weight_g=delta_g,
                event_at=datetime.now(timezone.utc),
                principal=None,
                source="bambuddy",
            )
            logger.info(
                f"Recorded {delta_g:.1f}g consumption for FilaMan spool {filaman_spool_id} "
                f"(remaining: {remaining}g)"
            )

    # -- Inventory Sync (FilaMan → Bambuddy) ---------------------------------

    def _map_spool(self, spool: Spool) -> dict:
        """FilaMan Spool (ORM) → Bambuddy SpoolCreate/Update-Payload.

        Mapping:
          filament.material_type                    → material
          filament.manufacturer.name                → brand
          filament.filament_colors[0].color.hex_code→ rgba (8-stellig RRGGBBAA)
          initial_total_weight_g                    → label_weight
          initial - remaining                       → weight_used
          rfid_uid                                  → tag_uid
          "filaman:{id}"                            → note (Reverse-Lookup-Schlüssel)
          printer_params bambu_idx                  → slicer_filament
          printer_params bambu_nozzle_*             → nozzle_temp_min/max
        """
        fil = spool.filament
        manufacturer_name = (fil.manufacturer.name if fil and fil.manufacturer else None)
        colors = (fil.filament_colors if fil else [])

        # Farbe: FilaMan 6-stellig hex → 8-stellig RRGGBBAA
        raw_color = "FFFFFF"
        if colors:
            raw_color = (colors[0].color.hex_code or "FFFFFF").lstrip("#")
        if len(raw_color) == 6:
            rgba = (raw_color + "FF").upper()
        elif len(raw_color) == 8:
            rgba = raw_color.upper()
        else:
            rgba = "FFFFFFFF"

        initial_weight = float(spool.initial_total_weight_g or 1000.0)
        remaining = spool.remaining_weight_g
        weight_used = max(0.0, initial_weight - float(remaining)) if remaining is not None else 0.0

        # printer_params als {key: value} dict aus der Relationship
        pp: dict[str, str | None] = {
            p.param_key: p.param_value
            for p in (spool.printer_params or [])
            if p.printer_id == self.printer_id
        }

        payload: dict[str, Any] = {
            "material":      (fil.material_type if fil else "PLA") or "PLA",
            "brand":         manufacturer_name,
            "rgba":          rgba,
            "label_weight":  int(initial_weight),
            "weight_used":   round(weight_used, 2),
            "weight_locked": False,
            "note":          f"filaman:{spool.id}",
        }

        if spool.rfid_uid:
            payload["tag_uid"] = spool.rfid_uid

        if slicer := pp.get("bambu_idx") or pp.get("bambu_tray_idx"):
            payload["slicer_filament"] = slicer

        if (nozzle_min := _int_or_none(pp.get("bambu_nozzle_temp_min"))) is not None:
            payload["nozzle_temp_min"] = nozzle_min

        if (nozzle_max := _int_or_none(pp.get("bambu_nozzle_temp_max"))) is not None:
            payload["nozzle_temp_max"] = nozzle_max

        return payload

    async def _sync_all_spools(self) -> None:
        """Synchronisiert alle aktiven FilaMan-Spulen ins Bambuddy-Inventory.

        Ablauf:
        1. Alle FilaMan-Spulen holen (GET /api/v1/spools)
        2. Alle Bambuddy-Spulen mit note="filaman:*" indexieren
        3. CREATE oder UPDATE je nach ob note-Eintrag bereits existiert
        4. Bei CREATE: Bambuddy-Spool-ID als printer_param in FilaMan speichern
        5. Bambuddy-Spulen löschen, die in FilaMan nicht mehr existieren
        """
        if not self._client:
            return
        self._syncing = True
        try:
            # 1. FilaMan-Spulen direkt aus DB holen
            fm_spools: list[Spool] = await self._fetch_fm_spools()

            # 2. Bambuddy note-Index: {"filaman:42": {id: ..., ...}}
            bb_spools: list[dict] = await self._bb_get("/api/v1/inventory/spools")
            note_index: dict[str, dict] = {
                s["note"]: s
                for s in bb_spools
                if (s.get("note") or "").startswith("filaman:")
            }

            synced_fm_ids: set[int] = set()

            for fm_spool in fm_spools:
                fm_id = fm_spool.id
                note_key = f"filaman:{fm_id}"
                payload = self._map_spool(fm_spool)

                try:
                    if note_key in note_index:
                        bb_id = note_index[note_key]["id"]
                        await self._bb_patch(
                            f"/api/v1/inventory/spools/{bb_id}", payload
                        )
                        # Sicherstellen, dass bambuddy_spool_id auch bei bestehenden
                        # Spulen in der FilaMan-DB vorhanden ist (idempotentes UPSERT).
                        await self._store_bambuddy_id_db(fm_id, bb_id)
                    else:
                        response = await self._bb_post(
                            "/api/v1/inventory/spools", payload
                        )
                        bb_id = response["id"]
                        # Bambuddy-Spool-ID direkt in FilaMan-DB speichern
                        await self._store_bambuddy_id_db(fm_id, bb_id)
                        logger.info(
                            f"Created Bambuddy spool {bb_id} for FilaMan spool {fm_id}"
                        )
                    synced_fm_ids.add(fm_id)
                except Exception as e:
                    logger.warning(f"Failed to sync FilaMan spool {fm_id}: {e}")

            # 4. Veraltete Bambuddy-Spulen entfernen
            for note_key, bb_spool in note_index.items():
                try:
                    fm_id = int(note_key.removeprefix("filaman:"))
                    if fm_id not in synced_fm_ids:
                        await self._bb_delete(
                            f"/api/v1/inventory/spools/{bb_spool['id']}"
                        )
                        logger.info(
                            f"Deleted Bambuddy spool {bb_spool['id']} "
                            f"(FilaMan spool {fm_id} no longer active)"
                        )
                except Exception as e:
                    logger.warning(f"Failed to delete orphaned Bambuddy spool: {e}")

            self._last_sync_count = len(synced_fm_ids)
            self._last_sync_error = None
            logger.info(
                f"Inventory sync complete: {len(synced_fm_ids)} spools synced "
                f"to Bambuddy printer {self._bambuddy_printer_id}"
            )

        except Exception as e:
            self._last_sync_error = str(e)
            logger.error(f"Inventory sync failed for printer {self.printer_id}: {e}")
        finally:
            self._syncing = False


    async def _sync_inventory_loop(self) -> None:
        """Periodischer Inventory-Sync alle sync_interval_seconds."""
        while self._running:
            await asyncio.sleep(self._sync_interval)
            if self._running:
                await self._sync_all_spools()

    async def trigger_sync(self) -> None:
        """Manueller sofortiger Inventory-Sync (Drucker-Action)."""
        await self._sync_all_spools()

    async def full_resync(self) -> None:
        """Löscht ALLE Bambuddy-Inventarspulen und synchronisiert neu aus FilaMan."""
        if not self._client:
            raise RuntimeError("Driver not connected")
        logger.info(f"Full resync started for printer {self.printer_id}")
        # 1. ALLE Bambuddy-Inventarspulen löschen (keine Filterung nach filaman:)
        bb_spools = await self._bb_get("/api/v1/inventory/spools")
        for spool in bb_spools:
            await self._bb_delete(f"/api/v1/inventory/spools/{spool['id']}")
        logger.info(f"Deleted {len(bb_spools)} Bambuddy spools")
        # 2. bambuddy_spool_id-Params aus FilaMan-DB löschen
        async with async_session_maker() as db:
            await db.execute(
                delete(SpoolPrinterParam).where(
                    SpoolPrinterParam.printer_id == self.printer_id,
                    SpoolPrinterParam.param_key == "bambuddy_spool_id",
                )
            )
            await db.commit()
        # 3. Neu synchronisieren
        await self._sync_all_spools()
        logger.info(f"Full resync complete for printer {self.printer_id}")

    # -- WebSocket (Bambuddy → FilaMan Verbrauchsmeldung) --------------------

    async def _ws_loop(self) -> None:
        """WebSocket-Verbindung zu Bambuddy mit automatischem Reconnect."""
        if websockets is None:
            logger.warning(
                "websockets package not installed — WebSocket disabled. "
                "Install with: pip install websockets>=12.0"
            )
            return

        while self._running:
            uri = (
                self._bambuddy_url.rstrip("/")
                .replace("https://", "wss://")
                .replace("http://", "ws://")
            ) + "/api/v1/ws"
            try:
                async with websockets.connect(
                    uri,
                    additional_headers={"X-API-Key": self._api_key},
                    ping_interval=30,
                    ping_timeout=10,
                ) as ws:
                    self._connected = True
                    logger.info(f"WebSocket connected: {uri}")
                    async for message in ws:
                        if not self._running:
                            break
                        try:
                            event = json.loads(message)
                            await self._handle_ws_event(event)
                        except Exception as e:
                            logger.warning(f"WS message handling error: {e}")
            except Exception as e:
                self._connected = False
                if self._running:
                    logger.warning(
                        f"WebSocket disconnected ({e}). "
                        f"Reconnecting in {self._reconnect_interval}s…"
                    )
                    await asyncio.sleep(self._reconnect_interval)

    async def _handle_ws_event(self, event: dict) -> None:
        """Verarbeitet eingehende Bambuddy WebSocket-Events."""
        event_type = event.get("type")

        if event_type == "printer_status":
            data = event.get("data", {})
            if event.get("printer_id") == self._bambuddy_printer_id:
                self._process_slots(data)

        elif event_type == "print_complete":
            data = event.get("data", {})
            if data.get("printer_id") == self._bambuddy_printer_id:
                await self._handle_print_complete(data)

    async def _handle_print_complete(self, data: dict) -> None:
        """Meldet Filament-Verbrauch nach Druckende an FilaMan.

        Das `weight_used`-Feld des Events kann sein:
        - float  → Gesamtgewicht aller Filamente
        - dict   → {"ams_id-tray_id": weight_g, ...} per Slot

        Für die Zuordnung Slot → FilaMan-Spool-ID wird der in-memory Cache
        `_slot_to_filaman_spool` genutzt, der bei jeder Tray-Zuweisung
        (`send_filament_to_tray`) aktualisiert wird.
        """
        weight_used = data.get("weight_used")
        if not weight_used:
            return

        if isinstance(weight_used, dict):
            # Per-Slot: {"0-0": 12.5, "0-1": 8.3, ...}
            for slot_key, weight_g in weight_used.items():
                filaman_spool_id = self._slot_to_filaman_spool.get(str(slot_key))
                if filaman_spool_id and float(weight_g) > 0:
                    await self._report_consumption(filaman_spool_id, float(weight_g))

        elif isinstance(weight_used, (int, float)) and float(weight_used) > 0:
            # Gesamtgewicht: nur melden wenn genau ein Slot aktiv
            active_slots = list(self._slot_to_filaman_spool.items())
            if len(active_slots) == 1:
                _, filaman_spool_id = active_slots[0]
                await self._report_consumption(filaman_spool_id, float(weight_used))
            elif len(active_slots) > 1:
                logger.debug(
                    f"print_complete: total weight {weight_used}g but {len(active_slots)} "
                    f"active slots — cannot split accurately, skipping consumption report"
                )

    async def _report_consumption(self, filaman_spool_id: int, delta_g: float) -> None:
        """Meldet delta_g Verbrauch direkt über SpoolService in FilaMan-DB."""
        try:
            await self._report_consumption_db(filaman_spool_id, delta_g)
        except Exception as e:
            logger.warning(
                f"Failed to report consumption for FilaMan spool {filaman_spool_id}: {e}"
            )

    # -- Tray-Konfiguration (FilaMan → Bambuddy) ------------------------------

    def send_filament_to_tray(self, ams_id: int, tray_id: int, filament_data: dict) -> None:
        """Weist FilaMan-Spule einem Bambuddy-AMS-Slot zu."""
        asyncio.create_task(self._assign_or_configure(ams_id, tray_id, filament_data))

    async def _assign_or_configure(
        self, ams_id: int, tray_id: int, filament_data: dict
    ) -> None:
        """Primär: Assignment-API (wenn bambuddy_spool_id gesetzt). Fallback: configure-Call.

        Beim ersten Start (vor dem Inventory-Sync) kennt FilaMan die bambuddy_spool_id
        noch nicht → Fallback auf den configure-Endpoint (alle Felder einzeln).
        Nach dem Sync ist bambuddy_spool_id via enrich_filament_data() verfügbar
        → einfacher Assignment-Call genügt (Bambuddy konfiguriert AMS automatisch).
        """
        bambuddy_spool_id = _int_or_none(filament_data.get("bambuddy_spool_id"))
        filaman_spool_id  = _int_or_none(filament_data.get("id"))
        slot_key = f"{ams_id}-{tray_id}"

        if bambuddy_spool_id and self._client:
            try:
                response = await self._bb_post(
                    "/api/v1/inventory/assignments",
                    {
                        "spool_id":   bambuddy_spool_id,
                        "printer_id": self._bambuddy_printer_id,
                        "ams_id":     ams_id,
                        "tray_id":    tray_id,
                    },
                )
                self.log_debug(
                    "out",
                    f"POST /api/v1/inventory/assignments",
                    {
                        "spool_id":   bambuddy_spool_id,
                        "printer_id": self._bambuddy_printer_id,
                        "ams_id":     ams_id,
                        "tray_id":    tray_id,
                        "configured": response.get("configured"),
                    },
                )
                logger.info(
                    f"Assigned Bambuddy spool {bambuddy_spool_id} to "
                    f"printer {self._bambuddy_printer_id} AMS {ams_id}/{tray_id} "
                    f"(auto-configured={response.get('configured', False)})"
                )
                # Slot-Spool-Cache für Verbrauchsmeldung nach Druckende aktualisieren
                if filaman_spool_id:
                    self._slot_to_filaman_spool[slot_key] = filaman_spool_id
                return
            except Exception as e:
                logger.warning(
                    f"Assignment API failed (slot {ams_id}/{tray_id}), "
                    f"falling back to configure-call: {e}"
                )

        # Fallback: Direkter configure-Call mit allen Feldern
        await self._send_assignment(ams_id, tray_id, filament_data)

        # Slot-Spool-Cache auch beim Fallback aktualisieren
        if filaman_spool_id:
            self._slot_to_filaman_spool[slot_key] = filaman_spool_id

    # -- Direkter configure-Call (Fallback) ----------------------------------

    async def _send_assignment(self, ams_id: int, tray_id: int, filament_data: dict) -> None:
        """Konfiguriert einen AMS-Slot direkt über Bambuddys configure-Endpunkt.

        Sendet einen einzigen API-Call an Bambuddy, der sowohl ams_filament_setting
        als auch extrusion_cali_sel via MQTT an den Drucker weiterleitet.

        Bambu-spezifische Felder (aus printer_params via enrich_filament_data):
        - bambu_idx → slicer_filament (tray_info_idx im Bambu-MQTT)
        - bambu_tray_idx → Fallback für slicer_filament
        - bambu_nozzle_temp_min/max → nozzle_temp_min/max
        - material_subgroup → tray_sub_brands
        - bambu_k_value → k_value (0.0 = skip)
        - bambu_cali_idx, bambu_setting_id → cali_idx, setting_id
        """
        if not self._client:
            logger.error("Cannot send assignment: HTTP client not initialized")
            return

        # -- Farbe normalisieren: Bambuddy erwartet 8-stelliges RRGGBBAA --
        color = filament_data.get("color", "FFFFFFFF")
        if len(color) == 6:
            color = color + "FF"
        elif len(color) != 8:
            color = "FFFFFFFF"
        color = color.upper()

        # -- Bambu Material Index (slicer_filament = tray_info_idx) --
        material = filament_data.get("material_type", "PLA")
        slicer_filament = (
            filament_data.get("bambu_idx")
            or filament_data.get("bambu_tray_idx")
            or _GENERIC_SLICER_IDS.get(material.upper(), "GFL99")
        )

        # tray_sub_brands: sub-brand/profile-name (z.B. "PLA Basic", "PETG HF")
        tray_sub_brands = filament_data.get("material_subgroup") or material

        # -- Temperaturen --
        nozzle_temp_min = (
            _int_or_none(filament_data.get("bambu_nozzle_temp_min"))
            or _int_or_none(filament_data.get("nozzle_temp_min"))
        )
        nozzle_temp_max = (
            _int_or_none(filament_data.get("bambu_nozzle_temp_max"))
            or _int_or_none(filament_data.get("nozzle_temp_max"))
        )

        # k_value für configure-Endpoint — 0.0 = skip (kein K-Profil setzen)
        k_value = _float_or_none(filament_data.get("bambu_k_value")) or 0.0

        configure_params: dict[str, Any] = {
            "tray_info_idx":        slicer_filament,
            "tray_type":            material,
            "tray_sub_brands":      tray_sub_brands,
            "tray_color":           color,                   # 8-stellig RRGGBBAA
            "nozzle_temp_min":      nozzle_temp_min or 190,  # REQUIRED — Fallback 190°C
            "nozzle_temp_max":      nozzle_temp_max or 230,  # REQUIRED — Fallback 230°C
            "cali_idx":             (
                _int_or_none(filament_data.get("bambu_cali_idx"))
                if filament_data.get("bambu_cali_idx") is not None
                else -1                                       # -1 = default 0.020
            ),
            "nozzle_diameter":      "0.4",
            "setting_id":           filament_data.get("bambu_setting_id") or "",
            "kprofile_filament_id": slicer_filament,
            "k_value":              k_value,                 # 0.0 = skip
        }

        try:
            r = await self._client.post(
                f"{self._bambuddy_url}/api/v1/printers/{self._bambuddy_printer_id}"
                f"/slots/{ams_id}/{tray_id}/configure",
                params=configure_params,
            )
            r.raise_for_status()
            self.log_debug(
                "out",
                f"POST /api/v1/printers/{self._bambuddy_printer_id}/slots/{ams_id}/{tray_id}/configure",
                configure_params,
            )

            # Bambu-Params cachen (für UI-Status-Anzeige)
            self._slot_params_cache[f"{ams_id}-{tray_id}"] = {
                "nozzle_temp_min":            nozzle_temp_min,
                "nozzle_temp_max":            nozzle_temp_max,
                "bambu_setting_id":           filament_data.get("bambu_setting_id", ""),
                "bambu_cali_idx":             filament_data.get("bambu_cali_idx"),
                "bambu_k_value":              filament_data.get("bambu_k_value"),
                "bambu_bed_temp":             filament_data.get("bambu_bed_temp"),
                "bambu_flow_ratio":           filament_data.get("bambu_flow_ratio"),
                "bambu_max_volumetric_speed": filament_data.get("bambu_max_volumetric_speed"),
            }

            logger.info(
                f"Configured Bambuddy printer {self._bambuddy_printer_id} "
                f"slot {ams_id}-{tray_id} "
                f"(material={material}, slicer_filament={slicer_filament})"
            )
        except httpx.HTTPStatusError as e:
            logger.error(
                f"Bambuddy configure error for slot {ams_id}-{tray_id}: "
                f"{e.response.status_code} {e.response.text}"
            )
        except Exception as e:
            logger.error(f"Failed to configure Bambuddy slot {ams_id}-{tray_id}: {e}")

    # -- Initialer Status-Fetch -----------------------------------------------

    async def _fetch_and_emit_status(self) -> None:
        """Initialen Drucker-Status von Bambuddy REST-API laden und als slots_update emittieren."""
        if not self._client or not self._bambuddy_printer_id:
            return
        try:
            r = await self._client.get(
                f"{self._bambuddy_url}/api/v1/printers/{self._bambuddy_printer_id}/status"
            )
            if r.status_code == 200:
                self._connected = True
                self._process_slots(r.json())
                logger.info(
                    f"Initial status fetched for Bambuddy printer {self._bambuddy_printer_id}"
                )
        except Exception as e:
            logger.warning(f"Could not fetch initial Bambuddy status: {e}")

    # -- Slot-Verarbeitung (AMS-Status → FilaMan Slots) ----------------------

    def _process_slots(self, printer_status: dict) -> None:
        """AMS-Daten aus Bambuddy printer_status verarbeiten und slots_update emittieren."""
        ams_list = printer_status.get("ams", [])
        vt_tray_list = printer_status.get("vt_tray", [])
        if not ams_list and not vt_tray_list:
            return

        ams_units: list[dict[str, Any]] = []
        ams_slots: list[dict[str, Any]] = []

        for ams_unit in ams_list:
            ams_id = int(ams_unit.get("id", 0))
            trays = ams_unit.get("tray", ams_unit.get("trays", []))
            ams_units.append({
                "ams_id":     ams_id,
                "humidity":   ams_unit.get("humidity"),
                "temp":       ams_unit.get("temp", ams_unit.get("temperature")),
                "tray_count": len(trays),
                "is_ams_ht":  ams_unit.get("is_ams_ht", False),
            })

            for tray in trays:
                tray_id    = int(tray.get("id", 0))
                slot_index = f"{ams_id}-{tray_id}"
                tray_type  = tray.get("tray_type", "")
                tray_color = tray.get("tray_color", "")
                cached     = self._slot_params_cache.get(slot_index, {})

                preset_id     = tray.get("preset_id", "")
                tray_info_idx = _extract_bambu_idx(preset_id) or tray.get("tray_info_idx", "")

                if not tray_type:
                    self._slot_params_cache.pop(slot_index, None)
                    self._slot_to_filaman_spool.pop(slot_index, None)

                ams_slots.append({
                    "slot_index":    slot_index,
                    "slot_name":     f"AMS {ams_id + 1} - Slot {tray_id + 1}",
                    "tray_info_idx": tray_info_idx,
                    "tray_type":     tray_type,
                    "tray_color":    tray_color,
                    "nozzle_temp_min": (
                        tray.get("nozzle_temp_min")
                        if tray.get("nozzle_temp_min") is not None
                        else cached.get("nozzle_temp_min")
                    ),
                    "nozzle_temp_max": (
                        tray.get("nozzle_temp_max")
                        if tray.get("nozzle_temp_max") is not None
                        else cached.get("nozzle_temp_max")
                    ),
                    "setting_id": tray.get("setting_id") or cached.get("bambu_setting_id", ""),
                    "cali_idx":   (
                        tray.get("cali_idx")
                        if tray.get("cali_idx") is not None
                        else cached.get("bambu_cali_idx")
                    ),
                    "bambu_k_value":              cached.get("bambu_k_value"),
                    "bambu_bed_temp":             cached.get("bambu_bed_temp"),
                    "bambu_flow_ratio":           cached.get("bambu_flow_ratio"),
                    "bambu_max_volumetric_speed": cached.get("bambu_max_volumetric_speed"),
                    "remain":  tray.get("remain", 0),
                    "present": bool(tray_type),
                })

        ext_slots: list[dict[str, Any]] = []
        for vt in vt_tray_list:
            vt_id     = int(vt.get("id", 254))
            vt_type   = vt.get("tray_type", "")
            vt_color  = vt.get("tray_color", "")
            vt_idx    = f"255-{vt_id}"
            vt_cached = self._slot_params_cache.get(vt_idx, {})

            vt_preset_id     = vt.get("preset_id", "")
            vt_tray_info_idx = _extract_bambu_idx(vt_preset_id) or vt.get("tray_info_idx", "")

            if not vt_type:
                self._slot_params_cache.pop(vt_idx, None)
                self._slot_to_filaman_spool.pop(vt_idx, None)

            ext_slots.append({
                "slot_index":    vt_idx,
                "slot_name":     "External Tray",
                "tray_info_idx": vt_tray_info_idx,
                "tray_type":     vt_type,
                "tray_color":    vt_color,
                "nozzle_temp_min": (
                    vt.get("nozzle_temp_min")
                    if vt.get("nozzle_temp_min") is not None
                    else vt_cached.get("nozzle_temp_min")
                ),
                "nozzle_temp_max": (
                    vt.get("nozzle_temp_max")
                    if vt.get("nozzle_temp_max") is not None
                    else vt_cached.get("nozzle_temp_max")
                ),
                "setting_id": vt.get("setting_id") or vt_cached.get("bambu_setting_id", ""),
                "cali_idx":   (
                    vt.get("cali_idx")
                    if vt.get("cali_idx") is not None
                    else vt_cached.get("bambu_cali_idx")
                ),
                "bambu_k_value":              vt_cached.get("bambu_k_value"),
                "bambu_bed_temp":             vt_cached.get("bambu_bed_temp"),
                "bambu_flow_ratio":           vt_cached.get("bambu_flow_ratio"),
                "bambu_max_volumetric_speed": vt_cached.get("bambu_max_volumetric_speed"),
                "remain":  vt.get("remain", 0),
                "present": bool(vt_type),
            })

        self._current_ams_units = ams_units
        has_external = len(ext_slots) > 0
        slots = ams_slots + ext_slots

        if slots == self._current_slots:
            return

        self._current_slots = slots

        total_slots = sum(u.get("tray_count", 0) for u in ams_units)
        if has_external:
            total_slots += len(ext_slots)
        ams_info = {
            "ams_count":     len(ams_units),
            "ams_type":      "AMS",
            "slot_count":    total_slots,
            "external_spool": has_external,
            "ams_units":     ams_units,
        }

        logger.info(
            f"Slot data changed for printer {self.printer_id}, emitting slots_update"
        )
        self.emit({"event_type": "slots_update", "slots": slots, "ams_info": ams_info})

    # -- Health ---------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        total_slots = sum(u.get("tray_count", 0) for u in self._current_ams_units)
        return {
            "driver_key":          self.driver_key,
            "printer_id":          self.printer_id,
            "running":             self._running,
            "connected":           self._connected,
            "bambuddy_printer_id": self._bambuddy_printer_id,
            "ams_count":           len(self._current_ams_units),
            "slot_count":          total_slots,
            "ams_units":           self._current_ams_units,
            "slots":               self._current_slots,
            "last_sync_count":     self._last_sync_count,
            "last_sync_error":     self._last_sync_error,
            "active_slot_mappings": len(self._slot_to_filaman_spool),
            "sync_actions": [
                {
                    "action":   "trigger_sync",
                    "label":    "Sync Now",
                    "label_de": "Jetzt synchronisieren",
                    "variant":  "secondary",
                },
                {
                    "action":   "full_resync",
                    "label":    "Full Resync",
                    "label_de": "Vollständiger Resync",
                    "variant":  "danger",
                },
            ],
        }
