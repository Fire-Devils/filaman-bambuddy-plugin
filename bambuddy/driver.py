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
import time
from datetime import datetime, timezone
from typing import Any, Callable, ClassVar

import httpx
from sqlalchemy import delete, event, func, select
from sqlalchemy.orm import Session, selectinload

from app.core.database import async_session_maker
from app.models.filament import Filament, FilamentColor
from app.models.location import Location
from app.models.printer import Printer
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
    "PLA": "GFL99",
    "PETG": "GFG99",
    "ABS": "GFB99",
    "ASA": "GFB98",
    "TPU": "GFU99",
    "NYLON": "GFN99",
    "PA": "GFN99",
    "PVA": "GFS99",
    "HIPS": "GFS98",
    "PC": "GFC99",
    "PP": "GFP97",
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

    # -- URL-basierte Sync-Koordination (Klassenlevel) -------------------------
    # Verhindert mehrfache Syncs wenn mehrere Drucker dieselbe Bambuddy-Instanz nutzen.
    # Pro eindeutige bambuddy_url läuft maximal ein Sync gleichzeitig.
    _url_sync_locks: ClassVar[dict[str, asyncio.Lock]] = {}
    _url_instances: ClassVar[dict[str, list["Driver"]]] = {}
    _url_last_sync: ClassVar[dict[str, float]] = {}
    _SYNC_COOLDOWN: ClassVar[float] = 5.0  # Sekunden

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
        self._sync_interval: int = int(config.get("sync_interval_seconds", 3600))
        self._reconnect_interval: int = int(
            config.get("reconnect_interval_seconds", 30)
        )
        self._sync_enabled: bool = config.get("sync_enabled", "enabled") == "enabled"
        _debug_val = config.get("debug_enabled", False)
        self._debug_enabled: bool = (
            _debug_val
            if isinstance(_debug_val, bool)
            else str(_debug_val).lower() in ("true", "1", "enabled")
        )

        # -- Background Tasks --
        self._ws_task: asyncio.Task | None = None
        self._sync_task: asyncio.Task | None = None

        # -- Verbindungs-Status --
        self._ws_connected: bool = False  # WebSocket-Verbindung zu Bambuddy-Server
        self._printer_connected: bool = False  # Bambu-Drucker↔Bambuddy Verbindung

        # -- Status-Cache --
        self._current_slots: list[dict[str, Any]] = []
        self._current_ams_units: list[dict[str, Any]] = []
        # Cache für Bambu-Parameter (nozzle temps, k_value etc.) pro Slot
        self._slot_params_cache: dict[str, dict] = {}
        # Slot-Key ("ams_id-tray_id") → FilaMan-Spool-ID (für Verbrauchsmeldungen)
        self._slot_to_filaman_spool: dict[str, int] = {}
        # Slot-Key ("ams_id-tray_id") → Bambu tray_uuid (für Spoolman-Link)
        self._slot_to_tray_uuid: dict[str, str] = {}
        # Letzte Sync-Statistik
        self._last_sync_count: int = 0
        self._last_sync_error: str | None = None

        # -- Spoolman-Cache --
        self._spoolman_enabled: bool = False
        self._spoolman_url: str = ""

        # -- Original-Location-Cache für Spoolman-Verknüpfung --
        self._spool_original_location: dict[int, int | None] = {}

        # -- Drucker-Name für Location-Generierung --
        self._printer_name: str | None = None

        # Sofortiger Push: Guard gegen Endlosschleife + Debounce-Task
        self._syncing: bool = False
        self._debounce_task: asyncio.Task | None = None

        # -- Task Restart Management --
        self._ws_restart_count: int = 0
        self._sync_restart_count: int = 0
        self._last_ws_restart: float = 0.0
        self._last_sync_restart: float = 0.0
        self._max_restart_attempts: int = 5
        self._restart_backoff_base: float = 2.0  # Exponential backoff base

        # -- Event Emission Tracking --
        self._last_status_emit: float = 0.0
        self._status_emit_interval: float = (
            10.0  # Emit status every 10s even if unchanged
        )

    # -- URL-basierte Sync-Koordination ----------------------------------------

    def _register(self) -> None:
        """Registriert diese Instanz für URL-basierte Sync-Koordination."""
        url = self._bambuddy_url
        if url not in self._url_instances:
            self._url_instances[url] = []
        if self not in self._url_instances[url]:
            self._url_instances[url].append(self)
            logger.debug(
                f"Driver {self.printer_id} registered for URL {url} "
                f"({len(self._url_instances[url])} driver(s) total)"
            )

    def _unregister(self) -> None:
        """Entfernt diese Instanz aus der URL-Koordination."""
        url = self._bambuddy_url
        instances = self._url_instances.get(url, [])
        if self in instances:
            instances.remove(self)
        if not instances:
            self._url_instances.pop(url, None)
            self._url_sync_locks.pop(url, None)
            self._url_last_sync.pop(url, None)

    def _get_url_lock(self) -> asyncio.Lock:
        """Liefert den gemeinsamen Sync-Lock für diese Bambuddy-URL."""
        url = self._bambuddy_url
        if url not in self._url_sync_locks:
            self._url_sync_locks[url] = asyncio.Lock()
        return self._url_sync_locks[url]

    def _peer_printer_ids(self) -> list[int]:
        """Alle printer_ids die dieselbe Bambuddy-URL nutzen (inkl. eigene)."""
        return [
            d.printer_id for d in self._url_instances.get(self._bambuddy_url, [self])
        ]

    def _is_sync_coordinator(self) -> bool:
        """True wenn dieser Driver der Sync-Koordinator für seine URL ist.

        Der erste registrierte Driver pro URL übernimmt die Koordination:
        periodischer Sync-Loop und Debounce-Trigger bei DB-Commits.
        """
        instances = self._url_instances.get(self._bambuddy_url, [])
        return bool(instances) and instances[0] is self

    # -- Lifecycle ------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._client = httpx.AsyncClient(headers=self._headers, timeout=15.0)

        # -- Drucker-Name für Location-Generierung cachen --
        try:
            async with async_session_maker() as db:
                printer = await db.get(Printer, self.printer_id)
                self._printer_name = (
                    printer.name if printer else f"Printer {self.printer_id}"
                )
        except Exception as e:
            logger.warning(f"Failed to load printer name: {e}")
            self._printer_name = f"Printer {self.printer_id}"

        # -- Spoolman-Settings cachen --
        try:
            resp = await self._client.get(
                f"{self._bambuddy_url}/api/v1/settings/spoolman"
            )
            if resp.status_code == 200:
                data = resp.json()
                spoolman_val = data.get("spoolman_enabled", False)
                if isinstance(spoolman_val, bool):
                    self._spoolman_enabled = spoolman_val
                else:
                    self._spoolman_enabled = str(spoolman_val).lower() == "true"
                self._spoolman_url = data.get("spoolman_url", "") or ""
                logger.debug(
                    f"Spoolman status cached: enabled={self._spoolman_enabled}, "
                    f"url={self._spoolman_url}"
                )
            else:
                logger.warning(
                    f"Spoolman settings returned {resp.status_code}, assuming disabled"
                )
        except Exception as e:
            logger.warning(f"Failed to fetch Spoolman settings: {e}")

        # -- Cleanup: Alte bambuddy_spool_id Einträge entfernen wenn Sync deaktiviert --
        if not self._sync_enabled:
            try:
                async with async_session_maker() as db:
                    result = await db.execute(
                        select(SpoolPrinterParam).where(
                            SpoolPrinterParam.printer_id == self.printer_id,
                            SpoolPrinterParam.param_key == "bambuddy_spool_id",
                        )
                    )
                    old_params = result.scalars().all()
                    for param in old_params:
                        await db.delete(param)
                    await db.commit()
                    if old_params:
                        logger.info(
                            f"Cleaned up {len(old_params)} old bambuddy_spool_id entries "
                            f"(inventory sync is disabled)"
                        )
            except Exception as e:
                logger.warning(f"Failed to cleanup old bambuddy_spool_id entries: {e}")

        if self._sync_enabled:
            # Instanz registrieren (VOR erstem Sync, damit Peer-Erkennung funktioniert)
            self._register()

        # Initialen AMS-Status laden
        await self._fetch_and_emit_status()

        if self._sync_enabled:
            # Inventory-Sync: FilaMan → Bambuddy (URL-Lock verhindert Duplikate)
            await self._sync_all_spools()

        # Background-Tasks starten
        self._ws_task = asyncio.create_task(self._ws_loop())
        self._ws_task.add_done_callback(self._on_task_done)

        if self._sync_enabled:
            # Periodischer Sync nur vom Koordinator (erster Driver pro URL)
            if self._is_sync_coordinator():
                self._sync_task = asyncio.create_task(self._sync_inventory_loop())
                self._sync_task.add_done_callback(self._on_task_done)
                logger.debug(
                    f"Printer {self.printer_id} is sync coordinator for {self._bambuddy_url}"
                )

            # DB-Event-Listener für sofortigen Push registrieren
            event.listen(Session, "after_commit", self._on_session_commit)

        logger.info(
            f"Bambuddy driver started for FilaMan printer {self.printer_id} "
            f"(Bambuddy printer_id={self._bambuddy_printer_id})"
        )

    async def stop(self) -> None:
        was_coordinator = self._is_sync_coordinator()

        if self._sync_enabled:
            # DB-Event-Listener entfernen
            try:
                event.remove(Session, "after_commit", self._on_session_commit)
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
        self._ws_connected = False
        self._printer_connected = False
        if self._client:
            await self._client.aclose()
            self._client = None

        if self._sync_enabled:
            # Instanz abmelden und ggf. Koordinator-Rolle delegieren
            self._unregister()
            if was_coordinator:
                peers = self._url_instances.get(self._bambuddy_url, [])
                if peers:
                    new_coord = peers[0]
                    if new_coord._running and (
                        not new_coord._sync_task or new_coord._sync_task.done()
                    ):
                        new_coord._sync_task = asyncio.create_task(
                            new_coord._sync_inventory_loop()
                        )
                        new_coord._sync_task.add_done_callback(new_coord._on_task_done)
                        logger.info(
                            f"Sync coordinator delegated to printer {new_coord.printer_id} "
                            f"for {self._bambuddy_url}"
                        )

        logger.info(f"Bambuddy driver stopped for printer {self.printer_id}")

    # -- Sofortiger Push (SQLAlchemy Event-Listener) -------------------------

    def _on_session_commit(self, session: Session) -> None:
        """Reagiert auf jeden DB-Commit im Prozess (synchron, im Event-Loop-Thread).

        Nur der Sync-Koordinator für diese URL triggert den Debounce-Sync,
        damit nicht mehrere Drucker an derselben Bambuddy-Instanz gleichzeitig syncen.
        Ignoriert eigene Commits während eines laufenden Syncs (_syncing-Guard).
        """
        if (
            not self._running
            or not self._ws_connected
            or self._syncing
            or not self._sync_enabled
        ):
            return
        if not self._is_sync_coordinator():
            return
        if self._debounce_task and not self._debounce_task.done():
            self._debounce_task.cancel()
        self._debounce_task = asyncio.create_task(self._debounced_sync())
        self._debounce_task.add_done_callback(self._on_task_done)

    async def _debounced_sync(self) -> None:
        """Wartet 3 Sekunden auf weitere Commits, dann Inventory-Sync."""
        await asyncio.sleep(3)
        if (
            self._running
            and self._ws_connected
            and not self._syncing
            and self._sync_enabled
        ):
            logger.debug(
                f"Data change detected, triggering inventory sync for printer {self.printer_id}"
            )
            await self._sync_all_spools()

    def _on_task_done(self, task: asyncio.Task) -> None:
        """Callback to catch unhandled exceptions in background tasks.

        Automatically restarts critical tasks (WebSocket, Sync) with exponential backoff.
        """
        try:
            task.result()
        except asyncio.CancelledError:
            pass  # Expected on shutdown
        except Exception as e:
            logger.error(f"Background task failed: {e}", exc_info=True)

            # Nur restarten wenn Driver noch läuft
            if not self._running:
                return

            # Identifiziere welcher Task gefailed ist
            task_name = "unknown"
            restart_count = 0
            last_restart = 0.0

            if task is self._ws_task:
                task_name = "WebSocket"
                self._ws_restart_count += 1
                restart_count = self._ws_restart_count
                self._last_ws_restart = time.monotonic()
                last_restart = self._last_ws_restart
            elif task is self._sync_task:
                task_name = "Sync"
                self._sync_restart_count += 1
                restart_count = self._sync_restart_count
                self._last_sync_restart = time.monotonic()
                last_restart = self._last_sync_restart
            else:
                # Andere Tasks (debounce, assignment etc.) nicht automatisch restarten
                logger.warning(f"Untracked background task failed, not restarting")
                return

            # Max restart attempts check
            if restart_count > self._max_restart_attempts:
                logger.error(
                    f"{task_name} task failed {restart_count} times, "
                    f"exceeded max restart attempts ({self._max_restart_attempts}). "
                    f"Manual intervention required."
                )
                return

            # Exponential backoff berechnen
            backoff_delay = min(
                self._restart_backoff_base ** (restart_count - 1),
                300,  # Max 5 Minuten
            )

            logger.warning(
                f"{task_name} task crashed (attempt {restart_count}/{self._max_restart_attempts}). "
                f"Restarting in {backoff_delay:.1f}s..."
            )

            # Task mit Delay neu starten
            asyncio.create_task(self._restart_task_delayed(task_name, backoff_delay))

    async def _restart_task_delayed(self, task_name: str, delay: float) -> None:
        """Startet einen gecrasht Task nach Delay neu."""
        await asyncio.sleep(delay)

        if not self._running:
            logger.debug(f"Driver stopped, skipping {task_name} restart")
            return

        try:
            if task_name == "WebSocket":
                logger.info(
                    f"Restarting WebSocket task (attempt {self._ws_restart_count})"
                )
                self._ws_task = asyncio.create_task(self._ws_loop())
                self._ws_task.add_done_callback(self._on_task_done)
            elif task_name == "Sync":
                logger.info(
                    f"Restarting Sync task (attempt {self._sync_restart_count})"
                )
                self._sync_task = asyncio.create_task(self._sync_inventory_loop())
                self._sync_task.add_done_callback(self._on_task_done)
        except Exception as e:
            logger.error(f"Failed to restart {task_name} task: {e}", exc_info=True)

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
                    selectinload(Spool.filament)
                    .selectinload(Filament.filament_colors)
                    .selectinload(FilamentColor.color),
                    selectinload(Spool.printer_params),
                )
            )
            return list(result.scalars().all())

    async def _store_bambuddy_id_db(
        self, filaman_spool_id: int, bambuddy_spool_id: int
    ) -> None:
        """Speichert Bambuddy-Spool-ID als SpoolPrinterParam für ALLE Drucker an dieser URL.

        Da das Bambuddy-Inventar pro Instanz (URL) global ist, wird die Spool-ID
        für jeden Drucker gespeichert, der dieselbe Bambuddy-URL nutzt.
        Danach liefert enrich_filament_data() automatisch bambuddy_spool_id
        in filament_data["printer_params"] für jeden dieser Drucker.
        """
        # Keine IDs speichern wenn Inventory-Sync deaktiviert ist
        if not self._sync_enabled:
            return

        printer_ids = self._peer_printer_ids()
        async with async_session_maker() as db:
            for pid in printer_ids:
                result = await db.execute(
                    select(SpoolPrinterParam).where(
                        SpoolPrinterParam.spool_id == filaman_spool_id,
                        SpoolPrinterParam.printer_id == pid,
                        SpoolPrinterParam.param_key == "bambuddy_spool_id",
                    )
                )
                existing = result.scalar_one_or_none()
                if existing:
                    existing.param_value = str(bambuddy_spool_id)
                else:
                    db.add(
                        SpoolPrinterParam(
                            spool_id=filaman_spool_id,
                            printer_id=pid,
                            param_key="bambuddy_spool_id",
                            param_value=str(bambuddy_spool_id),
                        )
                    )
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
        manufacturer_name = fil.manufacturer.name if fil and fil.manufacturer else None
        colors = sorted(fil.filament_colors, key=lambda fc: fc.position) if fil else []

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
        weight_used = (
            max(0.0, initial_weight - float(remaining))
            if remaining is not None
            else 0.0
        )

        # printer_params als {key: value} dict aus der Relationship
        pp: dict[str, str | None] = {
            p.param_key: p.param_value
            for p in (spool.printer_params or [])
            if p.printer_id == self.printer_id
        }

        payload: dict[str, Any] = {
            "material": (fil.material_type if fil else "PLA") or "PLA",
            "brand": manufacturer_name,
            "rgba": rgba,
            "label_weight": int(initial_weight),
            "weight_used": round(weight_used, 2),
            "weight_locked": False,
            "note": f"filaman:{spool.id}",
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

        Nutzt URL-basiertes Lock: pro Bambuddy-URL läuft maximal ein Sync,
        auch wenn mehrere Drucker dieselbe Instanz nutzen.
        Cooldown verhindert redundante Syncs durch DB-Commit-Kaskaden.
        """
        if not self._client:
            return
        url_lock = self._get_url_lock()
        if url_lock.locked():
            logger.debug(
                f"Sync skipped: already in progress for URL {self._bambuddy_url}"
            )
            return
        # Cooldown: Sync überspringen wenn kürzlich abgeschlossen (verhindert Kaskaden
        # durch DB-Commits die _on_session_commit in Peer-Drivern auslösen)
        last = self._url_last_sync.get(self._bambuddy_url, 0.0)
        if (time.monotonic() - last) < self._SYNC_COOLDOWN:
            logger.debug(
                f"Sync skipped: recently completed for URL {self._bambuddy_url}"
            )
            return
        async with url_lock:
            await self._do_sync_inner()
            self._url_last_sync[self._bambuddy_url] = time.monotonic()

    async def _do_sync_inner(self) -> None:
        """Innere Sync-Logik ohne Lock — muss unter URL-Lock aufgerufen werden.

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
                and s["note"].removeprefix("filaman:").isdigit()
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
                try:
                    await self._sync_all_spools()
                    # Reset restart counter bei erfolgreichem Durchlauf
                    if self._sync_restart_count > 0:
                        logger.info(
                            f"Sync task stable after {self._sync_restart_count} restarts, "
                            "resetting restart counter"
                        )
                        self._sync_restart_count = 0
                except Exception as e:
                    logger.error(f"Inventory sync failed: {e}")

    async def trigger_sync(self) -> None:
        """Manueller sofortiger Inventory-Sync (Drucker-Action)."""
        if not self._sync_enabled:
            logger.info("Inventory sync is disabled — skipping trigger_sync")
            return
        await self._sync_all_spools()

    async def full_resync(self) -> None:
        """Löscht ALLE Bambuddy-Inventarspulen und synchronisiert neu aus FilaMan.

        Nutzt URL-basiertes Lock für die gesamte Dauer (Löschen + Neu-Sync), damit kein
        paralleler Debounce-Sync oder periodischer Sync Duplikate erzeugen kann.
        Löscht bambuddy_spool_id-Params für ALLE Drucker an dieser URL.
        """
        if not self._sync_enabled:
            logger.info("Inventory sync is disabled — skipping full_resync")
            return
        if not self._client:
            raise RuntimeError("Driver not connected")
        url_lock = self._get_url_lock()
        async with url_lock:
            logger.info(
                f"Full resync started for URL {self._bambuddy_url} "
                f"(triggered by printer {self.printer_id})"
            )
            # 1. ALLE Bambuddy-Inventarspulen löschen (keine Filterung nach filaman:)
            bb_spools = await self._bb_get("/api/v1/inventory/spools")
            for spool in bb_spools:
                await self._bb_delete(f"/api/v1/inventory/spools/{spool['id']}")
            logger.info(f"Deleted {len(bb_spools)} Bambuddy spools")
            # 2. bambuddy_spool_id-Params für ALLE Drucker an dieser URL löschen
            peer_ids = self._peer_printer_ids()
            async with async_session_maker() as db:
                await db.execute(
                    delete(SpoolPrinterParam).where(
                        SpoolPrinterParam.printer_id.in_(peer_ids),
                        SpoolPrinterParam.param_key == "bambuddy_spool_id",
                    )
                )
                await db.commit()
            # 3. Neu synchronisieren (Lock bereits gehalten, direkt _do_sync_inner aufrufen)
            await self._do_sync_inner()
            self._url_last_sync[self._bambuddy_url] = time.monotonic()
            logger.info(f"Full resync complete for URL {self._bambuddy_url}")

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
                    self._ws_connected = True
                    logger.info(f"WebSocket connected: {uri}")

                    # Reset restart counter bei erfolgreicher Verbindung
                    if self._ws_restart_count > 0:
                        logger.info(
                            f"WebSocket stable after {self._ws_restart_count} restarts, "
                            "resetting restart counter"
                        )
                        self._ws_restart_count = 0

                    async for message in ws:
                        if not self._running:
                            break
                        try:
                            event = json.loads(message)
                            logger.info(
                                f"WS message received (type={json.loads(message).get('type')})"
                            )
                            logger.debug(f"WS message full payload: {event}")
                            await self._handle_ws_event(event)
                        except Exception as e:
                            logger.warning(f"WS message handling error: {e}")
            except Exception as e:
                self._ws_connected = False
                if self._running:
                    logger.warning(
                        f"WebSocket disconnected ({e}). "
                        f"Reconnecting in {self._reconnect_interval}s…"
                    )
                    await asyncio.sleep(self._reconnect_interval)

    async def _handle_ws_event(self, event: dict) -> None:
        """Verarbeitet eingehende Bambuddy WebSocket-Events."""
        self.log_debug("in", "websocket", event)
        event_type = event.get("type")

        if event_type == "printer_status":
            data = event.get("data", {})
            if event.get("printer_id") == self._bambuddy_printer_id:
                old_connected = self._printer_connected
                self._printer_connected = data.get("connected", self._printer_connected)

                # Process slots (may emit slots_update if changed)
                self._process_slots(data)

                # Emit heartbeat status if:
                # 1. Connection state changed, OR
                # 2. Enough time passed since last emit (heartbeat)
                now = time.monotonic()
                connection_changed = old_connected != self._printer_connected
                heartbeat_due = (
                    now - self._last_status_emit
                ) >= self._status_emit_interval

                if connection_changed or heartbeat_due:
                    self._last_status_emit = now
                    self.emit(
                        {
                            "event_type": "printer_status",
                            "connected": self._printer_connected,
                            "timestamp": now,
                        }
                    )
                    logger.debug(
                        f"Emitted printer_status: connected={self._printer_connected} "
                        f"(reason: {'connection_change' if connection_changed else 'heartbeat'})"
                    )

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

    def send_filament_to_tray(
        self, ams_id: int, tray_id: int, filament_data: dict
    ) -> None:
        """Weist FilaMan-Spule einem Bambuddy-AMS-Slot zu."""
        _t = asyncio.create_task(
            self._assign_or_configure(ams_id, tray_id, filament_data)
        )
        _t.add_done_callback(self._on_task_done)

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
        filaman_spool_id = _int_or_none(filament_data.get("id"))
        slot_key = f"{ams_id}-{tray_id}"

        # -- Alte Spule aus Standort entfernen wenn Slot überschrieben wird --
        old_filaman_spool_id = self._slot_to_filaman_spool.get(slot_key)
        if old_filaman_spool_id and old_filaman_spool_id != filaman_spool_id:
            # Alte Spule aus diesem Slot entfernen
            _t = asyncio.create_task(self._restore_spool_location(old_filaman_spool_id))
            _t.add_done_callback(self._on_task_done)
            logger.info(
                f"Removed old spool {old_filaman_spool_id} from slot {slot_key} "
                f"(replaced by {filaman_spool_id})"
            )

        # -- Spoolman-Link: Vor Assignment alte Spoolman-Verknüpfung prüfen/entfernen --
        if filaman_spool_id:
            try:
                async with async_session_maker() as db:
                    spool = await db.get(Spool, filaman_spool_id)
                    self._spool_original_location[filaman_spool_id] = (
                        spool.location_id if spool else None
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to cache original location for FilaMan spool "
                    f"{filaman_spool_id}: {e}"
                )
            # Spoolman-Linking asynchron im Hintergrund ausführen
            # (nicht blockieren - Assignment soll sofort durchlaufen)
            _t = asyncio.create_task(
                self._handle_spoolman_linking(ams_id, tray_id, filaman_spool_id)
            )
            _t.add_done_callback(self._on_task_done)

        # -- Delayed Refetch Helper --
        async def _delayed_refetch():
            await asyncio.sleep(3)
            try:
                await self._fetch_and_emit_status()
            except Exception as e:
                logger.warning(f"Delayed refetch after assignment failed: {e}")

        # Inventory-Assignment nur wenn Sync aktiviert ist
        if bambuddy_spool_id and self._client and self._sync_enabled:
            try:
                response = await self._bb_post(
                    "/api/v1/inventory/assignments",
                    {
                        "spool_id": bambuddy_spool_id,
                        "printer_id": self._bambuddy_printer_id,
                        "ams_id": ams_id,
                        "tray_id": tray_id,
                    },
                )
                self.log_debug(
                    "out",
                    f"POST /api/v1/inventory/assignments",
                    {
                        "spool_id": bambuddy_spool_id,
                        "printer_id": self._bambuddy_printer_id,
                        "ams_id": ams_id,
                        "tray_id": tray_id,
                        "configured": response.get("configured"),
                    },
                )
                logger.info(
                    f"Assigned Bambuddy spool {bambuddy_spool_id} to "
                    f"printer {self._bambuddy_printer_id} AMS {ams_id}/{tray_id} "
                    f"(auto-configured={response.get('configured', False)})"
                )

                # Location nach erfolgreichem Assignment setzen
                if filaman_spool_id:
                    await self._update_spool_location(filaman_spool_id, ams_id, tray_id)

                # Slot-Spool-Cache für Verbrauchsmeldung nach Druckende aktualisieren
                if filaman_spool_id:
                    self._slot_to_filaman_spool[slot_key] = filaman_spool_id

                _t = asyncio.create_task(_delayed_refetch())
                _t.add_done_callback(self._on_task_done)
                return
            except Exception as e:
                logger.warning(
                    f"Assignment API failed (slot {ams_id}/{tray_id}), "
                    f"falling back to configure-call: {e}"
                )

        # Fallback: Direkter configure-Call mit allen Feldern
        await self._send_assignment(ams_id, tray_id, filament_data)

        # Location nach erfolgreichem Fallback-Assignment setzen
        if filaman_spool_id:
            await self._update_spool_location(filaman_spool_id, ams_id, tray_id)

        # Slot-Spool-Cache auch beim Fallback aktualisieren
        if filaman_spool_id:
            self._slot_to_filaman_spool[slot_key] = filaman_spool_id

        # -- Delayed Refetch nach Fallback --
        _t = asyncio.create_task(_delayed_refetch())
        _t.add_done_callback(self._on_task_done)

    # -- Direkter configure-Call (Fallback) ----------------------------------

    async def _send_assignment(
        self, ams_id: int, tray_id: int, filament_data: dict
    ) -> None:
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
        nozzle_temp_min = _int_or_none(
            filament_data.get("bambu_nozzle_temp_min")
        ) or _int_or_none(filament_data.get("nozzle_temp_min"))
        nozzle_temp_max = _int_or_none(
            filament_data.get("bambu_nozzle_temp_max")
        ) or _int_or_none(filament_data.get("nozzle_temp_max"))

        # k_value für configure-Endpoint — 0.0 = skip (kein K-Profil setzen)
        k_value = _float_or_none(filament_data.get("bambu_k_value")) or 0.0

        configure_params: dict[str, Any] = {
            "tray_info_idx": slicer_filament,
            "tray_type": material,
            "tray_sub_brands": tray_sub_brands,
            "tray_color": color,  # 8-stellig RRGGBBAA
            "nozzle_temp_min": nozzle_temp_min or 190,  # REQUIRED — Fallback 190°C
            "nozzle_temp_max": nozzle_temp_max or 230,  # REQUIRED — Fallback 230°C
            "cali_idx": (
                _int_or_none(filament_data.get("bambu_cali_idx"))
                if filament_data.get("bambu_cali_idx") is not None
                else -1  # -1 = default 0.020
            ),
            "nozzle_diameter": "0.4",
            "setting_id": filament_data.get("bambu_setting_id") or "",
            "kprofile_filament_id": slicer_filament,
            "k_value": k_value,  # 0.0 = skip
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
                "nozzle_temp_min": nozzle_temp_min,
                "nozzle_temp_max": nozzle_temp_max,
                "bambu_setting_id": filament_data.get("bambu_setting_id", ""),
                "bambu_cali_idx": filament_data.get("bambu_cali_idx"),
                "bambu_k_value": filament_data.get("bambu_k_value"),
                "bambu_bed_temp": filament_data.get("bambu_bed_temp"),
                "bambu_flow_ratio": filament_data.get("bambu_flow_ratio"),
                "bambu_max_volumetric_speed": filament_data.get(
                    "bambu_max_volumetric_speed"
                ),
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
            logger.error(f"Failed to configure Bambuddy slot {ams_id}/{tray_id}: {e}")

    async def _handle_spoolman_linking(
        self, ams_id: int, tray_id: int, filaman_spool_id: int
    ) -> None:
        if not self._spoolman_enabled:
            return

        if not self._client:
            logger.warning("Spoolman linking skipped: HTTP client not initialized")
            return

        try:
            # Bei Spoolman-Integration ist die FilaMan-Spool-ID identisch mit der
            # Spoolman-Spool-ID (via SpoolmanAPI-Plugin). Diese wird direkt für
            # den Link-API-Call verwendet, NICHT die Bambuddy-Inventory-Spool-ID.
            spoolman_spool_id = filaman_spool_id

            assignments = await self._bb_get(
                "/api/v1/inventory/assignments",
                params={"printer_id": self._bambuddy_printer_id},
            )
            if not isinstance(assignments, list):
                assignments = []

            old_spool_id: int | None = None
            for assignment in assignments:
                if (
                    int(assignment.get("ams_id", -1)) == ams_id
                    and int(assignment.get("tray_id", -1)) == tray_id
                ):
                    old_spool_id = _int_or_none(assignment.get("spool_id"))
                    break

            if old_spool_id and old_spool_id != spoolman_spool_id:
                try:
                    unlink_resp = await self._client.post(
                        f"{self._bambuddy_url}/api/v1/spoolman/spools/{old_spool_id}/unlink"
                    )
                    self.log_debug(
                        "out",
                        f"POST /api/v1/spoolman/spools/{old_spool_id}/unlink",
                        {"status": unlink_resp.status_code},
                    )
                    logger.info(
                        f"Unlinked old Spoolman spool {old_spool_id} from tray "
                        f"{ams_id}/{tray_id} (replaced by {spoolman_spool_id})"
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to unlink Spoolman spool {old_spool_id} "
                        f"from tray {ams_id}/{tray_id}: {e}"
                    )

            # tray_uuid from cache holen
            slot_key = f"{ams_id}-{tray_id}"
            tray_uuid = self._slot_to_tray_uuid.get(slot_key)

            # Wenn nicht im Cache: Status explizit abrufen
            if not tray_uuid:
                logger.info(
                    f"tray_uuid not in cache for slot {slot_key}, "
                    f"fetching printer status..."
                )
                await self._fetch_and_emit_status()
                await asyncio.sleep(
                    0
                )  # Yield to event loop to let _process_slots populate cache
                # Nochmal versuchen
                tray_uuid = self._slot_to_tray_uuid.get(slot_key)

            if not tray_uuid:
                logger.warning(
                    f"Cannot link Spoolman spool {spoolman_spool_id}: "
                    f"tray_uuid not available for slot {slot_key} "
                    f"(AMS tray may not be initialized yet)"
                )
                return

            try:
                link_resp = await self._client.post(
                    f"{self._bambuddy_url}/api/v1/spoolman/spools/{spoolman_spool_id}/link",
                    json={"tray_uuid": tray_uuid},
                )
                self.log_debug(
                    "out",
                    f"POST /api/v1/spoolman/spools/{spoolman_spool_id}/link",
                    {
                        "status": link_resp.status_code,
                        "tray_uuid": tray_uuid,
                    },
                )
                logger.info(
                    f"Linked Spoolman spool {spoolman_spool_id} to tray {tray_uuid}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to link Spoolman spool {spoolman_spool_id} "
                    f"to tray {tray_uuid}: {e}"
                )

        except Exception as e:
            logger.warning(f"Spoolman linking failed for tray {ams_id}/{tray_id}: {e}")

    async def _restore_spool_location(self, filaman_spool_id: int) -> None:
        """Restores a spool's original FilaMan location when its AMS tray becomes empty."""
        if filaman_spool_id not in self._spool_original_location:
            return

        # Check if the spool is assigned to another tray (use list() to avoid dict modification during iteration)
        if filaman_spool_id in list(self._slot_to_filaman_spool.values()):
            return

        original_location_id = self._spool_original_location.pop(filaman_spool_id, None)
        if original_location_id is None:
            return

        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, filaman_spool_id)
                if spool and spool.location_id != original_location_id:
                    await SpoolService(db).move_location(
                        spool,
                        original_location_id,
                        datetime.now(timezone.utc),
                        source="driver",
                        note="Restored from AMS tray",
                    )
        except Exception as e:
            logger.warning(f"Failed to restore spool {filaman_spool_id} location: {e}")

    def _generate_slot_location_name(self, ams_id: int, tray_id: int) -> str:
        """Generiert Location-Namen für AMS-Slot.

        Format:
        - AMS Slots: "{Drucker Name} - AMS A{ams_id+1}"
        - External Slots: "{Drucker Name} - ext. Slot {tray_id+1}"

        Beispiele:
        - "Bambu P1S - AMS A2" (AMS 0, Slot 1)
        - "Bambu X1C - ext. Slot 1" (External, Slot 0)
        """
        printer_name = self._printer_name or f"Printer {self.printer_id}"

        if ams_id >= 200:  # External slot
            return f"{printer_name} - ext. Slot {tray_id + 1}"
        else:
            # AMS slots: A1, A2, A3, A4 (tray_id 0-3)
            slot_label = chr(65 + tray_id)  # 65 = 'A' in ASCII
            return f"{printer_name} - AMS {slot_label}{ams_id + 1}"

    async def _update_spool_location(
        self, filaman_spool_id: int, ams_id: int, tray_id: int
    ) -> None:
        """Setzt Spulen-Standort auf AMS-Slot-Location.

        Erstellt die Location automatisch falls sie noch nicht existiert.
        Nutzt SpoolService.move_location() für konsistente Event-Generierung.
        """
        try:
            slot_location_name = self._generate_slot_location_name(ams_id, tray_id)

            async with async_session_maker() as db:
                # 1. Location suchen (case-insensitive)
                result = await db.execute(
                    select(Location).where(
                        func.lower(Location.name) == slot_location_name.lower()
                    )
                )
                location = result.scalar_one_or_none()

                # 2. Location erstellen falls nicht vorhanden
                if not location:
                    location = Location(
                        name=slot_location_name,
                        identifier=f"bambuddy_{self.printer_id}_{ams_id}_{tray_id}",
                        custom_fields={
                            "managed_by": "bambuddy_plugin",
                            "printer_id": self.printer_id,
                        },
                    )
                    db.add(location)
                    await db.flush()  # Für location.id
                    logger.info(f"Created location: {slot_location_name}")

                # 3. Spule zur Location bewegen (wenn nicht bereits dort)
                spool = await db.get(Spool, filaman_spool_id)
                if not spool:
                    logger.warning(
                        f"Spool {filaman_spool_id} not found, cannot update location"
                    )
                    return

                if spool.location_id == location.id:
                    logger.debug(
                        f"Spool {filaman_spool_id} already at location '{slot_location_name}'"
                    )
                    return

                # SpoolService für konsistente Event-Generierung nutzen
                await SpoolService(db).move_location(
                    spool,
                    location.id,
                    datetime.now(timezone.utc),
                    source="driver",
                    note=f"Assigned to {slot_location_name}",
                )

                # Einmaliger commit für beide Operationen (Location + Move)
                await db.commit()

                logger.info(
                    f"Moved spool {filaman_spool_id} to location '{slot_location_name}' "
                    f"(location_id={location.id})"
                )

        except Exception as e:
            logger.error(
                f"Failed to update location for spool {filaman_spool_id} "
                f"(slot {ams_id}-{tray_id}): {e}",
                exc_info=True,
            )

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
                status_data = r.json()
                self._printer_connected = status_data.get("connected", False)
                self._process_slots(status_data)
                logger.info(
                    f"Initial status fetched for Bambuddy printer {self._bambuddy_printer_id} "
                    f"(printer connected={self._printer_connected})"
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
            ams_units.append(
                {
                    "ams_id": ams_id,
                    "humidity": ams_unit.get("humidity"),
                    "temp": ams_unit.get("temp", ams_unit.get("temperature")),
                    "tray_count": len(trays),
                    "is_ams_ht": ams_unit.get("is_ams_ht", False),
                }
            )

            for tray in trays:
                tray_id = int(tray.get("id", 0))
                slot_index = f"{ams_id}-{tray_id}"
                # tray_uuid für Spoolman-Link cachen
                tray_uuid = tray.get("tray_uuid")
                if tray_uuid:
                    self._slot_to_tray_uuid[slot_index] = tray_uuid
                tray_type = tray.get("tray_type", "")
                tray_color = tray.get("tray_color", "")
                cached = self._slot_params_cache.get(slot_index, {})

                preset_id = tray.get("preset_id", "")
                tray_info_idx = _extract_bambu_idx(preset_id) or tray.get(
                    "tray_info_idx", ""
                )

                if not tray_type:
                    self._slot_params_cache.pop(slot_index, None)
                    old_spool_id = self._slot_to_filaman_spool.get(slot_index)
                    if old_spool_id:
                        _t = asyncio.create_task(
                            self._restore_spool_location(old_spool_id)
                        )
                        _t.add_done_callback(self._on_task_done)
                    self._slot_to_filaman_spool.pop(slot_index, None)

                ams_slots.append(
                    {
                        "slot_index": slot_index,
                        "slot_name": f"AMS {ams_id + 1} - Slot {tray_id + 1}",
                        "tray_info_idx": tray_info_idx,
                        "tray_type": tray_type,
                        "tray_color": tray_color,
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
                        "setting_id": tray.get("setting_id")
                        or cached.get("bambu_setting_id", ""),
                        "cali_idx": (
                            tray.get("cali_idx")
                            if tray.get("cali_idx") is not None
                            else cached.get("bambu_cali_idx")
                        ),
                        "bambu_k_value": cached.get("bambu_k_value"),
                        "bambu_bed_temp": cached.get("bambu_bed_temp"),
                        "bambu_flow_ratio": cached.get("bambu_flow_ratio"),
                        "bambu_max_volumetric_speed": cached.get(
                            "bambu_max_volumetric_speed"
                        ),
                        "remain": tray.get("remain", 0),
                        "present": bool(tray_type),
                    }
                )

        ext_slots: list[dict[str, Any]] = []
        for vt in vt_tray_list:
            vt_id = int(vt.get("id", 254))
            vt_type = vt.get("tray_type", "")
            vt_color = vt.get("tray_color", "")
            vt_idx = f"255-{vt_id}"
            vt_cached = self._slot_params_cache.get(vt_idx, {})

            vt_preset_id = vt.get("preset_id", "")
            vt_tray_info_idx = _extract_bambu_idx(vt_preset_id) or vt.get(
                "tray_info_idx", ""
            )

            if not vt_type:
                self._slot_params_cache.pop(vt_idx, None)
                old_spool_id = self._slot_to_filaman_spool.get(vt_idx)
                if old_spool_id:
                    _t = asyncio.create_task(self._restore_spool_location(old_spool_id))
                    _t.add_done_callback(self._on_task_done)
                self._slot_to_filaman_spool.pop(vt_idx, None)

            ext_slots.append(
                {
                    "slot_index": vt_idx,
                    "slot_name": "External Tray",
                    "tray_info_idx": vt_tray_info_idx,
                    "tray_type": vt_type,
                    "tray_color": vt_color,
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
                    "setting_id": vt.get("setting_id")
                    or vt_cached.get("bambu_setting_id", ""),
                    "cali_idx": (
                        vt.get("cali_idx")
                        if vt.get("cali_idx") is not None
                        else vt_cached.get("bambu_cali_idx")
                    ),
                    "bambu_k_value": vt_cached.get("bambu_k_value"),
                    "bambu_bed_temp": vt_cached.get("bambu_bed_temp"),
                    "bambu_flow_ratio": vt_cached.get("bambu_flow_ratio"),
                    "bambu_max_volumetric_speed": vt_cached.get(
                        "bambu_max_volumetric_speed"
                    ),
                    "remain": vt.get("remain", 0),
                    "present": bool(vt_type),
                }
            )

        self._current_ams_units = ams_units
        has_external = len(ext_slots) > 0
        slots = ams_slots + ext_slots

        # Prüfe ob Slots sich geändert haben (skip emit wenn unverändert)
        # ABER: Beim ersten Start IMMER emittieren (self._current_slots ist [])
        slots_changed = slots != self._current_slots
        is_first_status = len(self._current_slots) == 0 and len(slots) > 0

        if not slots_changed and not is_first_status:
            return

        self._current_slots = slots

        total_slots = sum(u.get("tray_count", 0) for u in ams_units)
        if has_external:
            total_slots += len(ext_slots)
        ams_info = {
            "ams_count": len(ams_units),
            "ams_type": "AMS",
            "slot_count": total_slots,
            "external_spool": has_external,
            "ams_units": ams_units,
        }

        logger.info(
            f"Slot data changed for printer {self.printer_id}, emitting slots_update"
        )
        self.emit({"event_type": "slots_update", "slots": slots, "ams_info": ams_info})

    # -- Health ---------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        total_slots = sum(u.get("tray_count", 0) for u in self._current_ams_units)

        # Task-Liveness prüfen
        ws_task_alive = self._ws_task is not None and not self._ws_task.done()
        sync_task_alive = (
            (self._sync_task is not None and not self._sync_task.done())
            if self._sync_enabled and self._is_sync_coordinator()
            else None
        )

        # Task-Status Details
        task_status = {
            "ws_task_alive": ws_task_alive,
            "ws_restart_count": self._ws_restart_count,
            "sync_task_alive": sync_task_alive,
            "sync_restart_count": self._sync_restart_count,
        }

        # Overall health: critical tasks müssen leben
        tasks_healthy = ws_task_alive
        if sync_task_alive is not None:  # Nur wenn dieser Driver Sync-Coordinator ist
            tasks_healthy = tasks_healthy and sync_task_alive

        return {
            "driver_key": self.driver_key,
            "printer_id": self.printer_id,
            "running": self._running,
            "connected": self._ws_connected and self._printer_connected,
            "tasks_healthy": tasks_healthy,
            "task_status": task_status,
            "bambuddy_printer_id": self._bambuddy_printer_id,
            "ams_count": len(self._current_ams_units),
            "slot_count": total_slots,
            "ams_units": self._current_ams_units,
            "slots": self._current_slots,
            "last_sync_count": self._last_sync_count,
            "last_sync_error": self._last_sync_error,
            "spoolman_enabled": self._spoolman_enabled,
            "spoolman_url": self._spoolman_url,
            "active_slot_mappings": len(self._slot_to_filaman_spool),
            "sync_enabled": self._sync_enabled,
            "sync_actions": [
                {
                    "action": "trigger_sync",
                    "label": "Sync Now",
                    "label_de": "Jetzt synchronisieren",
                    "variant": "secondary",
                },
                {
                    "action": "full_resync",
                    "label": "Full Resync",
                    "label_de": "Vollständiger Resync",
                    "variant": "danger",
                },
            ]
            if self._sync_enabled
            else [],
        }
