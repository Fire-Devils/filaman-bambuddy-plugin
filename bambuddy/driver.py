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
import pathlib
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
from app.models.printer_params import FilamentPrinterParam, SpoolPrinterParam
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
# Frozenset for O(1) "is this a generic fallback?" checks
_GENERIC_SLICER_ID_SET: frozenset[str] = frozenset(_GENERIC_SLICER_IDS.values())

# Reverse-Lookup: Anzeigename → Slicer-Code (z.B. "Generic PLA" → "GFL99")
# FilaMan-Dropdowns speichern den Anzeigenamen, nicht den Key aus bambu_filaments.json.
_FILAMENTS_FILE = pathlib.Path(__file__).parent / "bambu_filaments.json"
_FILAMENT_IDX_TO_NAME: dict[str, str] = {}  # "GFL99" → "Generic PLA"
_FILAMENT_NAME_TO_IDX: dict[str, str] = {}  # "Generic PLA" → "GFL99"
if _FILAMENTS_FILE.exists():
    _raw = json.loads(_FILAMENTS_FILE.read_text(encoding="utf-8"))
    _FILAMENT_IDX_TO_NAME = {k: v for k, v in _raw.items() if not k.startswith("_")}
    _FILAMENT_NAME_TO_IDX = {v: k for k, v in _FILAMENT_IDX_TO_NAME.items()}


def _resolve_slicer_id(raw_value: str | None, material: str) -> str:
    """Löst einen Bambu Slicer-Code aus einem Rohwert auf.

    Der Rohwert kann sein:
    - Ein gültiger Slicer-Code (z.B. "GFL99") → wird direkt zurückgegeben
    - Ein Anzeigename aus dem Dropdown (z.B. "Generic PLA") → Reverse-Lookup
    - None/leer → generischer Fallback anhand des Material-Typs

    Returns:
        Gültiger Bambu Slicer-Code (z.B. "GFL99").
    """
    if not raw_value:
        return _GENERIC_SLICER_IDS.get(material.upper(), "GFL99")
    # Bereits ein gültiger Slicer-Code?
    if raw_value in _FILAMENT_IDX_TO_NAME:
        return raw_value
    # Reverse-Lookup: Anzeigename → Code
    if raw_value in _FILAMENT_NAME_TO_IDX:
        return _FILAMENT_NAME_TO_IDX[raw_value]
    # Sieht wie ein gültiger Slicer-Code aus (z.B. "SUN20019") → direkt verwenden
    if raw_value.isalnum() and raw_value == raw_value.upper() and len(raw_value) >= 3:
        return raw_value
    # Unbekannter Wert: Material-basierter Fallback
    return _GENERIC_SLICER_IDS.get(material.upper(), "GFL99")


# Normalisierung von FilaMan-Materialtypen auf Bambu-Basistypen für tray_type.
# Der Drucker erwartet den Basistyp (z.B. "PLA"), nicht Varianten wie "PLA+" oder "PLA-CF".
_MATERIAL_TYPE_NORMALIZE: dict[str, str] = {
    # PLA-Varianten
    "PLA+": "PLA",
    "PLA-CF": "PLA",
    "PLA-PLUS": "PLA",
    "PLA+/PRO": "PLA",
    "PLA+WOOD": "PLA",
    "APLA": "PLA",
    "PLA SILK": "PLA",
    "PLA MATTE": "PLA",
    "PLA GLOW": "PLA",
    "PLA WOOD": "PLA",
    "PLA MARBLE": "PLA",
    "PLA METAL": "PLA",
    "PLA GALAXY": "PLA",
    "PLA SPARKLE": "PLA",
    "PLA HIGH SPEED": "PLA",
    "WOOD": "PLA",
    # PETG-Varianten
    "PETG-CF": "PETG",
    "PETG-PLUS": "PETG",
    "PETG HF": "PETG",
    "PCTG": "PETG",
    # ABS/ASA-Varianten
    "ABS-GF": "ABS",
    "ABS-PLUS": "ABS",
    "ASA-CF": "ASA",
    "ASA-PLUS": "ASA",
    # PA/Nylon-Varianten
    "PA-CF": "PA",
    "PA6": "PA",
    "PA6-CF": "PA",
    "PA6-GF": "PA",
    "PA12": "PA",
    "PA12-CF": "PA",
    "PA612-CF": "PA",
    "PAHT": "PA",
    "PAHT-CF": "PA",
    "PPA": "PA",
    "PPA-CF": "PA",
    "PPA-GF": "PA",
    "NYLON": "PA",
    # TPU-Varianten
    "TPU 95A": "TPU",
    "TPU 95A HF": "TPU",
    "TPU 90A": "TPU",
    "TPU 85A": "TPU",
    "TPU-85A": "TPU",
    "TPU-90A": "TPU",
    "TPU-95A": "TPU",
    "TPU FOR AMS": "TPU",
    # PC-Varianten
    "PC FR": "PC",
    "PC-ABS": "PC",
    # PET-Varianten
    "PET-CF": "PET",
    # PVA/PVB-Varianten
    "PVB": "PVA",
    # PPS-Varianten
    "PPS-CF": "PPS",
    # PP-Varianten
    "PP-CF": "PP",
    "PP-GF": "PP",
    # PE-Varianten
    "PE-CF": "PE",
    # Support-Materialien
    "SUPPORT": "PLA",
    "SUPPORT G": "PLA",
    "SUPPORT W": "PLA",
    "SUPPORT FOR PLA": "PLA",
    "SUPPORT FOR PLA/PETG": "PLA",
    "SUPPORT FOR PA/PET": "PA",
    "SUPPORT FOR ABS": "ABS",
}


def _normalize_tray_type(material: str) -> str:
    """Normalisiert einen FilaMan-Materialtyp auf den Bambu-Basistyp.

    Args:
        material: Materialtyp aus FilaMan (z.B. "PLA+", "PETG-CF", "PA6-CF")

    Returns:
        Bambu-Basistyp (z.B. "PLA", "PETG", "PA")
    """
    upper = material.upper().strip()
    return _MATERIAL_TYPE_NORMALIZE.get(upper, upper)


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
        self._sync_enabled: bool = config.get("sync_enabled", "disabled") == "enabled"
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
        # Drucker-Seriennummer (für Spoolman Fallback-Tag-Berechnung)
        self._printer_serial: str = ""
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

        # -- Pending Spool (auto-assign via scale RFID scan) --
        self._pending_spool_id: int | None = None
        self._pending_filament_data: dict | None = None
        self._pending_rfid_hex: str | None = None
        self._pending_timer: asyncio.Task | None = None

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

        # -- Bambu-Cloud Preset-Auflösung (PFUS… → AMS-Code wie SUN20013) --
        # filament-id-map (AMS-Code → Anzeigename); reverse für Name → Code.
        self._cloud_idmap_reverse: dict[str, str] = {}
        # forward (AMS-Code → Anzeigename), für slicer_filament_name im Inventory.
        self._cloud_idmap_forward: dict[str, str] = {}
        self._cloud_idmap_ts: float = 0.0
        self._cloud_idmap_ttl: float = 3600.0  # 1h Cache
        # preset_id (PFUS…) → AMS-Code, einmal aufgelöst, dann gecached.
        self._cloud_preset_cache: dict[str, str] = {}

        # -- Cloud-Profil-Picker (Option A) --
        # Gemergte Preset-Liste (cloud/filaments + builtin) für die FilaMan-UI.
        # code → {code, name, displayName, isCustom}; gecached mit TTL.
        self._cloud_presets: list[dict[str, Any]] = []
        self._cloud_presets_by_code: dict[str, dict[str, Any]] = {}
        self._cloud_presets_ts: float = 0.0
        self._cloud_presets_ttl: float = 600.0  # 10min Cache
        # FilaMan-Spool-ID → monotonic ts der letzten lokalen Profiländerung
        # (für Last-Writer-Wins beim Bambuddy→FilaMan-Reflect).
        self._local_profile_writes: dict[int, float] = {}

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
            # Slot-Cache aus Bambuddy-Assignments wiederherstellen (überlebt Neustarts)
            await self._restore_slot_cache_from_assignments()

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
        self._clear_pending()
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
                    selectinload(Spool.filament).selectinload(
                        Filament.printer_params
                    ),
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

    async def _store_original_location_db(
        self, filaman_spool_id: int, location_id: int | None
    ) -> None:
        """Persistiert die Original-Location einer Spule vor AMS-Zuweisung.

        Wird beim Startup genutzt, um _spool_original_location wiederherzustellen,
        da der In-Memory-Cache bei Plugin-Neustart verloren geht.
        "0" ist der Sentinel für None (Spule kam aus dem Lager ohne Location).
        """
        # "0" = no location (came from storage) — must be persisted so restart
        # recovery can distinguish "unknown origin" from "known: was in storage"
        param_value = str(location_id) if location_id is not None else "0"
        try:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(SpoolPrinterParam).where(
                        SpoolPrinterParam.spool_id == filaman_spool_id,
                        SpoolPrinterParam.printer_id == self.printer_id,
                        SpoolPrinterParam.param_key == "original_location_id",
                    )
                )
                existing = result.scalar_one_or_none()
                if existing:
                    existing.param_value = param_value
                else:
                    db.add(
                        SpoolPrinterParam(
                            spool_id=filaman_spool_id,
                            printer_id=self.printer_id,
                            param_key="original_location_id",
                            param_value=param_value,
                        )
                    )
                await db.commit()
        except Exception as e:
            logger.warning(
                f"Failed to persist original location for spool {filaman_spool_id}: {e}"
            )

    async def _get_cloud_idmap_reverse(self) -> dict[str, str]:
        """Lädt (gecached) die Bambu-Cloud filament-id-map als Name → AMS-Code.

        Die Cloud-Map liefert AMS-Code → Anzeigename (z.B. "SUN20013" →
        "SUNLU PLA PLUS GEN2"). Wir cachen das umgekehrte Mapping für die
        Preset-Auflösung. Benötigt einen Cloud-authentifizierten API-Key.
        """
        now = time.monotonic()
        if self._cloud_idmap_reverse and (
            now - self._cloud_idmap_ts < self._cloud_idmap_ttl
        ):
            return self._cloud_idmap_reverse
        try:
            idmap = await self._bb_get("/api/v1/cloud/filament-id-map")
            if isinstance(idmap, dict) and idmap and "detail" not in idmap:
                # code → name  ⇒  name → code
                self._cloud_idmap_reverse = {
                    name: code for code, name in idmap.items()
                }
                self._cloud_idmap_forward = dict(idmap)
                self._cloud_idmap_ts = now
        except Exception as e:
            logger.debug(f"Could not load cloud filament-id-map: {e}")
        return self._cloud_idmap_reverse

    async def _resolve_cloud_preset(self, preset_id: str) -> str | None:
        """Löst einen Bambu-Cloud-Preset (z.B. "PFUS…") zum AMS-Code auf.

        Ablauf:
          1. Ist es bereits ein bekannter AMS-Code (in der id-map)? → direkt zurück.
          2. filament-info(preset_id) → Anzeigename (z.B. "SUNLU PLA PLUS GEN2
             @Bambu Lab P2S 0.4 nozzle"); "@…"-Suffix entfernen → Basisname.
          3. Reverse-Lookup in der id-map: Basisname → AMS-Code (z.B. "SUN20013").

        Ergebnisse werden gecached. Gibt None zurück wenn nicht auflösbar
        (z.B. Cloud nicht verbunden oder unbekannter Custom-Preset).
        """
        if not preset_id:
            return None
        if preset_id in self._cloud_preset_cache:
            return self._cloud_preset_cache[preset_id]

        reverse = await self._get_cloud_idmap_reverse()
        # 1. Schon ein gültiger AMS-Code? (id-map-Werte sind Namen, Keys sind Codes)
        forward_codes = set(reverse.values())
        if preset_id in forward_codes:
            self._cloud_preset_cache[preset_id] = preset_id
            return preset_id

        # 2. + 3. filament-info → Name → Basisname → reverse-Lookup
        try:
            info = await self._bb_post("/api/v1/cloud/filament-info", [preset_id])
        except Exception as e:
            logger.debug(f"cloud/filament-info failed for {preset_id}: {e}")
            return None
        if not isinstance(info, dict):
            return None
        entry = info.get(preset_id) or {}
        name = (entry.get("name") or "").strip()
        if not name:
            return None
        base_name = name.split(" @", 1)[0].strip()
        code = reverse.get(base_name) or reverse.get(name)
        if code:
            self._cloud_preset_cache[preset_id] = code
            logger.info(
                f"Resolved Bambu cloud preset {preset_id!r} "
                f"({base_name!r}) → AMS code {code!r}"
            )
            return code
        logger.debug(
            f"Cloud preset {preset_id!r} name {base_name!r} not found in id-map"
        )
        return None

    async def _spool_cloud_preset(self, filaman_spool_id: int | None) -> str | None:
        """Liest den vom Nutzer in Bambuddy gesetzten Cloud-Preset einer Spule.

        Bambuddy schreibt die Profilauswahl via Spoolman-Feld zurück nach FilaMan,
        gespeichert in spools.custom_fields["bambu_slicer_filament"] (z.B. "PFUS…").
        """
        if not filaman_spool_id:
            return None
        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, filaman_spool_id)
                if not spool or not spool.custom_fields:
                    return None
                cf = spool.custom_fields
                if isinstance(cf, str):
                    cf = json.loads(cf)
                if isinstance(cf, dict):
                    return cf.get("bambu_slicer_filament") or None
        except Exception as e:
            logger.debug(
                f"Could not read cloud preset for spool {filaman_spool_id}: {e}"
            )
        return None

    # -- Cloud-Profil-Picker (Option A) ----------------------------------------

    async def _load_cloud_presets(self, force: bool = False) -> list[dict[str, Any]]:
        """Lädt (gecached) die gemergte Bambu-Cloud-Preset-Liste.

        Quelle (genau wie Bambuddys nativer Picker):
          - GET /api/v1/cloud/filaments       (~1825 Cloud-Presets, code = setting_id)
          - GET /api/v1/cloud/builtin-filaments (generische Basis, code = filament_id/GFxx)

        Liefert eine Liste aus {code, name, displayName, isCustom}. Bei fehlender
        Cloud-Verbindung wird eine leere Liste zurückgegeben (nie Exception).
        """
        now = time.monotonic()
        if (
            not force
            and self._cloud_presets
            and (now - self._cloud_presets_ts) < self._cloud_presets_ttl
        ):
            return self._cloud_presets

        merged: list[dict[str, Any]] = []
        seen: set[str] = set()

        # 1. Generische Basis-Presets (GFxx)
        try:
            builtins = await self._bb_get("/api/v1/cloud/builtin-filaments")
            if isinstance(builtins, list):
                for b in builtins:
                    code = (b.get("filament_id") or "").strip()
                    name = (b.get("name") or "").strip()
                    if not code or code in seen:
                        continue
                    seen.add(code)
                    merged.append(
                        {
                            "code": code,
                            "name": name,
                            "displayName": name,
                            "isCustom": False,
                        }
                    )
        except Exception as e:
            logger.debug(f"Could not load cloud/builtin-filaments: {e}")

        # 2. Cloud-Presets (setting_id), inkl. Drucker-/Düsen-Varianten
        try:
            cloud = await self._bb_get("/api/v1/cloud/filaments")
            if isinstance(cloud, list):
                for c in cloud:
                    code = (c.get("setting_id") or "").strip()
                    name = (c.get("name") or "").strip()
                    if not code or code in seen:
                        continue
                    seen.add(code)
                    is_custom = bool(c.get("is_custom"))
                    display = f"{name} (Custom)" if is_custom else name
                    merged.append(
                        {
                            "code": code,
                            "name": name,
                            "displayName": display,
                            "isCustom": is_custom,
                        }
                    )
        except Exception as e:
            logger.debug(f"Could not load cloud/filaments: {e}")

        if merged:
            self._cloud_presets = merged
            self._cloud_presets_by_code = {p["code"]: p for p in merged}
            self._cloud_presets_ts = now
        return self._cloud_presets

    async def list_cloud_presets(self, force: bool = False) -> dict[str, Any]:
        """Öffentliche Action: gibt die Cloud-Preset-Liste für die FilaMan-UI zurück.

        Returns {"presets": [...], "count": N}.
        """
        presets = await self._load_cloud_presets(force=force)
        return {"presets": presets, "count": len(presets)}

    async def resolve_preset_name(self, code: str | None) -> str | None:
        """Löst einen gespeicherten Code zum Anzeigenamen auf (wie Bambuddys Label).

        Reihenfolge:
          1. filament-id-map (SUN…/GFxx → Name) — gecachte forward-Map
          2. gemergte Preset-Liste (setting_id/GFxx → name)
        """
        if not code:
            return None
        # 1. id-map forward (AMS-Codes wie SUN20013, GFxx)
        await self._get_cloud_idmap_reverse()
        name = self._cloud_idmap_forward.get(code)
        if name:
            return name
        # 2. gemergte Preset-Liste (setting_id)
        if not self._cloud_presets_by_code:
            await self._load_cloud_presets()
        entry = self._cloud_presets_by_code.get(code)
        if entry:
            return entry.get("name") or entry.get("displayName")
        return None

    async def resolve_preset_label(self, code: str | None = None) -> dict[str, Any]:
        """Public action: resolves a stored code to its display name for the UI.

        The FilaMan picker only loads the selectable cloud-preset catalog
        (cloud/filaments + builtins). Codes reflected from the printer's AMS
        (e.g. "SUN20010") live in the separate filament-id-map and are therefore
        absent from that catalog, so the picker would otherwise show the raw
        code. This lets the UI look up the readable name without polluting the
        selectable list. Returns {"code": code, "name": <resolved or code>}.
        """
        if not code:
            return {"code": "", "name": ""}
        name = await self.resolve_preset_name(code)
        return {"code": code, "name": name or code}

    async def _get_bambuddy_spool_id(self, filaman_spool_id: int) -> int | None:
        """Liest die in FilaMan gespeicherte Bambuddy-Spool-ID einer Spule."""
        try:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(SpoolPrinterParam).where(
                        SpoolPrinterParam.spool_id == filaman_spool_id,
                        SpoolPrinterParam.printer_id.in_(self._peer_printer_ids()),
                        SpoolPrinterParam.param_key == "bambuddy_spool_id",
                    )
                )
                for p in result.scalars().all():
                    if p.param_value and p.param_value.isdigit():
                        return int(p.param_value)
        except Exception as e:
            logger.debug(
                f"Could not read bambuddy_spool_id for spool {filaman_spool_id}: {e}"
            )
        return None

    async def _upsert_spool_bambu_idx(self, filaman_spool_id: int, code: str) -> bool:
        """Setzt spool_printer_params.bambu_idx für ALLE Peer-Drucker dieser URL.

        Returns True wenn ein Wert neu geschrieben/geändert wurde.
        """
        changed = False
        async with async_session_maker() as db:
            result = await db.execute(
                select(SpoolPrinterParam).where(
                    SpoolPrinterParam.spool_id == filaman_spool_id,
                    SpoolPrinterParam.printer_id.in_(self._peer_printer_ids()),
                    SpoolPrinterParam.param_key == "bambu_idx",
                )
            )
            existing_by_pid = {p.printer_id: p for p in result.scalars().all()}
            for pid in self._peer_printer_ids():
                existing = existing_by_pid.get(pid)
                if existing:
                    if existing.param_value != code:
                        existing.param_value = code
                        changed = True
                else:
                    db.add(
                        SpoolPrinterParam(
                            spool_id=filaman_spool_id,
                            printer_id=pid,
                            param_key="bambu_idx",
                            param_value=code,
                        )
                    )
                    changed = True
            if changed:
                await db.commit()
        return changed

    async def _upsert_spool_slicer_custom_fields(
        self, filaman_spool_id: int, code: str, name: str | None
    ) -> None:
        """Spiegelt das Slicer-Profil in die Spool-custom_fields (Spoolman-Sicht).

        Bambuddys Spoolman-Sync liest ``bambu_slicer_filament[_name]`` aus den
        Spool-extra-Feldern (in FilaMan = ``Spool.custom_fields``). Fehlen diese
        Felder, fällt Bambuddy auf den Filamentnamen zurück und zeigt ein kurzes
        Ersatzlabel statt des echten Profilnamens. Wir schreiben sie daher beim
        Setzen des Profils, damit beide Sync-Pfade (Treiber-PATCH und
        Spoolman-Sync) denselben vollständigen Namen liefern.
        """
        if not filaman_spool_id or not code:
            return
        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, filaman_spool_id)
                if spool is None:
                    return
                cf = dict(spool.custom_fields or {})
                changed = False
                if cf.get("bambu_slicer_filament") != code:
                    cf["bambu_slicer_filament"] = code
                    changed = True
                if name and cf.get("bambu_slicer_filament_name") != name:
                    cf["bambu_slicer_filament_name"] = name
                    changed = True
                if changed:
                    spool.custom_fields = cf
                    await db.commit()
        except Exception as e:
            logger.debug(
                f"Could not store slicer custom_fields for spool "
                f"{filaman_spool_id}: {e}"
            )

    async def _upsert_spool_color_custom_field(
        self, filaman_spool_id: int, color_name: str | None
    ) -> None:
        """Spiegelt den Hersteller-Farbnamen in die Spool-custom_fields.

        Bambuddys Spoolman-Sync liest ``bambu_color_name`` aus den Spool-extra-
        Feldern (in FilaMan = ``Spool.custom_fields``). Fehlt das Feld, synthetisiert
        Bambuddy den Farbnamen aus dem Subtyp (Material-losen Designation-Rest) und
        zeigt z.B. "Matte" statt des echten Hersteller-Farbnamens. Wir schreiben den
        FilaMan-Farbnamen daher hierher, damit die Bambuddy-Inventarliste den
        korrekten Namen anzeigt. FilaMan ist maßgeblich (Filament-Eigenschaft).
        """
        if not filaman_spool_id or not color_name:
            return
        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, filaman_spool_id)
                if spool is None:
                    return
                cf = dict(spool.custom_fields or {})
                if cf.get("bambu_color_name") != color_name:
                    cf["bambu_color_name"] = color_name
                    spool.custom_fields = cf
                    await db.commit()
        except Exception as e:
            logger.debug(
                f"Could not store color custom_field for spool "
                f"{filaman_spool_id}: {e}"
            )

    async def set_spool_profile(self, spool_id: int, code: str) -> dict[str, Any]:
        """Setzt das Slicer-Profil einer Spule (FilaMan → Bambuddy).

        Genau wie Bambuddys nativer Picker: schreibt `slicer_filament = code`
        (setting_id für Cloud-Presets, GFxx für Builtins) auf die Bambuddy-Spool
        und spiegelt den Wert nach spool_printer_params.bambu_idx in FilaMan.
        """
        if not spool_id or not code:
            raise ValueError("spool_id and code are required")

        # Lokalen Last-Write-Zeitstempel setzen (für LWW-Reflect-Guard)
        self._local_profile_writes[int(spool_id)] = time.monotonic()

        name = await self.resolve_preset_name(code)

        # FilaMan-Seite persistieren: bambu_idx (Treiber-Sicht) + custom_fields
        # (Spoolman-Sicht), damit Bambuddy über beide Sync-Pfade den vollen
        # Profilnamen erhält statt auf den Filamentnamen zurückzufallen.
        await self._upsert_spool_bambu_idx(int(spool_id), code)
        await self._upsert_spool_slicer_custom_fields(int(spool_id), code, name)

        # Bambuddy-Seite patchen (falls die Spool dort schon existiert)
        bb_id = await self._get_bambuddy_spool_id(int(spool_id))
        if bb_id is not None and self._client:
            payload: dict[str, Any] = {"slicer_filament": code}
            if name:
                payload["slicer_filament_name"] = name
            try:
                await self._bb_patch(f"/api/v1/inventory/spools/{bb_id}", payload)
            except Exception as e:
                logger.warning(
                    f"Failed to push profile {code!r} to Bambuddy spool {bb_id}: {e}"
                )
        else:
            # Noch nicht synchronisiert → nächster Sync übernimmt den Wert.
            await self._debounced_sync()

        logger.info(
            f"set_spool_profile: FilaMan spool {spool_id} → {code!r} "
            f"({name or 'unknown name'})"
        )
        return {"code": code, "name": name, "bambuddy_spool_id": bb_id}

    async def set_filament_profile(
        self,
        filament_id: int,
        code: str,
        apply_to_existing: bool = False,
    ) -> dict[str, Any]:
        """Setzt das Default-Slicer-Profil eines Filaments (FilaMan → Bambuddy).

        Schreibt bambu_idx ins filament_printer_params für ALLE Peer-Drucker
        dieser Bambuddy-URL. Neue Spulen dieses Filaments erben den Wert über
        _map_spool. Mit apply_to_existing=True wird das Profil zusätzlich auf
        alle aktiven Bestandsspulen dieses Filaments angewendet (und nach
        Bambuddy gepusht).
        """
        if not filament_id or not code:
            raise ValueError("filament_id and code are required")

        async with async_session_maker() as db:
            if await self._upsert_filament_bambu_idx(db, int(filament_id), code):
                await db.commit()

        name = await self.resolve_preset_name(code)
        applied = 0
        if apply_to_existing:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(Spool.id)
                    .join(SpoolStatus)
                    .where(
                        Spool.filament_id == int(filament_id),
                        SpoolStatus.key != "archived",
                    )
                )
                spool_ids = [row[0] for row in result.all()]
            for sid in spool_ids:
                try:
                    await self.set_spool_profile(sid, code)
                    applied += 1
                except Exception as e:
                    logger.warning(
                        f"apply_to_existing: failed for spool {sid}: {e}"
                    )
        else:
            await self._debounced_sync()

        logger.info(
            f"set_filament_profile: filament {filament_id} → {code!r} "
            f"({name or 'unknown'}); applied_to_existing={applied}"
        )
        return {
            "code": code,
            "name": name,
            "applied_to_existing": applied,
        }

    async def _upsert_filament_bambu_idx(
        self, db: Any, filament_id: int, tray_info_idx: str
    ) -> bool:
        """Setzt bambu_idx für ein Filament (innerhalb einer offenen Session).

        Der AMS-Slicer-Code (z.B. "SUN20013") ist ein globaler Bambu-Identifier,
        nicht druckerspezifisch. Deshalb wird der Wert für ALLE Drucker an dieser
        Bambuddy-URL geschrieben, damit ein auf einem Drucker gelerntes Profil
        sofort auf allen anderen Druckern verfügbar ist (eine Spule kann in jeden
        Drucker gelegt werden). Spiegelt _store_bambuddy_id_db, das ebenfalls für
        alle Peer-printer_ids schreibt.

        Returns True wenn ein Wert neu geschrieben/geändert wurde, sonst False.
        Commit erfolgt durch den Aufrufer.
        """
        printer_ids = self._peer_printer_ids()
        result = await db.execute(
            select(FilamentPrinterParam).where(
                FilamentPrinterParam.filament_id == filament_id,
                FilamentPrinterParam.printer_id.in_(printer_ids),
                FilamentPrinterParam.param_key == "bambu_idx",
            )
        )
        existing_by_pid = {p.printer_id: p for p in result.scalars().all()}
        changed = False
        for pid in printer_ids:
            existing = existing_by_pid.get(pid)
            if existing:
                if existing.param_value != tray_info_idx:
                    existing.param_value = tray_info_idx
                    changed = True
            else:
                db.add(
                    FilamentPrinterParam(
                        filament_id=filament_id,
                        printer_id=pid,
                        param_key="bambu_idx",
                        param_value=tray_info_idx,
                    )
                )
                changed = True
        return changed

    async def _persist_filament_bambu_idx(
        self, filaman_spool_id: int | None, tray_info_idx: str
    ) -> None:
        """Persistiert einen aufgelösten AMS-Code dauerhaft am Filament der Spule.

        Eigene Session + Commit (Standalone-Variante von _upsert_filament_bambu_idx).
        """
        if not filaman_spool_id or not tray_info_idx:
            return
        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, filaman_spool_id)
                if not spool or not spool.filament_id:
                    return
                if await self._upsert_filament_bambu_idx(
                    db, spool.filament_id, tray_info_idx
                ):
                    await db.commit()
        except Exception as e:
            logger.debug(
                f"Could not persist bambu_idx for spool {filaman_spool_id}: {e}"
            )

    async def _learn_slot_profile(
        self,
        filaman_spool_id: int,
        tray_info_idx: str,
        ams_id: int | None = None,
        tray_id: int | None = None,
    ) -> None:
        """Lernt das AMS-Profil (tray_info_idx) für das Filament einer Spule.

        Wenn der Nutzer einen Slot manuell in der Bambuddy-UI konfiguriert, löst
        Bambuddy den Bambu-Cloud-Preset (z.B. "PFUS...") zum AMS-Code (z.B.
        "SUN20013") auf und setzt ihn im AMS. Der Driver beobachtet diesen Code
        und persistiert ihn als FilamentPrinterParam `bambu_idx`. Beim nächsten
        Auto-Assign liefert enrich_filament_data() diesen Wert direkt — der Slot
        bekommt sofort das richtige Profil, ohne Cloud-Zugriff.

        Zusätzlich wird der Code auf ALLE Filamente propagiert, die denselben
        Bambu-Cloud-Preset (custom_fields.bambu_slicer_filament) verwenden — so
        muss pro Preset nur EINE Farbe einmal manuell konfiguriert werden, nicht
        jede Farbe einzeln.
        """
        if not tray_info_idx or tray_info_idx in _GENERIC_SLICER_ID_SET:
            return
        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, filaman_spool_id)
                if not spool or not spool.filament_id:
                    return

                wrote = await self._upsert_filament_bambu_idx(
                    db, spool.filament_id, tray_info_idx
                )

                # Propagation: alle Filamente mit gleichem Cloud-Preset mitlernen.
                # Preset-Quelle 1 (stabil): Bambuddy slot-preset, gesetzt durch das
                # manuelle Configure das dieses Lernen ausgelöst hat.
                # Quelle 2 (volatil, Fallback): FilaMan custom_fields.
                preset = None
                if ams_id is not None and tray_id is not None and self._client:
                    try:
                        sp = await self._bb_get(
                            f"/api/v1/printers/{self._bambuddy_printer_id}"
                            f"/slot-presets/{ams_id}/{tray_id}"
                        )
                        preset = (sp or {}).get("preset_id") or None
                    except Exception as e:
                        logger.debug(f"Could not read slot preset for learning: {e}")
                if not preset and spool.custom_fields:
                    try:
                        cf = json.loads(spool.custom_fields)
                        if isinstance(cf, dict):
                            preset = cf.get("bambu_slicer_filament") or None
                    except (ValueError, TypeError):
                        preset = None

                propagated: list[int] = []
                if preset:
                    sib = await db.execute(
                        select(Spool.filament_id)
                        .where(
                            func.json_extract(
                                Spool.custom_fields, "$.bambu_slicer_filament"
                            )
                            == preset,
                            Spool.filament_id.isnot(None),
                            Spool.filament_id != spool.filament_id,
                        )
                        .distinct()
                    )
                    for (sib_fid,) in sib.all():
                        if await self._upsert_filament_bambu_idx(
                            db, sib_fid, tray_info_idx
                        ):
                            propagated.append(sib_fid)

                if not wrote and not propagated:
                    return  # nothing changed
                await db.commit()
                msg = (
                    f"Learned AMS profile {tray_info_idx!r} for filament "
                    f"{spool.filament_id} (from slot config)"
                )
                if propagated:
                    msg += (
                        f"; propagated to {len(propagated)} sibling filament(s) "
                        f"sharing preset {preset!r}: {propagated}"
                    )
                msg += " — future auto-assigns will apply it automatically"
                logger.info(msg)
        except Exception as e:
            logger.warning(
                f"Failed to learn slot profile for spool {filaman_spool_id}: {e}"
            )

    async def _delete_original_location_db(self, filaman_spool_id: int) -> None:
        """Entfernt den persistierten Original-Location-Eintrag nach Restore."""
        try:
            async with async_session_maker() as db:
                await db.execute(
                    delete(SpoolPrinterParam).where(
                        SpoolPrinterParam.spool_id == filaman_spool_id,
                        SpoolPrinterParam.printer_id == self.printer_id,
                        SpoolPrinterParam.param_key == "original_location_id",
                    )
                )
                await db.commit()
        except Exception as e:
            logger.warning(
                f"Failed to delete original location param for spool "
                f"{filaman_spool_id}: {e}"
            )

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

    def _map_spool(
        self,
        spool: Spool,
        existing_slicer: str | None = None,
        existing_name: str | None = None,
    ) -> dict:
        """FilaMan Spool (ORM) → Bambuddy SpoolCreate/Update-Payload.

        Mapping:
          filament.material_type                    → material
          filament.manufacturer.name                → brand
          filament.filament_colors[0].color.hex_code→ rgba (8-stellig RRGGBBAA)
          filament.manufacturer_color_name          → color_name
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
        # bambu_idx (das gelernte AMS-Profil) wird auf Filament-Ebene persistiert
        # (filament_printer_params, via _learn_slot_profile/_reconcile_cloud_presets),
        # nicht auf Spool-Ebene. Daher hier die Filament-Params zusätzlich einlesen.
        fpp: dict[str, str | None] = {
            p.param_key: p.param_value
            for p in ((fil.printer_params or []) if fil else [])
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

        # Hersteller-Farbname (FilaMan) → Bambuddy color_name (Inventar-Feld).
        # FilaMan ist maßgeblich für den Farbnamen (Filament-Eigenschaft).
        if fil and fil.manufacturer_color_name:
            payload["color_name"] = fil.manufacturer_color_name

        if spool.rfid_uid:
            payload["tag_uid"] = self._to_hex_tag(spool.rfid_uid)

        # Profil-Auflösung: Das in Bambuddy gewählte Profil ist maßgeblich (der
        # Nutzer wählt dort das echte Slicer-Preset). Dieses NIE überschreiben —
        # nur erhalten. Ein gelerntes bambu_idx füllt lediglich einen leeren Slot
        # (Erst-Sync, bevor der Nutzer ein Preset gesetzt hat). Sonst würde jeder
        # Sync die Nutzerauswahl wieder auf den gelernten Basis-Code zurücksetzen
        # (z.B. spezifisches Preset → "SUN20013"), was das AMS falsch konfiguriert.
        if existing_slicer:
            payload["slicer_filament"] = existing_slicer
            if existing_name:
                payload["slicer_filament_name"] = existing_name
        else:
            raw_slicer = (
                fpp.get("bambu_idx")
                or fpp.get("bambu_tray_idx")
                or pp.get("bambu_idx")
                or pp.get("bambu_tray_idx")
            )
            if raw_slicer:
                # Cloud-Preset-Setting-IDs (PFUS…) werden – wie in Bambuddys
                # nativem Picker – direkt als slicer_filament durchgereicht und
                # NICHT auf einen generischen Basis-Code aufgelöst.
                if raw_slicer.startswith("PFUS") or (
                    raw_slicer in self._cloud_presets_by_code
                ):
                    code = raw_slicer
                else:
                    code = _resolve_slicer_id(raw_slicer, payload["material"])
                payload["slicer_filament"] = code
                name = self._cloud_idmap_forward.get(code) or (
                    self._cloud_presets_by_code.get(code, {}).get("name")
                )
                if name:
                    payload["slicer_filament_name"] = name

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
            # 0. Cloud-id-map (code → name) vorwärmen, damit _map_spool den
            #    lesbaren slicer_filament_name mitsenden kann (gecached, 1h TTL).
            await self._get_cloud_idmap_reverse()

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
                existing = note_index.get(note_key)
                payload = self._map_spool(
                    fm_spool,
                    existing_slicer=(existing or {}).get("slicer_filament"),
                    existing_name=(existing or {}).get("slicer_filament_name"),
                )

                try:
                    if existing is not None:
                        bb_id = existing["id"]
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

                # Bambuddy → FilaMan: gesetztes Profil zurückspiegeln (LWW).
                if existing is not None:
                    await self._reflect_spool_profile(
                        fm_id, existing.get("slicer_filament")
                    )

                # Effektives Profil (Bambuddy-Wert oder vererbtes bambu_idx) in die
                # Spool-custom_fields spiegeln, damit Bambuddys Spoolman-Sync den
                # vollen Profilnamen sieht – auch für neue/vererbte Spulen, die nie
                # explizit über set_spool_profile gesetzt wurden. Generische
                # Fallback-Codes werden ausgelassen; ein kürzlich lokal gesetztes
                # Profil gewinnt (Last-Writer-Wins, wie beim Reflect).
                eff_code = payload.get("slicer_filament")
                if eff_code and eff_code not in _GENERIC_SLICER_ID_SET:
                    last = self._local_profile_writes.get(fm_id)
                    recent_local = (
                        last is not None and (time.monotonic() - last) < 300.0
                    )
                    if not recent_local:
                        eff_name = payload.get(
                            "slicer_filament_name"
                        ) or await self.resolve_preset_name(eff_code)
                        await self._upsert_spool_slicer_custom_fields(
                            fm_id, eff_code, eff_name
                        )

                # Hersteller-Farbname in custom_fields spiegeln, damit Bambuddys
                # Spoolman-Sync ihn als bambu_color_name liest und in der Inventar-
                # liste anzeigt (statt den synthetisierten Subtyp). Backfillt auch
                # bestehende Spulen beim nächsten Sync.
                await self._upsert_spool_color_custom_field(
                    fm_id, payload.get("color_name")
                )

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

            # Proaktive Cloud-Preset-Auflösung: jede Spule mit einem in Bambuddy
            # gesetzten Profil (custom_fields.bambu_slicer_filament) wird zum AMS-Code
            # aufgelöst und dauerhaft am Filament gespeichert — solange das volatile
            # custom_fields-Feld noch befüllt ist. So ist der Code vor dem Einlegen
            # bereit und Auto-Assign braucht keinen Cloud-Call mehr.
            await self._reconcile_cloud_presets(fm_spools)

        except Exception as e:
            self._last_sync_error = str(e)
            logger.error(f"Inventory sync failed for printer {self.printer_id}: {e}")
        finally:
            self._syncing = False

    async def _reflect_spool_profile(
        self, filaman_spool_id: int, existing_slicer: str | None
    ) -> None:
        """Spiegelt das in Bambuddy gesetzte Profil zurück nach FilaMan (LWW).

        Schreibt das von Bambuddy gemeldete `slicer_filament` in
        spool_printer_params.bambu_idx, sodass beide Systeme dasselbe Profil
        zeigen. Guards:
          - generische/leere Codes werden ignoriert
          - laufende Zuweisung (pending) pausiert das Reflect
          - Last-Writer-Wins: eine kürzliche lokale Änderung gewinnt
        """
        if not existing_slicer or existing_slicer in _GENERIC_SLICER_ID_SET:
            return
        if self._pending_spool_id == filaman_spool_id:
            return
        last = self._local_profile_writes.get(filaman_spool_id)
        if last is not None and (time.monotonic() - last) < 300.0:
            return  # FilaMan war der jüngere Writer
        try:
            changed = await self._upsert_spool_bambu_idx(
                filaman_spool_id, existing_slicer
            )
            if changed:
                logger.info(
                    f"Reflected Bambuddy profile {existing_slicer!r} → "
                    f"FilaMan spool {filaman_spool_id}"
                )
            else:
                # Bereits synchron → Marker aufräumen.
                self._local_profile_writes.pop(filaman_spool_id, None)
        except Exception as e:
            logger.debug(
                f"Could not reflect profile for spool {filaman_spool_id}: {e}"
            )

    async def _reconcile_cloud_presets(self, fm_spools: list[Spool]) -> None:
        """Löst gesetzte Bambu-Cloud-Presets auf und persistiert sie am Filament.

        Liest pro Spule custom_fields.bambu_slicer_filament (z.B. "PFUS…"), löst
        es via Bambu cloud zum AMS-Code (z.B. "SUN20013") auf und schreibt es als
        bambu_idx ins filament_printer_params. Idempotent — nur Änderungen werden
        geschrieben. Fehlende Cloud-Auth oder unbekannte Presets werden still
        übersprungen.
        """
        for spool in fm_spools:
            try:
                cf = spool.custom_fields
                if isinstance(cf, str):
                    cf = json.loads(cf)
                if not isinstance(cf, dict):
                    continue
                preset_id = cf.get("bambu_slicer_filament") or None
                if not preset_id:
                    continue
                code = await self._resolve_cloud_preset(preset_id)
                if code:
                    await self._persist_filament_bambu_idx(spool.id, code)
            except Exception as e:
                logger.debug(
                    f"Cloud-preset reconcile skipped for spool {spool.id}: {e}"
                )

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

    # -- Pending Spool (auto-assign) -----------------------------------------

    async def assign_pending_spool(
        self,
        spool_id: int,
        filament_data: dict,
        slot_index: str | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        """Mark a spool as pending for the next AMS tray insertion.

        Called by Filaman's auto-assign flow when the scale scans a spool RFID.
        Stores the spool ID and preloads its rfid_uid so _process_slots can
        match it against incoming tray data from Bambuddy's WebSocket.
        """
        if self._pending_timer and not self._pending_timer.done():
            self._pending_timer.cancel()

        rfid_hex: str | None = None
        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, spool_id)
                if spool and spool.rfid_uid:
                    rfid_hex = self._to_hex_tag(spool.rfid_uid)
        except Exception as e:
            logger.warning(f"Could not load rfid_uid for pending spool {spool_id}: {e}")

        self._pending_spool_id = spool_id
        self._pending_filament_data = {**filament_data}
        self._pending_rfid_hex = rfid_hex

        effective_timeout = timeout_seconds if timeout_seconds is not None else 300
        self._pending_timer = asyncio.create_task(
            self._pending_timeout(effective_timeout)
        )
        self._pending_timer.add_done_callback(self._on_task_done)
        logger.info(
            f"Pending spool {spool_id} set for printer {self.printer_id} "
            f"(rfid={rfid_hex}, timeout={effective_timeout}s)"
        )

    async def _pending_timeout(self, timeout: int) -> None:
        await asyncio.sleep(timeout)
        if self._pending_spool_id is not None:
            logger.info(
                f"Pending spool {self._pending_spool_id} timed out "
                f"on printer {self.printer_id}"
            )
        self._pending_spool_id = None
        self._pending_filament_data = None
        self._pending_rfid_hex = None
        self._pending_timer = None

    def _clear_pending(self) -> None:
        """Clear pending spool state and cancel timeout."""
        if self._pending_timer and not self._pending_timer.done():
            self._pending_timer.cancel()
        self._pending_spool_id = None
        self._pending_filament_data = None
        self._pending_rfid_hex = None
        self._pending_timer = None

    def _clear_pending_peers(self) -> None:
        """Clear pending state on this driver AND all peer drivers on the same
        Bambuddy URL.

        A scale scan arms every printer (Filaman's auto-assign notifies all
        drivers). Once the spool is physically inserted into one printer and
        matched here, the other printers must be disarmed too — otherwise a
        DIFFERENT spool inserted into another printer within the remaining
        auto-assign window would false-match this pending spool. Third-party
        spools have no readable RFID, so they match purely on slot-appeared,
        which makes that mis-trigger easy to hit.
        """
        # Capture the target before the loop: clearing self mid-iteration would
        # null self._pending_spool_id and skip the remaining peers.
        target = self._pending_spool_id
        for d in self._url_instances.get(self._bambuddy_url, [self]):
            if d._pending_spool_id == target:
                d._clear_pending()

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
            ws_base = (
                self._bambuddy_url.rstrip("/")
                .replace("https://", "wss://")
                .replace("http://", "ws://")
            ) + "/api/v1/ws"
            try:
                # Bambuddy (>=0.2.4) requires a short-lived ws-token for the
                # WebSocket handshake; the X-API-Key header alone returns HTTP 403.
                # Mint a fresh token per connection attempt and pass it as a query
                # param (the header is kept for backward compatibility).
                uri = ws_base
                try:
                    if self._client is not None:
                        tok_resp = await self._bb_post("/api/v1/auth/ws-token", {})
                        ws_token = (tok_resp or {}).get("token")
                        if ws_token:
                            uri = f"{ws_base}?token={ws_token}"
                except Exception as e:
                    logger.warning(f"Could not mint ws-token (will try without): {e}")

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
                            self.log_debug(
                                "in",
                                f"ws/{event.get('type', 'unknown')}",
                                event,
                            )
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

        elif event_type == "inventory_changed":
            # Refresh-on-save: ein Profil-/Inventory-Wechsel in Bambuddy stößt
            # einen (debounced) Sync an, damit FilaMan zeitnah nachzieht.
            await self._debounced_sync()

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
            # Slot SOFORT freigeben, damit der Restore-Task den Guard in
            # _restore_spool_location() nicht als "noch aktiv" interpretiert
            del self._slot_to_filaman_spool[slot_key]
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
                    original_loc = spool.location_id if spool else None
                    self._spool_original_location[filaman_spool_id] = original_loc
                    # Original-Location in DB persistieren (überlebt Plugin-Neustart)
                    await self._store_original_location_db(
                        filaman_spool_id, original_loc
                    )
            except Exception as e:
                logger.warning(
                    f"Failed to cache original location for FilaMan spool "
                    f"{filaman_spool_id}: {e}"
                )
            # Spoolman-Linking nur wenn Inventory-Sync DEAKTIVIERT ist
            # (Bei aktiviertem Sync nutzt Bambuddy sein eigenes Inventar, nicht Spoolman)
            # Funktion selbst prüft zusätzlich _spoolman_enabled (defensive Programmierung)
            if not self._sync_enabled:
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

        # Inventory-Assignment (best-effort): registriert Bambuddy-interne Verknüpfung,
        # steuert aber NICHT zuverlässig tray_info_idx — deshalb immer _send_assignment danach.
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
            except Exception as e:
                logger.warning(
                    f"Assignment API failed (slot {ams_id}/{tray_id}), "
                    f"continuing with configure-call: {e}"
                )

        # Immer configure-Call ausführen um tray_info_idx via MQTT zu setzen
        await self._send_assignment(ams_id, tray_id, filament_data)

        if filaman_spool_id:
            await self._update_spool_location(filaman_spool_id, ams_id, tray_id)
            self._slot_to_filaman_spool[slot_key] = filaman_spool_id

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
        material_raw = filament_data.get("material_type", "PLA")
        material = _normalize_tray_type(material_raw)  # z.B. "PLA+" → "PLA"

        # Priority 1: a previously resolved/learned AMS code in the filament's
        # printer params (durable, fed in via enrich_filament_data as bambu_idx).
        bambu_idx_hint = filament_data.get("bambu_idx") or filament_data.get("bambu_tray_idx")

        # Priority 2: the cloud preset the user set on the spool in Bambuddy
        # (stored in FilaMan custom_fields as "bambu_slicer_filament", e.g. "PFUS…").
        # Resolve it live via Bambu cloud → AMS code (e.g. "SUN20013") and persist
        # the result to the filament's bambu_idx so it survives custom_fields wipes
        # and every future auto-assign is instant without another cloud call.
        if not bambu_idx_hint:
            fm_spool_id = _int_or_none(filament_data.get("id"))
            preset_id = await self._spool_cloud_preset(fm_spool_id)
            if preset_id:
                resolved = await self._resolve_cloud_preset(preset_id)
                if resolved:
                    bambu_idx_hint = resolved
                    await self._persist_filament_bambu_idx(fm_spool_id, resolved)
                    logger.info(
                        f"Using cloud preset {preset_id!r} → {resolved!r} "
                        f"for slot {ams_id}/{tray_id}"
                    )

        # Priority 3: the Bambuddy inventory spool's slicer_filament (legacy path).
        if not bambu_idx_hint:
            bb_spool_id = _int_or_none(filament_data.get("bambuddy_spool_id"))
            if bb_spool_id and self._client:
                try:
                    bb_spool = await self._bb_get(f"/api/v1/inventory/spools/{bb_spool_id}")
                    bambu_idx_hint = bb_spool.get("slicer_filament") or None
                    if bambu_idx_hint:
                        logger.info(
                            f"Using Bambuddy spool {bb_spool_id} slicer_filament "
                            f"{bambu_idx_hint!r} for slot {ams_id}/{tray_id}"
                        )
                except Exception as e:
                    logger.debug(f"Could not fetch Bambuddy spool {bb_spool_id}: {e}")

        slicer_filament = _resolve_slicer_id(bambu_idx_hint, material)

        # tray_sub_brands: Filament-Anzeigename aus Lookup oder Fallback auf Material
        tray_sub_brands = (
            filament_data.get("material_subgroup")
            or _FILAMENT_IDX_TO_NAME.get(slicer_filament)
            or material_raw
        )

        # -- Temperaturen --
        nozzle_temp_min = _int_or_none(
            filament_data.get("bambu_nozzle_temp_min")
        ) or _int_or_none(filament_data.get("nozzle_temp_min"))
        nozzle_temp_max = _int_or_none(
            filament_data.get("bambu_nozzle_temp_max")
        ) or _int_or_none(filament_data.get("nozzle_temp_max"))

        # k_value für configure-Endpoint — 0.0 = skip (kein K-Profil setzen)
        k_value = _float_or_none(filament_data.get("bambu_k_value")) or 0.0

        # cali_idx: Aus Zusatzfeldern der Spule, oder -1 (Drucker-Default)
        cali_idx = _int_or_none(filament_data.get("bambu_cali_idx"))
        if cali_idx is None:
            cali_idx = -1

        configure_params: dict[str, Any] = {
            "tray_info_idx": slicer_filament,
            "tray_type": material,
            "tray_sub_brands": tray_sub_brands,
            "tray_color": color,  # 8-stellig RRGGBBAA
            "nozzle_temp_min": nozzle_temp_min or 190,  # REQUIRED — Fallback 190°C
            "nozzle_temp_max": nozzle_temp_max or 230,  # REQUIRED — Fallback 230°C
            "cali_idx": cali_idx,
            "setting_id": filament_data.get("bambu_setting_id") or "",
            "kprofile_filament_id": slicer_filament,
            "kprofile_setting_id": filament_data.get("bambu_setting_id") or "",
            "k_value": k_value,  # 0.0 = skip
        }

        try:
            # Neue Konfiguration setzen
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

    @staticmethod
    def _to_hex_tag(raw: str) -> str:
        """Strip ALL non-hex chars and pad to 16 (or 32) uppercase hex characters.

        Bambuddy requires spool tags to be exactly 16 or 32 hex characters.
        FilaMan stores rfid_uid colon-separated (e.g. '04:f5:02:3a') which
        needs to be converted to raw uppercase hex, zero-padded.

        Used for Inventory Sync payload (tag_uid field).
        """
        _HEX = set("0123456789abcdefABCDEF")
        hex_only = "".join(c for c in raw if c in _HEX)
        if not hex_only:
            return ""
        if len(hex_only) > 16:
            return hex_only.upper().zfill(32)
        return hex_only.upper().zfill(16)

    @staticmethod
    def _hash_serial_to_hex32(serial: str) -> str:
        """FNV-1a hash of printer serial number to 8 uppercase hex chars.

        Mirrors Bambuddy frontend hashSerialToHex32() and backend
        _hash_serial_to_hex32() exactly — deterministic tag generation.
        """
        input_str = (serial or "").strip().upper()
        hash_value = 0x811C9DC5
        for char in input_str:
            hash_value ^= ord(char)
            hash_value = (hash_value * 0x01000193) & 0xFFFFFFFF
        return format(hash_value, "X").zfill(8)

    def _get_fallback_spool_tag(self, ams_id: int, tray_id: int) -> str:
        """Generate deterministic 16-hex-char spool tag from printer serial + slot.

        Mirrors Bambuddy frontend getFallbackSpoolTag(serial, amsId, trayId)
        and backend _get_fallback_spool_tag() exactly. Used for Spoolman
        linking when the AMS tray has no Bambu Lab RFID tag (third-party spools).
        """
        if not self._printer_serial:
            return ""
        h = self._hash_serial_to_hex32(self._printer_serial)
        a = format(max(0, ams_id), "X").zfill(4)[-4:]
        t = format(max(0, tray_id), "X").zfill(4)[-4:]
        return f"{h}{a}{t}"

    async def _handle_spoolman_linking(
        self, ams_id: int, tray_id: int, filaman_spool_id: int
    ) -> None:
        # Early return wenn Inventory-Sync AKTIVIERT
        # (Bei aktiviertem Sync nutzt Bambuddy sein Inventar, nicht Spoolman)
        if self._sync_enabled:
            return

        if not self._spoolman_enabled:
            logger.debug(
                f"Spoolman linking skipped for spool {filaman_spool_id}: "
                f"spoolman_enabled=False on Bambuddy side"
            )
            return

        if not self._client:
            logger.warning("Spoolman linking skipped: HTTP client not initialized")
            return

        # -- Diagnostic-Log am Einstiegspunkt --
        logger.info(
            f"Spoolman linking: spool={filaman_spool_id} -> tray {ams_id}/{tray_id} "
            f"(sync_enabled={self._sync_enabled}, "
            f"spoolman_enabled={self._spoolman_enabled})"
        )

        try:
            # Bei Spoolman-Integration ist die FilaMan-Spool-ID identisch mit der
            # Spoolman-Spool-ID (via SpoolmanAPI-Plugin). Diese wird direkt für
            # den Link-API-Call verwendet, NICHT die Bambuddy-Inventory-Spool-ID.
            spoolman_spool_id = filaman_spool_id

            # -- Fallback-Tag berechnen --
            # Für Drittanbieter-Spulen (ohne Bambu-RFID) generiert das Frontend
            # einen deterministischen Tag aus Drucker-Serial + Slot-Position.
            # Wir verwenden exakt denselben Algorithmus (FNV-1a), damit das
            # Frontend die Verknüpfung erkennt.
            resolved_tag = self._get_fallback_spool_tag(ams_id, tray_id)
            if not resolved_tag:
                logger.warning(
                    f"Spoolman linking skipped for spool {spoolman_spool_id}: "
                    f"no printer serial available (needed for fallback tag)"
                )
                return

            logger.debug(
                f"Spoolman link tag for slot {ams_id}/{tray_id}: "
                f"tag={resolved_tag} (fallback from serial={self._printer_serial!r})"
            )

            # -- Alte Spoolman-Verknüpfung erkennen und entfernen --
            # Über die Spoolman-Linked-API abfragen. Das Format ist
            # {"linked": {"<TAG_UPPER>": {"id": ..., ...}}}
            old_spool_id: int | None = None
            try:
                linked_resp = await self._bb_get("/api/v1/spoolman/spools/linked")
                if isinstance(linked_resp, dict):
                    linked_map = linked_resp.get("linked", linked_resp)
                    existing = linked_map.get(resolved_tag.upper())
                    if existing is not None:
                        # existing kann int oder dict mit "id" sein
                        if isinstance(existing, dict):
                            existing_id = int(existing.get("id", 0))
                        else:
                            existing_id = int(existing)
                        if existing_id and existing_id != spoolman_spool_id:
                            old_spool_id = existing_id
            except Exception as e:
                logger.debug(f"Could not fetch linked spools for unlink check: {e}")

            if old_spool_id:
                try:
                    unlink_resp = await self._client.post(
                        f"{self._bambuddy_url}/api/v1/spoolman/spools/{old_spool_id}/unlink"
                    )
                    unlink_resp.raise_for_status()
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

            # -- Neue Spoolman-Verknüpfung setzen --
            # Bambuddy speichert den Tag als extra.tag auf dem Spoolman-Spool.
            # Das Frontend schaut dann per getFallbackSpoolTag() nach genau
            # diesem Tag. Wir senden als spool_tag (höchste Prio in der
            # OR-Kette: spool_tag > tray_uuid > tag_uid).
            link_body: dict[str, Any] = {
                "spool_tag": resolved_tag,
                "printer_id": self._bambuddy_printer_id,
                "ams_id": ams_id,
                "tray_id": tray_id,
            }

            logger.debug(
                f"Spoolman link request for spool {spoolman_spool_id}: {link_body}"
            )

            try:
                link_resp = await self._client.post(
                    f"{self._bambuddy_url}/api/v1/spoolman/spools/{spoolman_spool_id}/link",
                    json=link_body,
                )
                link_resp.raise_for_status()
                self.log_debug(
                    "out",
                    f"POST /api/v1/spoolman/spools/{spoolman_spool_id}/link",
                    {
                        "status": link_resp.status_code,
                        **link_body,
                    },
                )
                logger.info(
                    f"Linked Spoolman spool {spoolman_spool_id} to "
                    f"printer {self._bambuddy_printer_id} tray {ams_id}/{tray_id} "
                    f"(tag={resolved_tag})"
                )
            except httpx.HTTPStatusError as e:
                logger.error(
                    f"Spoolman link API error for spool {spoolman_spool_id}: "
                    f"{e.response.status_code} {e.response.text}"
                )
            except Exception as e:
                logger.warning(
                    f"Failed to link Spoolman spool {spoolman_spool_id} "
                    f"to tray {ams_id}/{tray_id}: {e}"
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

        original_location_id = self._spool_original_location.pop(filaman_spool_id)

        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, filaman_spool_id)
                if spool and spool.location_id != original_location_id:
                    await SpoolService(db).move_location(
                        spool,
                        original_location_id,  # None = clear location (spool came from storage)
                        datetime.now(timezone.utc),
                        source="driver",
                        note="Restored from AMS tray",
                    )
            # Persistierten Original-Location-Eintrag aufräumen
            await self._delete_original_location_db(filaman_spool_id)
        except Exception as e:
            logger.warning(f"Failed to restore spool {filaman_spool_id} location: {e}")

    async def _reconfigure_slot_with_profile(
        self, ams_id: int, tray_id: int, tray_info_idx: str, tray: dict
    ) -> None:
        """Re-push slot config when AMS NFC read completes with a specific profile.

        Called when _process_slots detects a tray_info_idx transition from
        generic/empty → specific (e.g. "" → "GFA01") on an already-assigned slot.
        Builds minimal filament_data from cached slot params and calls _send_assignment.
        """
        slot_key = f"{ams_id}-{tray_id}"
        cached = self._slot_params_cache.get(slot_key, {})
        filament_data = {
            "color": tray.get("tray_color", "FFFFFFFF"),
            "material_type": tray.get("tray_type", "PLA"),
            "bambu_idx": tray_info_idx,
            "bambu_nozzle_temp_min": cached.get("nozzle_temp_min"),
            "bambu_nozzle_temp_max": cached.get("nozzle_temp_max"),
            "bambu_k_value": cached.get("bambu_k_value"),
            "bambu_cali_idx": cached.get("bambu_cali_idx"),
            "bambu_setting_id": cached.get("bambu_setting_id"),
        }
        await self._send_assignment(ams_id, tray_id, filament_data)

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
                    # Nur ggf. neu erstellte Location persistieren (kein Move nötig)
                    await db.commit()
                    return

                # SpoolService für konsistente Event-Generierung nutzen
                # (move_location() committet intern — inkl. gefluschter Location)
                await SpoolService(db).move_location(
                    spool,
                    location.id,
                    datetime.now(timezone.utc),
                    source="driver",
                    note=f"Assigned to {slot_location_name}",
                )

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

    async def _restore_slot_cache_from_assignments(self) -> None:
        """Stellt _slot_to_filaman_spool und _spool_original_location beim Start wieder her.

        Nutzt Bambuddys GET /api/v1/inventory/assignments um zu erfahren, welche
        Spulen aktuell welchen AMS-Slots zugewiesen sind. Über SpoolPrinterParam
        wird die bambuddy_spool_id auf die filaman_spool_id zurückgemappt.
        """
        if not self._client or not self._bambuddy_printer_id:
            return

        try:
            assignments = await self._bb_get(
                "/api/v1/inventory/assignments",
                params={"printer_id": self._bambuddy_printer_id},
            )
        except Exception as e:
            logger.warning(f"Failed to fetch assignments for cache recovery: {e}")
            return

        if not assignments:
            return

        # Bambuddy-Spool-ID → FilaMan-Spool-ID Reverse-Lookup aufbauen
        try:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(SpoolPrinterParam).where(
                        SpoolPrinterParam.printer_id == self.printer_id,
                        SpoolPrinterParam.param_key == "bambuddy_spool_id",
                    )
                )
                bb_params = result.scalars().all()
                bb_to_filaman: dict[int, int] = {
                    int(p.param_value): p.spool_id for p in bb_params
                }

                # Original-Location-Einträge laden
                result = await db.execute(
                    select(SpoolPrinterParam).where(
                        SpoolPrinterParam.printer_id == self.printer_id,
                        SpoolPrinterParam.param_key == "original_location_id",
                    )
                )
                loc_params = result.scalars().all()
                orig_locs: dict[int, int | None] = {
                    p.spool_id: (None if p.param_value in ("0", "null", "") else int(p.param_value))
                    for p in loc_params
                }
        except Exception as e:
            logger.warning(f"Failed to load SpoolPrinterParams for cache recovery: {e}")
            return

        recovered = 0
        for assignment in assignments:
            bb_spool_id = assignment.get("spool_id")
            ams_id = assignment.get("ams_id")
            tray_id = assignment.get("tray_id")

            if bb_spool_id is None or ams_id is None or tray_id is None:
                continue

            filaman_spool_id = bb_to_filaman.get(bb_spool_id)
            if not filaman_spool_id:
                continue

            slot_key = f"{ams_id}-{tray_id}"
            self._slot_to_filaman_spool[slot_key] = filaman_spool_id

            # Original-Location aus DB wiederherstellen
            if filaman_spool_id in orig_locs:
                self._spool_original_location[filaman_spool_id] = orig_locs[
                    filaman_spool_id
                ]

            recovered += 1

        if recovered:
            logger.info(
                f"Recovered {recovered} slot-to-spool assignments from Bambuddy API"
            )

    async def _fetch_and_emit_status(self) -> None:
        """Initialen Drucker-Status von Bambuddy REST-API laden und als slots_update emittieren."""
        if not self._client or not self._bambuddy_printer_id:
            return

        # -- Printer-Seriennummer laden (für Spoolman Fallback-Tag) --
        if not self._printer_serial:
            try:
                pr = await self._client.get(
                    f"{self._bambuddy_url}/api/v1/printers/{self._bambuddy_printer_id}"
                )
                if pr.status_code == 200:
                    self._printer_serial = pr.json().get("serial_number", "")
                    if self._printer_serial:
                        logger.info(
                            f"Loaded printer serial '{self._printer_serial}' "
                            f"for Bambuddy printer {self._bambuddy_printer_id}"
                        )
                    else:
                        logger.warning(
                            f"Bambuddy printer {self._bambuddy_printer_id} "
                            f"has no serial_number — Spoolman fallback tag unavailable"
                        )
            except Exception as e:
                logger.warning(f"Could not fetch printer serial: {e}")

        try:
            status_url = f"/api/v1/printers/{self._bambuddy_printer_id}/status"
            self.log_debug("out", f"GET {status_url}", {})
            r = await self._client.get(f"{self._bambuddy_url}{status_url}")
            if r.status_code == 200:
                status_data = r.json()
                self.log_debug("in", f"GET {status_url}", status_data)
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

                # Pending spool: fire send_filament_to_tray when a spool is inserted.
                #
                # Two strategies in priority order:
                # 1. RFID match — tray tag_uid equals pending spool's rfid_uid.
                #    Works for genuine Bambu spools whose NFC the AMS can read.
                # 2. Slot-appeared match — slot transitioned empty → occupied.
                #    Works for all third-party spools (tag_uid stays null because
                #    the AMS cannot read external RFID stickers; only the scale can).
                if self._pending_spool_id is not None and tray_type:
                    tray_tag_uid = (tray.get("tag_uid") or "").upper()
                    prev_slot = next(
                        (s for s in self._current_slots if s["slot_index"] == slot_index),
                        None,
                    )
                    slot_was_empty = prev_slot is None or not prev_slot.get("present", False)

                    rfid_matched = bool(
                        tray_tag_uid
                        and self._pending_rfid_hex
                        and tray_tag_uid == self._pending_rfid_hex
                    )
                    # For third-party spools tag_uid is null; fall back to slot transition.
                    # Also detect direct swaps: if Bambuddy collapses occupied→empty→occupied
                    # into a single occupied→occupied snapshot, slot_was_empty is False even
                    # though a physical spool swap happened. Treat content changes (type or
                    # color differing from previous snapshot) as a swap trigger.
                    prev_tray_type = (prev_slot.get("tray_type") or "") if prev_slot else ""
                    prev_tray_color = (prev_slot.get("tray_color") or "") if prev_slot else ""
                    content_changed = (
                        not slot_was_empty
                        and prev_slot is not None
                        and prev_slot.get("present", False)
                        and not tray_tag_uid
                        and (tray_type != prev_tray_type or tray_color != prev_tray_color)
                    )
                    slot_matched = (slot_was_empty or content_changed) and not tray_tag_uid

                    if rfid_matched or slot_matched:
                        reason = "rfid" if rfid_matched else ("direct-swap" if content_changed else "slot-appeared")
                        logger.info(
                            f"Pending spool {self._pending_spool_id} matched "
                            f"AMS {ams_id}/tray {tray_id} ({reason})"
                        )
                        self.send_filament_to_tray(
                            ams_id, tray_id, {**self._pending_filament_data}
                        )
                        self._clear_pending_peers()

                # Specific (non-generic) profile on a known slot: learn it.
                # Captures the AMS code Bambuddy resolved when the user manually
                # configures a slot (cloud preset → e.g. "SUN20013"), and persists
                # it for the filament so future auto-assigns apply it without cloud.
                if (
                    tray_type
                    and slot_index in self._slot_to_filaman_spool
                    and tray_info_idx
                    and tray_info_idx not in _GENERIC_SLICER_ID_SET
                ):
                    learn_spool_id = self._slot_to_filaman_spool[slot_index]
                    _lt = asyncio.create_task(
                        self._learn_slot_profile(
                            learn_spool_id, tray_info_idx, ams_id, tray_id
                        )
                    )
                    _lt.add_done_callback(self._on_task_done)

                    # Late-NFC reconfigure: AMS finished reading NFC chip after our
                    # configure call. On generic/empty → specific transition, re-push.
                    prev_nfc_slot = next(
                        (s for s in self._current_slots if s["slot_index"] == slot_index),
                        None,
                    )
                    prev_idx = (prev_nfc_slot.get("tray_info_idx") or "") if prev_nfc_slot else ""
                    if not prev_idx or prev_idx in _GENERIC_SLICER_ID_SET:
                        logger.info(
                            f"Late NFC read on slot {slot_index}: "
                            f"{prev_idx!r} → {tray_info_idx!r}, reconfiguring"
                        )
                        _t = asyncio.create_task(
                            self._reconfigure_slot_with_profile(ams_id, tray_id, tray_info_idx, tray)
                        )
                        _t.add_done_callback(self._on_task_done)

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

            # Pending spool matching for external/bypass tray — same logic as AMS trays
            if self._pending_spool_id is not None and vt_type:
                vt_tag_uid = (vt.get("tag_uid") or "").upper()
                prev_vt = next(
                    (s for s in self._current_slots if s["slot_index"] == vt_idx),
                    None,
                )
                vt_was_empty = prev_vt is None or not prev_vt.get("present", False)

                rfid_matched = bool(
                    vt_tag_uid
                    and self._pending_rfid_hex
                    and vt_tag_uid == self._pending_rfid_hex
                )
                slot_matched = vt_was_empty and not vt_tag_uid

                if rfid_matched or slot_matched:
                    reason = "rfid" if rfid_matched else "slot-appeared"
                    ams_id_ext, tray_id_ext = 255, vt_id
                    logger.info(
                        f"Pending spool {self._pending_spool_id} matched "
                        f"external tray {ams_id_ext}/{tray_id_ext} ({reason})"
                    )
                    self.send_filament_to_tray(
                        ams_id_ext, tray_id_ext, {**self._pending_filament_data}
                    )
                    self._clear_pending_peers()

            # Late-NFC reconfigure for external tray
            if (
                vt_type
                and vt_idx in self._slot_to_filaman_spool
                and vt_tray_info_idx
                and vt_tray_info_idx not in _GENERIC_SLICER_ID_SET
            ):
                prev_nfc_vt = next(
                    (s for s in self._current_slots if s["slot_index"] == vt_idx),
                    None,
                )
                prev_vt_idx = (prev_nfc_vt.get("tray_info_idx") or "") if prev_nfc_vt else ""
                if not prev_vt_idx or prev_vt_idx in _GENERIC_SLICER_ID_SET:
                    logger.info(
                        f"Late NFC read on external tray {vt_idx}: "
                        f"{prev_vt_idx!r} → {vt_tray_info_idx!r}, reconfiguring"
                    )
                    _t = asyncio.create_task(
                        self._reconfigure_slot_with_profile(255, vt_id, vt_tray_info_idx, vt)
                    )
                    _t.add_done_callback(self._on_task_done)

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
            "pending": self._pending_spool_id is not None,
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
