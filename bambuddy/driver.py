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
import math
import pathlib
import re
import time
from datetime import datetime, timezone
from typing import Any, Callable, ClassVar

import httpx
from sqlalchemy import delete, event, func, select
from sqlalchemy.orm import Session, selectinload

from app.core.database import async_session_maker
from app.models.app_settings import AppSettings
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

from .external_slots import canonical_external_tray_id, external_slot_index

from .profile_variants import (
    build_variant_groups_from_index,
    build_variant_index_from_presets,
    canonical_printer_model_token,
    coerce_profile_base_name,
    expected_cloud_preset_name,
    extract_profile_base_name,
    filter_grouped_presets_for_model,
    group_presets_by_base_name,
    infer_default_base_name,
    is_cloud_setting_id,
    parse_cloud_preset_name,
    resolve_cloud_variant_detailed,
    resolve_cloud_variant_from_index,
    uniform_variant_code,
    compute_profile_backfill_diff,
    extract_profile_overrides,
    is_override_profile_source,
    normalize_profiles_for_filament_copy,
)

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

# Bambu-brand "basic" profile per material. Used when the app setting
# ``bambu_unmatched_profile_fallback`` is "bambu": an unmatched filament falls
# back to Bambu's own material profile instead of the Generic one. Materials with
# no clean Bambu-brand basic (e.g. PA/NYLON, HIPS, PP) intentionally omitted so
# they keep falling back to the generic code.
_BAMBU_BRAND_SLICER_IDS: dict[str, str] = {
    "PLA": "GFA00",  # Bambu PLA Basic
    "PETG": "GFG00",  # Bambu PETG Basic
    "ABS": "GFB00",  # Bambu ABS
    "ASA": "GFB01",  # Bambu ASA
    "TPU": "GFU01",  # Bambu TPU 95A
    "PC": "GFC00",  # Bambu PC
    "PVA": "GFS04",  # Bambu PVA
}

# Reverse-Lookup: Anzeigename → Slicer-Code (z.B. "Generic PLA" → "GFL99")
# FilaMan-Dropdowns speichern den Anzeigenamen, nicht den Key aus bambu_filaments.json.
_FILAMENTS_FILE = pathlib.Path(__file__).parent / "bambu_filaments.json"
_FILAMENT_IDX_TO_NAME: dict[str, str] = {}  # "GFL99" → "Generic PLA"
_FILAMENT_NAME_TO_IDX: dict[str, str] = {}  # "Generic PLA" → "GFL99"
if _FILAMENTS_FILE.exists():
    _raw = json.loads(_FILAMENTS_FILE.read_text(encoding="utf-8"))
    _FILAMENT_IDX_TO_NAME = {k: v for k, v in _raw.items() if not k.startswith("_")}
    _FILAMENT_NAME_TO_IDX = {v: k for k, v in _FILAMENT_IDX_TO_NAME.items()}


def _is_cloud_setting_id(code: str | None) -> bool:
    """True for slicer setting_ids that must not be used as AMS tray_info_idx.

    Policy (ABS/ASA custom profiles, Studio Device tab):
      - Keep PFUS/PFCN as ``setting_id`` on the slot — never clear it when known.
      - Never store or send PFUS/PFCN as ``tray_info_idx`` / ``bambu_idx``.
      - AMS vendor codes (SUN*, GF*) are tray identity; PFUS is the slicer preset.
    """
    if not code:
        return False
    upper = code.upper()
    return upper.startswith(("PFUS", "PFCN"))


# Studio "Create Filament" wizard ids: "P" + 7 hex chars, optional variant
# suffix (e.g. "Pccd0d10", "Pccd0d10_06"). These are real tray_info_idx values
# (the firmware persists them and AMS sync matches the custom filament), unlike
# PFUS/PFCN setting_ids.
_CUSTOM_FILAMENT_ID_RE = re.compile(r"^P[0-9a-fA-F]{7}(?:_\d+)?$")


def _is_custom_filament_id(code: str | None) -> bool:
    """True for Studio custom-filament ids like ``Pccd0d10`` / ``Pccd0d10_06``."""
    return bool(code) and bool(_CUSTOM_FILAMENT_ID_RE.match(str(code).strip()))


def _ams_tray_code(code: str | None) -> str | None:
    """Return *code* only when it is a tray-capable AMS id (not PFUS/PFCN/empty)."""
    if not code:
        return None
    c = str(code).strip()
    if not c or _is_cloud_setting_id(c):
        return None
    return c


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
    # PFUS/PFCN are setting_ids, never tray AMS codes (printer rejects them).
    if _is_cloud_setting_id(raw_value):
        return _GENERIC_SLICER_IDS.get(material.upper(), "GFL99")
    # Bereits ein gültiger Slicer-Code?
    if raw_value in _FILAMENT_IDX_TO_NAME:
        return raw_value
    # Reverse-Lookup: Anzeigename → Code
    if raw_value in _FILAMENT_NAME_TO_IDX:
        return _FILAMENT_NAME_TO_IDX[raw_value]
    # Custom-Filament-IDs aus Studios "Create Filament" (z.B. "Pccd0d10") sind
    # gemischt-groß/klein und daher vom Uppercase-Check unten ausgeschlossen.
    if _is_custom_filament_id(raw_value):
        return raw_value.strip()
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


# Aliases for in-driver use (pure logic lives in profile_variants.py).
_extract_profile_base_name = extract_profile_base_name
_canonical_printer_model_token = canonical_printer_model_token
_parse_cloud_preset_name = parse_cloud_preset_name
_build_variant_index_from_presets = build_variant_index_from_presets
_build_variant_groups_from_index = build_variant_groups_from_index
_resolve_cloud_variant_from_index = resolve_cloud_variant_from_index
_resolve_cloud_variant_detailed = resolve_cloud_variant_detailed
_uniform_variant_code = uniform_variant_code
_group_presets_by_base_name = group_presets_by_base_name
_filter_grouped_presets_for_model = filter_grouped_presets_for_model
_expected_cloud_preset_name = expected_cloud_preset_name

_PROFILES_BY_MODEL_KEY = "bambu_profiles_by_model"


def _extract_bambu_idx(preset_id: str) -> str:
    """Extrahiert bambu_idx aus Bambuddy preset_id.

    Bambuddy liefert z.B. 'builtin_GFA01' → extrahiert 'GFA01' (Teil nach erstem '_').
    Enthält preset_id kein Unterstriche, wird der gesamte Wert zurückgegeben.
    Cloud setting_ids (PFUS/PFCN) are never returned as tray codes.
    """
    if not preset_id:
        return ""
    extracted = preset_id.split("_", 1)[1] if "_" in preset_id else preset_id
    if _is_cloud_setting_id(extracted) or _is_cloud_setting_id(preset_id):
        return ""
    return extracted


def _is_known_ams_slicer_code(code: str) -> bool:
    """True when *code* is already a tray-capable AMS/slicer identifier.

    Builtin GFxx codes (GFA00, GFC00, …) and vendor SUN… codes are valid AMS
    tray identifiers but are absent from the tiny cloud filament-id-map, so they
    must not trigger filament-info lookups on every sync.
    """
    if not code:
        return False
    if code in _FILAMENT_IDX_TO_NAME:
        return True
    if code in _GENERIC_SLICER_ID_SET:
        return True
    if code in _BAMBU_BRAND_SLICER_IDS.values():
        return True
    if _is_custom_filament_id(code):
        return True
    upper = code.upper()
    if len(upper) >= 4 and upper.isalnum() and upper == code.upper():
        if upper.startswith(("GF", "SUN")):
            return True
    return False


def _tray_code_from_builtin_setting(code: str) -> str | None:
    """Map nozzle-suffixed builtins like ``GFSB01_16`` → tray ``GFB01``."""
    if not code or _is_cloud_setting_id(code):
        return None
    family = str(code).split("_", 1)[0].strip().upper()
    candidates: list[str] = []
    # GFSB01 → GFB01, GFSL04 → GFL04 (drop the 'S' after GF).
    if family.startswith("GFS") and len(family) >= 5 and family[3].isalpha():
        candidates.append("GF" + family[3:])
    if family:
        candidates.append(family)
    for cand in candidates:
        if (
            cand in _FILAMENT_IDX_TO_NAME
            or cand in _GENERIC_SLICER_ID_SET
            or cand in _BAMBU_BRAND_SLICER_IDS.values()
            or _is_known_ams_slicer_code(cand)
        ):
            return cand
    return None


def _tray_code_from_profile_base_name(base_name: str) -> str | None:
    """Best-effort tray code from a profile base name (e.g. ``Overture ASA``)."""
    if not base_name:
        return None
    upper = base_name.strip().upper()
    # Prefer longer material tokens first (ASA-CF before ASA).
    materials = sorted(_BAMBU_BRAND_SLICER_IDS.keys(), key=len, reverse=True)
    for mat in materials:
        if upper == mat or upper.endswith(" " + mat) or upper.endswith("-" + mat):
            return _BAMBU_BRAND_SLICER_IDS[mat]
    return None


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
    # Differential-sync cache: last payload successfully sent to Bambuddy per spool.
    # Keyed by bambuddy_url → {fm_spool_id → payload_dict}. Skips PATCH when unchanged.
    _url_last_payloads: ClassVar[dict[str, dict[int, dict]]] = {}
    # After any debounce-triggered sync, block further debounce syncs for this
    # long. Longer than the WS reconnect interval (~5-10 min) so each reconnect
    # burst of inventory_changed events only triggers one sync at most.
    # User-initiated changes (schedule_sync / _on_session_commit) bypass this.
    _SYNC_COOLDOWN: ClassVar[float] = 60.0  # Sekunden

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
        self._printer_write_mode = config.get("printer_write_mode", "full")
        if self._printer_write_mode not in {"full", "inventory_only"}:
            self._printer_write_mode = "full"
        self._apply_to_printer = self._printer_write_mode == "full"
        # Feature flag: per-model slicer-profile variants (PFUS) + setting_id on
        # AMS configure. Accepts per_model_profiles or legacy per_printer_profiles.
        _profile_flag = config.get("per_model_profiles") or config.get(
            "per_printer_profiles", "enabled"
        )
        self._per_printer_profiles: bool = _profile_flag != "disabled"
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
        # Cache für die globale "unmatched profile fallback"-Einstellung
        self._unmatched_fallback_cache: str | None = None
        self._unmatched_fallback_ts: float = 0.0
        # Slot-Key ("ams_id-tray_id") → FilaMan-Spool-ID (für Verbrauchsmeldungen)
        self._slot_to_filaman_spool: dict[str, int] = {}
        # Sticky AMS: throttle reassert POSTs after empty trays / unscanned reinserts.
        self._sticky_reassert_ts: dict[str, float] = {}
        self._STICKY_REASSERT_COOLDOWN: float = 5.0
        self._sticky_reconcile_task: asyncio.Task | None = None
        # In-flight sticky reassert tasks (cancelled when a newer assign wins the slot).
        self._sticky_tasks: dict[str, asyncio.Task] = {}
        # Per-slot generation: bumped on intentional assign/configure so a late
        # sticky MQTT for the previous occupant cannot overwrite the new one.
        self._slot_configure_gen: dict[str, int] = {}
        # Brief settle window so empty→reinsert / pending assign can bump gen
        # before a sticky reassert POSTs the old spool to Bambuddy.
        self._STICKY_REASSERT_SETTLE: float = 0.75
        # In-flight learn dedup: two MQTT events in the same second spawned two
        # identical learns whose concurrent INSERTs hit the UNIQUE constraint.
        self._learn_inflight: set[tuple[int, str]] = set()
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
        self._pending_slot_snapshot: dict[str, dict[str, Any]] | None = None
        self._pending_timer: asyncio.Task | None = None
        self._pending_poll_task: asyncio.Task | None = None

        # -- Task Restart Management --
        self._ws_restart_count: int = 0
        self._sync_restart_count: int = 0
        self._last_ws_restart: float = 0.0
        self._last_sync_restart: float = 0.0
        self._max_restart_attempts: int = 5
        self._restart_backoff_base: float = 2.0  # Exponential backoff base
        # In-loop WebSocket reconnect backoff (separate from the task watchdog
        # above, which never fires because _ws_loop catches its own errors and
        # reconnects internally). Counts consecutive short-lived connections so
        # a brief Bambuddy hiccup recovers in ~1s while a real outage backs off.
        self._ws_reconnect_attempt: int = 0
        # Timestamp of the last successful WS connect. Used to suppress
        # inventory_changed sync triggers for a grace period after reconnect
        # (the startup fetch already pushed current state; hammering Bambuddy
        # with a full 50-spool PATCH storm on every reconnect freezes its UI).
        self._ws_last_connected_at: float = 0.0

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
        # None = definitive miss (name found, not in id-map); expires after miss TTL.
        self._cloud_preset_cache: dict[str, str | None] = {}
        self._cloud_preset_miss_ts: dict[str, float] = {}
        self._cloud_preset_miss_ttl: float = 86400.0  # 24h — retry definitive misses

        # -- Cloud-Profil-Picker (Option A) --
        # Gemergte Preset-Liste (cloud/filaments + builtin) für die FilaMan-UI.
        # code → {code, name, displayName, isCustom}; gecached mit TTL.
        self._cloud_presets: list[dict[str, Any]] = []
        self._cloud_presets_by_code: dict[str, dict[str, Any]] = {}
        self._cloud_presets_ts: float = 0.0
        self._cloud_presets_ttl: float = 600.0  # 10min Cache
        # Pre-indexed (base, model, nozzle) -> PFUS for per-model variant lookup.
        self._variant_index: dict[tuple[str, str, float | None], str] = {}
        self._variant_groups: dict[
            tuple[str, str], list[tuple[float | None, str]]
        ] = {}
        self._variant_index_ts: float = 0.0
        self._printer_context_cache: dict[int, dict[str, Any]] = {}
        # FilaMan-Spool-ID → monotonic ts der letzten lokalen Profiländerung
        # (für Last-Writer-Wins beim Bambuddy→FilaMan-Reflect).
        self._local_profile_writes: dict[int, float] = {}
        # Nozzle-change auto-reconfigure state
        self._last_nozzle_context: dict[str, Any] | None = None
        self._reconfigure_task: asyncio.Task | None = None
        self._pending_reconfigure_after_print: bool = False
        self._last_print_state: str = ""

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

        # Always register for URL peer discovery. Per-model profile UI
        # (connected-models / profile-coverage) needs every Bambuddy driver on
        # this URL, even when inventory sync is disabled.
        self._register()

        # Heal PFUS wrongly stored as bambu_idx (pre-hardening / reflect bugs).
        if self._is_sync_coordinator():
            _heal = asyncio.create_task(self._sanitize_pfus_stored_as_bambu_idx())
            _heal.add_done_callback(self._on_task_done)

        # Initialen AMS-Status laden
        await self._fetch_and_emit_status()

        if self._sync_enabled:
            # Inventory-Sync: FilaMan → Bambuddy (URL-Lock verhindert Duplikate)
            await self._sync_all_spools()
            # Slot-Cache aus Bambuddy-Assignments wiederherstellen (überlebt Neustarts)
            await self._restore_slot_cache_from_assignments()
            # Drop sticky owners whose FilaMan location was cleared while we were down.
            await self._reconcile_sticky_slot_map()
            # Re-POST any FilaMan AMS locations missing from Bambuddy (recovery after
            # Bambuddy fingerprint auto-unlink / Spoolman mode wipes).
            await self._backfill_missing_ams_assignments()
            if self._per_printer_profiles and self._is_sync_coordinator():
                _lf = asyncio.create_task(self._legacy_fanout_mirrored_profiles())
                _lf.add_done_callback(self._on_task_done)

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
        for slot_key in list(self._sticky_tasks.keys()):
            self._cancel_sticky_task(slot_key)
        for task in (
            self._ws_task,
            self._sync_task,
            self._debounce_task,
            self._sticky_reconcile_task,
        ):
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        self._ws_task = self._sync_task = None
        self._debounce_task = self._sticky_reconcile_task = None
        self._ws_connected = False
        self._printer_connected = False
        self._clear_pending()
        if self._client:
            await self._client.aclose()
            self._client = None

        # Always unregister from URL peer map (registered regardless of sync).
        self._unregister()
        if self._sync_enabled and was_coordinator:
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

        Every driver reconciles sticky slot ownership so a manual location clear
        drops in-memory sticky. Only the sync-coordinator for this URL triggers
        the inventory debounce sync.
        """
        if not self._running or not self._sync_enabled:
            return

        # Sticky map reconcile (all peer drivers — not only the coordinator).
        if self._sticky_reconcile_task and not self._sticky_reconcile_task.done():
            self._sticky_reconcile_task.cancel()
        self._sticky_reconcile_task = asyncio.create_task(
            self._debounced_sticky_reconcile()
        )
        self._sticky_reconcile_task.add_done_callback(self._on_task_done)

        if (
            not self._ws_connected
            or self._syncing
            or not self._is_sync_coordinator()
        ):
            return
        if self._debounce_task and not self._debounce_task.done():
            self._debounce_task.cancel()
        self._debounce_task = asyncio.create_task(self._debounced_sync())
        self._debounce_task.add_done_callback(self._on_task_done)

    async def _debounced_sticky_reconcile(self) -> None:
        """Wait briefly for commit bursts, then drop stale sticky owners."""
        await asyncio.sleep(1.0)
        if self._running and self._sync_enabled:
            await self._reconcile_sticky_slot_map()

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
                new_forward = dict(idmap)
                new_reverse = {name: code for code, name in idmap.items()}
                # Drop preset resolve cache when the id-map actually changes so
                # previously unresolvable PFUS… codes can retry against new entries.
                if new_forward != self._cloud_idmap_forward:
                    self._cloud_preset_cache.clear()
                    self._cloud_preset_miss_ts.clear()
                self._cloud_idmap_reverse = new_reverse
                self._cloud_idmap_forward = new_forward
                self._cloud_idmap_ts = now
        except Exception as e:
            logger.debug(f"Could not load cloud filament-id-map: {e}")
        return self._cloud_idmap_reverse

    def _cache_cloud_preset_result(
        self, preset_id: str, code: str | None
    ) -> str | None:
        """Cache a resolve result (including TTL'd negative None) and return it."""
        self._cloud_preset_cache[preset_id] = code
        if code is None:
            self._cloud_preset_miss_ts[preset_id] = time.monotonic()
        else:
            self._cloud_preset_miss_ts.pop(preset_id, None)
        return code

    def _get_cached_cloud_preset(self, preset_id: str) -> tuple[bool, str | None]:
        """Return (hit, code). Negative hits expire after ``_cloud_preset_miss_ttl``."""
        if preset_id not in self._cloud_preset_cache:
            return False, None
        code = self._cloud_preset_cache[preset_id]
        if code is not None:
            return True, code
        miss_ts = self._cloud_preset_miss_ts.get(preset_id)
        if miss_ts is None or (
            time.monotonic() - miss_ts
        ) >= self._cloud_preset_miss_ttl:
            self._cloud_preset_cache.pop(preset_id, None)
            self._cloud_preset_miss_ts.pop(preset_id, None)
            return False, None
        return True, None

    async def _resolve_cloud_preset(self, preset_id: str) -> str | None:
        """Löst einen Bambu-Cloud-Preset (z.B. "PFUS…") zum AMS-Code auf.

        Ablauf:
          1. Ist es bereits ein bekannter AMS-Code (id-map, builtin GFxx/SUN…)? → direkt.
          2. filament-info(preset_id) → Anzeigename (z.B. "SUNLU PLA PLUS GEN2
             @Bambu Lab P2S 0.4 nozzle"); "@…"-Suffix entfernen → Basisname.
          3. Reverse-Lookup in der id-map: Basisname → AMS-Code (z.B. "SUN20013").

        Positive Treffer bleiben gecached bis die id-map sich ändert. Definitive
        Misses (Name gefunden, nicht in id-map) werden 24h negativ gecached.
        Transient failures werden nicht gecached.

        Never returns a PFUS/PFCN — those are setting_ids, not tray codes.
        """
        if not preset_id:
            return None
        if preset_id.startswith("builtin_"):
            preset_id = _extract_bambu_idx(preset_id)
        hit, cached = self._get_cached_cloud_preset(preset_id)
        if hit:
            return _ams_tray_code(cached)

        reverse = await self._get_cloud_idmap_reverse()
        # Cache may have been cleared by an id-map reload above.
        hit, cached = self._get_cached_cloud_preset(preset_id)
        if hit:
            return _ams_tray_code(cached)
        # 1. Schon ein gültiger AMS-Code? (never treat PFUS/PFCN as tray codes)
        if not _is_cloud_setting_id(preset_id) and (
            preset_id in self._cloud_idmap_forward
            or _is_known_ams_slicer_code(preset_id)
        ):
            return self._cache_cloud_preset_result(preset_id, preset_id)

        # 2. + 3. filament-info → Name → Basisname → reverse-Lookup
        # 4. Fallbacks for custom ABS/ASA (and similar) presets that have no
        #    vendor AMS code in the id-map: follow filament_id/base_id when the
        #    Bambuddy payload includes them, else derive GFB01 from GFSB01_xx /
        #    the material token in the display name.
        try:
            info = await self._bb_post("/api/v1/cloud/filament-info", [preset_id])
        except Exception as e:
            logger.debug(f"cloud/filament-info failed for {preset_id}: {e}")
            # Transient failure — do not negative-cache; retry on next sync.
            return None
        if not isinstance(info, dict):
            return None
        entry = info.get(preset_id) or {}
        if not isinstance(entry, dict):
            entry = {}
        name = (entry.get("name") or "").strip()
        filament_id = (entry.get("filament_id") or "").strip()
        base_id = (entry.get("base_id") or "").strip()
        if not name and not filament_id and not base_id:
            # Empty/missing payload may be transient; allow retry.
            return None
        if name:
            base_name = name.split(" @", 1)[0].strip()
            code = reverse.get(base_name) or reverse.get(name)
            if code:
                logger.info(
                    f"Resolved Bambu cloud preset {preset_id!r} "
                    f"({base_name!r}) → AMS code {code!r}"
                )
                return self._cache_cloud_preset_result(preset_id, _ams_tray_code(code))
        if filament_id and not _is_cloud_setting_id(filament_id):
            tray = _ams_tray_code(filament_id) or filament_id
            if tray and (
                tray in self._cloud_idmap_forward
                or _is_known_ams_slicer_code(tray)
                or tray in _FILAMENT_IDX_TO_NAME
                or tray in _GENERIC_SLICER_ID_SET
            ):
                logger.info(
                    f"Resolved Bambu cloud preset {preset_id!r} "
                    f"via filament_id → AMS code {tray!r}"
                )
                return self._cache_cloud_preset_result(preset_id, tray)
        if base_id and base_id != preset_id:
            nested = await self._resolve_cloud_preset(base_id)
            if nested:
                logger.info(
                    f"Resolved Bambu cloud preset {preset_id!r} "
                    f"via base_id {base_id!r} → AMS code {nested!r}"
                )
                return self._cache_cloud_preset_result(preset_id, nested)
        # Builtin nozzle variants (GFSB01_16) encode the family before '_'.
        if not _is_cloud_setting_id(preset_id):
            derived = _tray_code_from_builtin_setting(preset_id)
            if derived:
                logger.info(
                    f"Resolved Bambu cloud preset {preset_id!r} "
                    f"via builtin family → AMS code {derived!r}"
                )
                return self._cache_cloud_preset_result(preset_id, derived)
        # Custom PFUS with no vendor AMS code (Overture/Sunlu ASA @ …): use the
        # Bambu-brand tray for the material token so Studio keeps a valid
        # tray_info_idx while setting_id carries the PFUS name.
        if name:
            base_name = name.split(" @", 1)[0].strip()
            material_tray = _tray_code_from_profile_base_name(base_name)
            if material_tray:
                logger.info(
                    f"Resolved Bambu cloud preset {preset_id!r} "
                    f"({base_name!r}) → AMS code {material_tray!r} "
                    f"(material fallback)"
                )
                return self._cache_cloud_preset_result(preset_id, material_tray)
        logger.debug(
            f"Cloud preset {preset_id!r} name "
            f"{(name.split(' @', 1)[0].strip() if name else '')!r} "
            f"not found in id-map (filament_id={filament_id!r}, base_id={base_id!r})"
        )
        return self._cache_cloud_preset_result(preset_id, None)

    async def _resolved_ams_tray_code(self, code: str | None) -> str | None:
        """Resolve a preset or AMS code to a tray-capable id (never PFUS/PFCN)."""
        if not code:
            return None
        if not _is_cloud_setting_id(code):
            direct = _ams_tray_code(code)
            if direct and (
                direct in self._cloud_idmap_forward
                or _is_known_ams_slicer_code(direct)
                or direct in _FILAMENT_IDX_TO_NAME
                or direct in _GENERIC_SLICER_ID_SET
            ):
                return direct
        return _ams_tray_code(await self._resolve_cloud_preset(str(code)))

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
            logger.warning(f"Could not load cloud/builtin-filaments: {e}")

        # 2. Cloud-Presets (setting_id), inkl. Drucker-/Düsen-Varianten
        skipped_no_id = 0
        try:
            cloud = await self._bb_get("/api/v1/cloud/filaments")
            if isinstance(cloud, list):
                for c in cloud:
                    code = (c.get("setting_id") or "").strip()
                    name = (c.get("name") or "").strip()
                    if not code:
                        skipped_no_id += 1
                        continue
                    if code in seen:
                        continue
                    seen.add(code)
                    is_custom = bool(c.get("is_custom"))
                    display = f"{name} (Custom)" if is_custom else name
                    setting = c.get("setting") if isinstance(c.get("setting"), dict) else None
                    compat = c.get("compatible_printers")
                    if not isinstance(compat, list) and setting:
                        compat = setting.get("compatible_printers")
                    entry: dict[str, Any] = {
                        "code": code,
                        "name": name,
                        "displayName": display,
                        "isCustom": is_custom,
                    }
                    if setting:
                        entry["setting"] = setting
                    if isinstance(compat, list):
                        entry["compatible_printers"] = compat
                    merged.append(entry)
        except Exception as e:
            logger.warning(f"Could not load cloud/filaments: {e}")

        if skipped_no_id:
            logger.warning(
                f"Cloud preset catalog: skipped {skipped_no_id} entr(y/ies) from "
                f"Bambuddy /api/v1/cloud/filaments without a setting_id — those "
                f"cannot appear in the slicer profile picker."
            )
        if not merged:
            logger.warning(
                "Cloud preset catalog is empty — Bambuddy returned no usable "
                "presets (check Bambuddy cloud sync / API auth). The slicer "
                "profile picker will show 'no presets found'."
            )
        if merged:
            self._cloud_presets = merged
            self._cloud_presets_by_code = {p["code"]: p for p in merged}
            self._cloud_presets_ts = now
            self._variant_index = _build_variant_index_from_presets(merged)
            self._variant_groups = _build_variant_groups_from_index(self._variant_index)
            self._variant_index_ts = now
            n_custom = sum(1 for p in merged if p.get("isCustom"))
            n_pfus = sum(
                1 for p in merged if str(p.get("code") or "").upper().startswith("PFUS")
            )
            logger.info(
                f"Cloud preset catalog loaded: {len(merged)} total, "
                f"{n_custom} custom, {n_pfus} PFUS "
                f"(from Bambuddy {self._bambuddy_url})"
            )
        return self._cloud_presets

    async def _ensure_variant_index(
        self,
    ) -> dict[tuple[str, str, float | None], str]:
        """Return the pre-indexed cloud variant map (rebuild when preset cache refreshes)."""
        await self._load_cloud_presets()
        if self._variant_index_ts != self._cloud_presets_ts:
            self._variant_index = _build_variant_index_from_presets(self._cloud_presets)
            self._variant_groups = _build_variant_groups_from_index(self._variant_index)
            self._variant_index_ts = self._cloud_presets_ts
        return self._variant_index

    async def _ensure_variant_groups(
        self,
    ) -> dict[tuple[str, str], list[tuple[float | None, str]]]:
        await self._ensure_variant_index()
        return self._variant_groups

    def _merge_printer_context_nozzle(
        self, bambuddy_printer_id: int | None, nozzle_mm: float | None
    ) -> None:
        """Merge live nozzle from WebSocket into the context cache."""
        if bambuddy_printer_id is None or nozzle_mm is None:
            return
        cached = self._printer_context_cache.get(bambuddy_printer_id)
        if cached is None:
            return
        if cached.get("nozzle_mm") != nozzle_mm:
            cached["nozzle_mm"] = nozzle_mm

    def _invalidate_printer_context(self, bambuddy_printer_id: int | None) -> None:
        if bambuddy_printer_id is not None:
            self._printer_context_cache.pop(bambuddy_printer_id, None)

    async def _model_printer_map(self) -> dict[str, list[int]]:
        """Map canonical model token → FilaMan printer_ids on this Bambuddy URL."""
        by_model: dict[str, list[int]] = {}
        for driver in self._peer_drivers():
            ctx = await driver._get_bambuddy_printer_context()
            model = (ctx.get("model") or "").strip().upper()
            if not model:
                continue
            by_model.setdefault(model, []).append(driver.printer_id)
        return by_model

    async def list_connected_models(self) -> dict[str, Any]:
        """Distinct printer model types with all printer_ids for each model."""
        by_model = await self._model_printer_map()
        models: list[dict[str, Any]] = []
        for model, printer_ids in sorted(by_model.items()):
            models.append(
                {
                    "model": model,
                    "printer_ids": printer_ids,
                    "representative_printer_id": printer_ids[0],
                }
            )
        return {"models": models, "count": len(models)}

    @staticmethod
    def _normalize_profiles_by_model(raw: Any) -> dict[str, dict[str, str]]:
        if not isinstance(raw, dict):
            return {}
        out: dict[str, dict[str, str]] = {}
        for model, entry in raw.items():
            if not model or not isinstance(entry, dict):
                continue
            base = (entry.get("base_name") or "").strip()
            if not base:
                continue
            source = (entry.get("source") or "manual").strip()
            out[str(model).upper()] = {"base_name": base, "source": source}
        return out

    async def _read_spool_profiles_by_model(
        self, spool_id: int
    ) -> dict[str, dict[str, str]]:
        async with async_session_maker() as db:
            spool = await db.get(Spool, spool_id)
            if not spool:
                return {}
            cf = dict(spool.custom_fields or {})
            return self._normalize_profiles_by_model(
                cf.get(_PROFILES_BY_MODEL_KEY)
            )

    async def _write_spool_profiles_by_model(
        self, spool_id: int, profiles: dict[str, dict[str, str]]
    ) -> None:
        if not spool_id:
            return
        async with async_session_maker() as db:
            spool = await db.get(Spool, spool_id)
            if spool is None:
                return
            cf = dict(spool.custom_fields or {})
            if profiles:
                cf[_PROFILES_BY_MODEL_KEY] = profiles
            elif _PROFILES_BY_MODEL_KEY in cf:
                del cf[_PROFILES_BY_MODEL_KEY]
            spool.custom_fields = cf
            await db.commit()

    async def _read_filament_profiles_by_model(
        self, filament_id: int
    ) -> dict[str, dict[str, str]]:
        async with async_session_maker() as db:
            filament = await db.get(Filament, filament_id)
            if not filament:
                return {}
            cf = dict(filament.custom_fields or {})
            return self._normalize_profiles_by_model(
                cf.get(_PROFILES_BY_MODEL_KEY)
            )

    async def _write_filament_profiles_by_model(
        self, filament_id: int, profiles: dict[str, dict[str, str]]
    ) -> None:
        if not filament_id:
            return
        async with async_session_maker() as db:
            filament = await db.get(Filament, filament_id)
            if filament is None:
                return
            cf = dict(filament.custom_fields or {})
            if profiles:
                cf[_PROFILES_BY_MODEL_KEY] = profiles
            elif _PROFILES_BY_MODEL_KEY in cf:
                del cf[_PROFILES_BY_MODEL_KEY]
            filament.custom_fields = cf
            await db.commit()

    async def _get_unmatched_profile_fallback(self) -> str:
        """Global setting for unmatched filaments: "generic" or "bambu".

        Cached briefly so the hot assignment path doesn't hit the DB every time.
        """
        now = time.monotonic()
        if (
            self._unmatched_fallback_cache is not None
            and (now - self._unmatched_fallback_ts) < 60.0
        ):
            return self._unmatched_fallback_cache
        value = "generic"
        try:
            async with async_session_maker() as db:
                row = await db.get(AppSettings, 1)
                if row is not None:
                    candidate = (row.bambu_unmatched_profile_fallback or "").strip().lower()
                    if candidate in ("generic", "bambu"):
                        value = candidate
        except Exception as e:
            logger.debug(f"Could not read unmatched profile fallback setting: {e}")
        self._unmatched_fallback_cache = value
        self._unmatched_fallback_ts = now
        return value

    async def _read_spool_default_base_name(self, spool_id: int) -> str:
        async with async_session_maker() as db:
            spool = await db.get(Spool, spool_id)
            if not spool:
                return ""
            return str((spool.custom_fields or {}).get("bambu_profile_base_name") or "").strip()

    async def _read_filament_default_base_name(self, filament_id: int) -> str:
        async with async_session_maker() as db:
            filament = await db.get(Filament, filament_id)
            if not filament:
                return ""
            return str(
                (filament.custom_fields or {}).get("bambu_profile_base_name") or ""
            ).strip()

    async def _has_stored_cloud_setting_id(
        self, *, spool_id: int | None = None, filament_id: int | None = None
    ) -> bool:
        """True when a PFUS/PFCN is stored but may not have a human display name yet."""
        try:
            async with async_session_maker() as db:
                if spool_id:
                    spool = await db.get(Spool, int(spool_id))
                    cf = dict((spool.custom_fields or {}) if spool else {})
                    if is_cloud_setting_id(cf.get("bambu_slicer_filament")):
                        return True
                    res = await db.execute(
                        select(SpoolPrinterParam.param_value).where(
                            SpoolPrinterParam.spool_id == int(spool_id),
                            SpoolPrinterParam.param_key == "bambu_slicer_setting_id",
                        )
                    )
                    if any(is_cloud_setting_id(v) for v in res.scalars().all()):
                        return True
                if filament_id:
                    filament = await db.get(Filament, int(filament_id))
                    cf = dict((filament.custom_fields or {}) if filament else {})
                    if is_cloud_setting_id(cf.get("bambu_slicer_filament")):
                        return True
                    res = await db.execute(
                        select(FilamentPrinterParam.param_value).where(
                            FilamentPrinterParam.filament_id == int(filament_id),
                            FilamentPrinterParam.param_key == "bambu_slicer_setting_id",
                        )
                    )
                    if any(is_cloud_setting_id(v) for v in res.scalars().all()):
                        return True
        except Exception as e:
            logger.debug(f"Could not check stored cloud setting id: {e}")
        return False

    @staticmethod
    def _infer_default_base_name(
        profiles: dict[str, dict[str, str]], stored_default: str = ""
    ) -> str:
        return infer_default_base_name(profiles, stored_default)

    async def _link_default_to_models(
        self,
        profiles: dict[str, dict[str, str]],
        base_name: str,
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
    ) -> dict[str, dict[str, str]]:
        """Apply ``base_name`` to every connected model unless that model is overridden."""
        by_model = await self._model_printer_map()
        for model in by_model:
            if profiles.get(model, {}).get("source") == "override":
                continue
            profiles[model] = {"base_name": base_name, "source": "linked"}
        return profiles

    async def _nozzle_for_printer(
        self,
        printer_id: int,
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
    ) -> float | None:
        """Live or stored default nozzle for one FilaMan printer_id on this URL."""
        for driver in self._peer_drivers():
            if driver.printer_id != printer_id:
                continue
            ctx = await driver._get_bambuddy_printer_context()
            if ctx.get("nozzle_mm") is not None:
                return ctx.get("nozzle_mm")
            return await self._get_default_nozzle_mm(
                printer_id, spool_id=spool_id, filament_id=filament_id
            )
        return await self._get_default_nozzle_mm(
            printer_id, spool_id=spool_id, filament_id=filament_id
        )

    async def _nozzle_for_model(
        self,
        model: str,
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
    ) -> float | None:
        by_model = await self._model_printer_map()
        printer_ids = by_model.get(model.upper(), [])
        for pid in printer_ids:
            nozzle = await self._nozzle_for_printer(
                pid, spool_id=spool_id, filament_id=filament_id
            )
            if nozzle is not None:
                return nozzle
        return None

    async def _resolve_model_variant_detail(
        self,
        base_name: str,
        model: str,
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
        nozzle_mm: float | None = None,
    ) -> dict[str, Any]:
        index = await self._ensure_variant_index()
        groups = await self._ensure_variant_groups()
        if nozzle_mm is None:
            nozzle_mm = await self._nozzle_for_model(
                model, spool_id=spool_id, filament_id=filament_id
            )
        if nozzle_mm is None:
            nozzle_mm = 0.4
        detail = _resolve_cloud_variant_detailed(
            index, groups, base_name, model, nozzle_mm
        )
        entry: dict[str, Any] = {
            "model": model.upper(),
            "base_name": base_name,
            "nozzle_requested": detail.get("nozzle_requested"),
            "nozzle_resolved": detail.get("nozzle_resolved"),
            "mapped": bool(detail.get("mapped")),
            "code": detail.get("code"),
            "exact_nozzle": detail.get("exact_nozzle"),
            "fallback_nozzle": detail.get("fallback_nozzle"),
            "standard_nozzles": detail.get("standard_nozzles") or {},
            "requested_nozzle_in_cloud": bool(
                detail.get("requested_nozzle_in_cloud")
            ),
        }
        if not entry["mapped"]:
            entry["expected_name"] = _expected_cloud_preset_name(
                base_name, model, nozzle_mm
            )
            entry["status"] = "missing"
        elif entry.get("fallback_nozzle"):
            entry["status"] = "fallback"
        else:
            entry["status"] = "ok"
        return entry

    async def _mirror_pfus_by_model(
        self,
        variants_by_model: dict[str, str],
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
        spool: bool = True,
    ) -> bool:
        """Write resolved PFUS to every printer_id in each model group."""
        if not variants_by_model:
            return False
        by_model = await self._model_printer_map()
        mapping: dict[int, str] = {}
        for model, pfus in variants_by_model.items():
            if not pfus:
                continue
            for pid in by_model.get(model.upper(), []):
                mapping[pid] = pfus
        if not mapping:
            return False
        if spool and spool_id:
            return await self._upsert_spool_bambu_slicer_setting_id(
                int(spool_id), mapping
            )
        if filament_id:
            async with async_session_maker() as db:
                changed = await self._upsert_filament_bambu_slicer_setting_id(
                    db, int(filament_id), mapping
                )
                if changed:
                    await db.commit()
                return changed
        return False

    async def _resolve_and_mirror_profiles(
        self,
        profiles_by_model: dict[str, dict[str, str]],
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
        spool: bool = True,
    ) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
        """Resolve PFUS per model and mirror to printer params."""
        variants: dict[str, str] = {}
        coverage: dict[str, dict[str, Any]] = {}
        for model, entry in profiles_by_model.items():
            base = entry.get("base_name") or ""
            if not base:
                continue
            detail = await self._resolve_model_variant_detail(
                base,
                model,
                spool_id=spool_id,
                filament_id=filament_id,
            )
            detail["source"] = entry.get("source") or "manual"
            coverage[model.upper()] = detail
            if detail.get("code"):
                variants[model.upper()] = str(detail["code"])
        if variants:
            await self._mirror_pfus_by_model(
                variants,
                spool_id=spool_id,
                filament_id=filament_id,
                spool=spool,
            )
        # Clear stale per-printer PFUS/AMS params for any model that has a base name
        # but resolved to NO cloud variant. Without this, switching to a profile that
        # lacks a variant for one model would leave that model's printer silently
        # using the previously resolved (now wrong) profile at assign time.
        unmapped_models = [
            model.upper()
            for model, entry in profiles_by_model.items()
            if (entry.get("base_name") or "") and model.upper() not in variants
        ]
        if unmapped_models:
            await self._clear_profile_params_for_models(
                unmapped_models,
                spool_id=spool_id,
                filament_id=filament_id,
                spool=spool,
            )
        return variants, coverage

    async def _clear_profile_params_for_models(
        self,
        models: list[str],
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
        spool: bool = True,
    ) -> None:
        """Delete resolved PFUS/AMS params for printers whose model lost its variant.

        Prevents a stale per-printer ``bambu_slicer_setting_id`` / ``bambu_idx`` from a
        previous profile being silently sent at assign time when the newly selected
        profile has no cloud variant for that printer's model.
        """
        if not models:
            return
        by_model = await self._model_printer_map()
        peers = set(self._peer_printer_ids())
        pids: list[int] = []
        for m in models:
            for pid in by_model.get(m.upper(), []):
                if pid in peers:
                    pids.append(pid)
        if not pids:
            return
        keys = ["bambu_slicer_setting_id", "bambu_idx"]
        async with async_session_maker() as db:
            if spool and spool_id:
                await db.execute(
                    delete(SpoolPrinterParam).where(
                        SpoolPrinterParam.spool_id == int(spool_id),
                        SpoolPrinterParam.printer_id.in_(pids),
                        SpoolPrinterParam.param_key.in_(keys),
                    )
                )
                await db.commit()
            elif filament_id:
                await db.execute(
                    delete(FilamentPrinterParam).where(
                        FilamentPrinterParam.filament_id == int(filament_id),
                        FilamentPrinterParam.printer_id.in_(pids),
                        FilamentPrinterParam.param_key.in_(keys),
                    )
                )
                await db.commit()

    async def _scrub_unmapped_profiles(
        self,
        profiles: dict[str, dict[str, str]],
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
    ) -> bool:
        """Drop stored rows that do not resolve to a cloud variant for that model.

        Override and manual rows are never deleted — they were explicitly set by the
        user and should survive stale/offline cloud cache.  Only auto-populated rows
        (linked, legacy, reflect) are candidates for removal.
        """
        default_base = ""
        if spool_id:
            default_base = await self._read_spool_default_base_name(int(spool_id))
        elif filament_id:
            default_base = await self._read_filament_default_base_name(int(filament_id))
        changed = False
        for model, entry in list(profiles.items()):
            base = entry.get("base_name") or ""
            source = (entry.get("source") or "").lower()
            if not base:
                del profiles[model.upper()]
                changed = True
                continue
            # User-explicit rows are never auto-deleted regardless of cloud state.
            if source in ("override", "manual"):
                continue
            # Linked row that still matches the current default is always kept.
            if source == "linked" and default_base and base == default_base:
                continue
            detail = await self._resolve_model_variant_detail(
                base, model, spool_id=spool_id, filament_id=filament_id
            )
            if not detail.get("mapped"):
                del profiles[model.upper()]
                changed = True
        return changed

    async def _scrub_unmapped_auto_profiles(
        self,
        profiles: dict[str, dict[str, str]],
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
    ) -> bool:
        """Drop linked/reflect/auto rows that have no cloud variant for that model."""
        return await self._scrub_unmapped_profiles(
            profiles,
            spool_id=spool_id,
            filament_id=filament_id,
        )

    async def _resolve_model_coverage_map(
        self,
        profiles_by_model: dict[str, dict[str, str]],
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
        default_base: str = "",
    ) -> dict[str, dict[str, Any]]:
        """Read-only coverage for every connected model."""
        if not default_base:
            if spool_id:
                default_base = await self._read_spool_default_base_name(int(spool_id))
            elif filament_id:
                default_base = await self._read_filament_default_base_name(
                    int(filament_id)
                )
        by_model = await self._model_printer_map()
        coverage: dict[str, dict[str, Any]] = {}
        for model in sorted(by_model.keys()):
            entry = profiles_by_model.get(model, {})
            source = (entry.get("source") or "").lower()
            if source == "override":
                base = entry.get("base_name") or ""
            else:
                base = entry.get("base_name") or default_base
            if not base:
                coverage[model] = {
                    "model": model,
                    "base_name": "",
                    "mapped": False,
                    "status": "not_set",
                    "nozzle_requested": await self._nozzle_for_model(
                        model, spool_id=spool_id, filament_id=filament_id
                    ),
                    "standard_nozzles": {
                        f"{s:g}": False for s in (0.2, 0.4, 0.6, 0.8)
                    },
                    "requested_nozzle_in_cloud": False,
                }
                continue
            detail = await self._resolve_model_variant_detail(
                base,
                model,
                spool_id=spool_id,
                filament_id=filament_id,
            )
            detail["source"] = entry.get("source") or (
                "linked" if base == default_base else "manual"
            )
            coverage[model] = detail
        return coverage

    async def get_profile_coverage(
        self,
        spool_id: int | None = None,
        filament_id: int | None = None,
    ) -> dict[str, Any]:
        """Read-only per-model PFUS resolution + coverage for UI."""
        if not spool_id and not filament_id:
            raise ValueError("spool_id or filament_id is required")
        profiles: dict[str, dict[str, str]] = {}
        base_name = ""
        if spool_id:
            async with async_session_maker() as db:
                spool = await db.get(Spool, int(spool_id))
                if not spool:
                    raise ValueError(f"Spool {spool_id} not found")
                cf = dict(spool.custom_fields or {})
                profiles = self._normalize_profiles_by_model(
                    cf.get(_PROFILES_BY_MODEL_KEY)
                )
                base_name = cf.get("bambu_profile_base_name") or ""
                if not filament_id:
                    filament_id = spool.filament_id
        if filament_id and not profiles and not base_name:
            profiles = await self._read_filament_profiles_by_model(int(filament_id))
        if not profiles and base_name:
            by_model = await self._model_printer_map()
            for model in by_model:
                profiles[model] = {"base_name": base_name, "source": "legacy"}
        # Only scrub unmapped rows when the cloud preset index actually loaded.
        # If the cloud list failed to load (offline/transient), every variant would
        # resolve as "missing" and we would wrongly delete valid stored profiles on
        # a plain read. Absence of a healthy index is not evidence the row is invalid.
        cloud_index = await self._ensure_variant_index()
        cloud_healthy = bool(cloud_index)
        scrubbed = False
        if spool_id and profiles and cloud_healthy:
            scrubbed = await self._scrub_unmapped_profiles(
                profiles,
                spool_id=int(spool_id),
                filament_id=int(filament_id) if filament_id else None,
            )
            if scrubbed:
                await self._write_spool_profiles_by_model(int(spool_id), profiles)
        elif filament_id and profiles and cloud_healthy:
            scrubbed = await self._scrub_unmapped_profiles(
                profiles,
                filament_id=int(filament_id),
            )
            if scrubbed:
                await self._write_filament_profiles_by_model(int(filament_id), profiles)
        coverage = await self._resolve_model_coverage_map(
            profiles,
            spool_id=int(spool_id) if spool_id else None,
            filament_id=int(filament_id) if filament_id else None,
        )
        stored_default = ""
        if spool_id:
            stored_default = await self._read_spool_default_base_name(int(spool_id))
        elif filament_id:
            stored_default = await self._read_filament_default_base_name(int(filament_id))
        default_base_name = self._infer_default_base_name(profiles, stored_default)
        pending_display_name = False
        if not default_base_name:
            pending_display_name = await self._has_stored_cloud_setting_id(
                spool_id=int(spool_id) if spool_id else None,
                filament_id=int(filament_id) if filament_id else None,
            )
        return {
            "spool_id": int(spool_id) if spool_id else None,
            "filament_id": int(filament_id) if filament_id else None,
            "default_base_name": default_base_name,
            "pending_display_name": pending_display_name,
            "profiles_by_model": profiles,
            "per_model_profiles_enabled": self._per_printer_profiles,
            "coverage": coverage,
        }

    async def _get_default_nozzle_mm(
        self,
        filaman_printer_id: int,
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
    ) -> float | None:
        """Read optional ``bambu_default_nozzle_mm`` from spool then filament printer params."""
        try:
            async with async_session_maker() as db:
                if spool_id is not None:
                    res = await db.execute(
                        select(SpoolPrinterParam.param_value).where(
                            SpoolPrinterParam.spool_id == spool_id,
                            SpoolPrinterParam.printer_id == filaman_printer_id,
                            SpoolPrinterParam.param_key == "bambu_default_nozzle_mm",
                        )
                    )
                    val = res.scalar_one_or_none()
                    if val not in (None, ""):
                        return _float_or_none(val)
                if filament_id is not None:
                    res = await db.execute(
                        select(FilamentPrinterParam.param_value).where(
                            FilamentPrinterParam.filament_id == filament_id,
                            FilamentPrinterParam.printer_id == filaman_printer_id,
                            FilamentPrinterParam.param_key == "bambu_default_nozzle_mm",
                        )
                    )
                    val = res.scalar_one_or_none()
                    if val not in (None, ""):
                        return _float_or_none(val)
        except Exception as e:
            logger.debug(
                f"Could not read bambu_default_nozzle_mm for printer {filaman_printer_id}: {e}"
            )
        return None

    async def _get_bambuddy_printer_context(
        self, bambuddy_printer_id: int | None = None
    ) -> dict[str, Any]:
        """Fetch model + live nozzle from Bambuddy (cached per bambuddy printer id)."""
        bb_id = bambuddy_printer_id or self._bambuddy_printer_id
        if not bb_id:
            return {"model": "", "nozzle_mm": None, "bambuddy_printer_id": bb_id}
        if bb_id in self._printer_context_cache:
            return self._printer_context_cache[bb_id]

        ctx: dict[str, Any] = {
            "model": "",
            "nozzle_mm": None,
            "bambuddy_printer_id": bb_id,
        }
        try:
            if self._client:
                pr = await self._bb_get(f"/api/v1/printers/{bb_id}")
                if isinstance(pr, dict):
                    raw_model = (
                        pr.get("machine_type")
                        or pr.get("model")
                        or pr.get("printer_model")
                        or pr.get("name")
                        or ""
                    )
                    ctx["model"] = _canonical_printer_model_token(str(raw_model))
                    if self._debug_enabled:
                        logger.debug(
                            f"Bambuddy printer {bb_id} info JSON: {json.dumps(pr)[:2000]}"
                        )
                status = await self._bb_get(f"/api/v1/printers/{bb_id}/status")
                if isinstance(status, dict):
                    for key in (
                        "nozzle_diameter",
                        "nozzle_size",
                        "nozzle_diameters",
                        "nozzle_diameter_mm",
                    ):
                        if key in status and status[key] not in (None, ""):
                            if isinstance(status[key], (list, tuple)) and status[key]:
                                ctx["nozzle_mm"] = _float_or_none(status[key][0])
                            else:
                                ctx["nozzle_mm"] = _float_or_none(status[key])
                            break
        except Exception as e:
            logger.debug(f"Could not fetch printer context for Bambuddy id {bb_id}: {e}")

        self._printer_context_cache[bb_id] = ctx
        return ctx

    def _peer_drivers(self) -> list["Driver"]:
        return list(self._url_instances.get(self._bambuddy_url, [self]))

    async def _resolve_peer_variant_map(
        self,
        base_name: str,
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
    ) -> tuple[dict[int, str], dict[int, dict[str, Any]]]:
        """Resolve PFUS variants per model and expand to per-printer maps."""
        by_model = await self._model_printer_map()
        profiles = {
            m: {"base_name": base_name, "source": "auto"} for m in by_model
        }
        variants_by_model, coverage_by_model = await self._resolve_and_mirror_profiles(
            profiles,
            spool_id=spool_id,
            filament_id=filament_id,
            spool=bool(spool_id),
        )
        variants: dict[int, str] = {}
        coverage: dict[int, dict[str, Any]] = {}
        for model, detail in coverage_by_model.items():
            code = detail.get("code")
            for pid in by_model.get(model, []):
                if code:
                    variants[pid] = code
                coverage[pid] = {**detail, "printer_id": pid}
                if not code and model:
                    logger.warning(
                        f"No cloud variant for {base_name!r} on model {model} "
                        f"(printer {pid}); create {detail.get('expected_name')!r} "
                        f"in Bambu Studio and sync"
                    )
        return variants, coverage

    async def _resolve_setting_id_for_assign(
        self, filament_data: dict[str, Any]
    ) -> str:
        """Pick the PFUS to send as AMS ``setting_id`` for this printer."""
        if not self._per_printer_profiles:
            return filament_data.get("bambu_setting_id") or ""

        pfus = (filament_data.get("bambu_slicer_setting_id") or "").strip()
        setting_id = pfus
        ctx = await self._get_bambuddy_printer_context()
        model = (ctx.get("model") or "").upper()
        live_nozzle = ctx.get("nozzle_mm")
        fm_spool_id = _int_or_none(filament_data.get("id"))
        filament_id = _int_or_none(filament_data.get("filament_id"))
        # Callers don't always include filament_id (older backends / third-party
        # plugins) — resolve it from the spool so the filament-level profile
        # fallbacks below actually fire.
        if filament_id is None and fm_spool_id:
            try:
                async with async_session_maker() as db:
                    spool = await db.get(Spool, fm_spool_id)
                    if spool:
                        filament_id = spool.filament_id
            except Exception as e:
                logger.debug(
                    f"Could not resolve filament_id for spool {fm_spool_id}: {e}"
                )
        if live_nozzle is None:
            live_nozzle = await self._get_default_nozzle_mm(
                self.printer_id, spool_id=fm_spool_id, filament_id=filament_id
            )
        if live_nozzle is None:
            live_nozzle = await self._nozzle_for_model(
                model, spool_id=fm_spool_id, filament_id=filament_id
            )

        # Resolve the logical base name for this printer's model. ``base_source``
        # records where it came from so a lazily-resolved variant is persisted at
        # the right level (and never pins a spool to a value it should inherit).
        base_name = ""
        base_source = ""  # "spool" | "filament"
        used_default_base_fallback = False
        if fm_spool_id:
            profiles = await self._read_spool_profiles_by_model(fm_spool_id)
            if model and model in profiles:
                base_name = profiles[model].get("base_name") or ""
                if base_name:
                    base_source = "spool"
        if not base_name and filament_id:
            profiles = await self._read_filament_profiles_by_model(filament_id)
            if model and model in profiles:
                base_name = profiles[model].get("base_name") or ""
                if base_name:
                    base_source = "filament"
        # New model (no per-model row yet): fall back to the stored default base
        # name so a freshly connected printer model resolves the correct variant at
        # assign time without requiring a manual profile re-save.
        if not base_name and fm_spool_id:
            base_name = await self._read_spool_default_base_name(fm_spool_id)
            if base_name:
                base_source = "spool"
                used_default_base_fallback = True
        if not base_name and filament_id:
            base_name = await self._read_filament_default_base_name(filament_id)
            if base_name:
                base_source = "filament"
                used_default_base_fallback = True
        if not base_name and pfus:
            preset_name = await self.resolve_preset_name(pfus)
            base_name = coerce_profile_base_name(preset_name, pfus)

        if base_name and model:
            detail = await self._resolve_model_variant_detail(
                base_name,
                model,
                spool_id=fm_spool_id,
                filament_id=filament_id,
                nozzle_mm=live_nozzle,
            )
            if detail.get("code"):
                resolved = str(detail["code"])
                # When there is no per-model profile row, the default base can
                # resolve to a *different* PFUS than the one already stored for
                # this printer (e.g. default "PLA PLUS GEN2" vs another model's
                # override still sitting in bambu_slicer_setting_id). Prefer the
                # stored *custom* setting_id (PFUS/PFCN) so inventory reflect /
                # missing map rows cannot silently swap that printer onto the
                # default variant.
                #
                # Do NOT prefer stock builtin ids (GFSB01_16 "Bambu ASA", etc.):
                # those often arrive via filament-level inheritance and make
                # Studio show the Bambu brand name even when the spool's profile
                # base resolves to a synced custom preset (Overture/Sunlu ASA).
                if (
                    used_default_base_fallback
                    and pfus
                    and pfus != resolved
                    and _is_cloud_setting_id(pfus)
                ):
                    setting_id = pfus
                else:
                    setting_id = resolved
                # Lazily persist when missing, or when replacing a non-custom
                # leftover so the next assign / picker coverage stays correct.
                if not pfus or (
                    setting_id != pfus and not _is_cloud_setting_id(pfus)
                ):
                    await self._persist_resolved_setting_id(
                        setting_id,
                        base_source,
                        spool_id=fm_spool_id,
                        filament_id=filament_id,
                    )
            elif live_nozzle is not None and pfus:
                stored_nozzle = _parse_cloud_preset_name(
                    await self.resolve_preset_name(pfus) or pfus
                )[2]
                if stored_nozzle is not None and stored_nozzle != live_nozzle:
                    logger.warning(
                        f"No cloud variant for nozzle {live_nozzle} on {model}; "
                        f"using stored PFUS for slot configure"
                    )
        if not setting_id:
            return filament_data.get("bambu_setting_id") or ""
        return setting_id

    async def _persist_resolved_setting_id(
        self,
        setting_id: str,
        base_source: str,
        *,
        spool_id: int | None = None,
        filament_id: int | None = None,
    ) -> None:
        """Persist an assign-time resolved PFUS to this printer's params.

        Writes to the spool only when the base name came from spool-level data;
        otherwise writes to the filament so shared spools keep inheriting it.
        """
        try:
            if base_source == "spool" and spool_id:
                await self._upsert_spool_bambu_slicer_setting_id(
                    int(spool_id), {self.printer_id: setting_id}
                )
            elif filament_id:
                async with async_session_maker() as db:
                    changed = await self._upsert_filament_bambu_slicer_setting_id(
                        db, int(filament_id), {self.printer_id: setting_id}
                    )
                    if changed:
                        await db.commit()
        except Exception as e:
            logger.debug(f"Could not persist resolved setting_id: {e}")

    async def list_cloud_presets(
        self,
        force: bool = False,
        model: str | None = None,
        group: str | None = None,
    ) -> dict[str, Any]:
        """Öffentliche Action: gibt die Cloud-Preset-Liste für die FilaMan-UI zurück.

        Optional ``model`` filters to one printer model; ``group=base`` dedupes by
        logical base name (nozzle suffix hidden from the picker).
        """
        presets = await self._load_cloud_presets(force=force)
        if group == "base":
            presets = _group_presets_by_base_name(presets, model_token=model)
            if model:
                index = await self._ensure_variant_index()
                groups = await self._ensure_variant_groups()
                presets = _filter_grouped_presets_for_model(
                    presets, groups, model
                )
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

    async def debug_profile_coverage(self, spool_id: int) -> dict[str, Any]:
        """Read-only: resolved per-model PFUS variants + coverage for QA/debug."""
        return await self.get_profile_coverage(spool_id=int(spool_id))

    async def _filament_data_for_spool(self, spool_id: int) -> dict[str, Any]:
        """Build assignment filament_data (color/material + printer params) for a spool."""
        from app.plugins.manager import plugin_manager

        base: dict[str, Any] = {"id": spool_id}
        async with async_session_maker() as db:
            result = await db.execute(
                select(Spool)
                .where(Spool.id == spool_id)
                .options(
                    selectinload(Spool.filament)
                    .selectinload(Filament.filament_colors)
                    .selectinload(FilamentColor.color),
                )
            )
            spool = result.scalar_one_or_none()
            if spool and spool.filament:
                fil = spool.filament
                if fil.material_type:
                    base["material_type"] = fil.material_type
                if fil.material_subgroup:
                    base["material_subgroup"] = fil.material_subgroup
                colors = sorted(
                    fil.filament_colors or [], key=lambda fc: fc.position
                )
                if colors and colors[0].color and colors[0].color.hex_code:
                    raw = colors[0].color.hex_code.lstrip("#")
                    base["color"] = (
                        (raw + "FF").upper() if len(raw) == 6 else raw.upper()
                    )
        return await plugin_manager.enrich_filament_data(
            spool_id, self.printer_id, base
        )

    async def reconfigure_assigned_slots(
        self,
        ams_id: int | None = None,
        tray_id: int | None = None,
    ) -> dict[str, Any]:
        """Re-configure occupied AMS slots (internal + manual debug action)."""
        if not self._slot_to_filaman_spool:
            return {"reconfigured": 0, "skipped": "no_assigned_slots"}

        count = 0
        for slot_key, spool_id in list(self._slot_to_filaman_spool.items()):
            parts = slot_key.split("-", 1)
            if len(parts) != 2:
                continue
            slot_ams, slot_tray = int(parts[0]), int(parts[1])
            if ams_id is not None and tray_id is not None:
                if slot_ams != ams_id or slot_tray != tray_id:
                    continue
            filament_data = await self._filament_data_for_spool(int(spool_id))
            await self._send_assignment(slot_ams, slot_tray, filament_data)
            count += 1
        if count:
            logger.info(
                f"Reconfigured {count} assigned slot(s) on printer {self.printer_id} "
                f"after nozzle/profile context change"
            )
            self.emit(
                {
                    "event_type": "slots_reconfigured",
                    "printer_id": self.printer_id,
                    "count": count,
                    "reason": "nozzle_change",
                }
            )
        return {"reconfigured": count}

    @staticmethod
    def _parse_print_state(status_data: dict[str, Any]) -> str:
        for key in ("gcode_state", "print_state", "state", "job_state"):
            val = status_data.get(key)
            if val not in (None, ""):
                return str(val)
        return ""

    @staticmethod
    def _is_printing(print_state: str) -> bool:
        upper = (print_state or "").upper()
        return upper in {
            "RUNNING",
            "PAUSE",
            "PREPARE",
            "SLICING",
            "PRINTING",
            "ACTIVE",
        }

    def _parse_nozzle_context(self, status_data: dict[str, Any]) -> dict[str, Any]:
        nozzle_mm: float | None = None
        for key in (
            "nozzle_diameter",
            "nozzle_size",
            "nozzle_diameter_mm",
            "nozzle_diameters",
        ):
            if key not in status_data:
                continue
            val = status_data[key]
            if isinstance(val, (list, tuple)) and val:
                nozzle_mm = _float_or_none(val[0])
            else:
                nozzle_mm = _float_or_none(val)
            if nozzle_mm is not None:
                break
        return {
            "nozzle_mm": nozzle_mm,
            "print_state": self._parse_print_state(status_data),
        }

    async def _run_reconfigure_assigned_slots(self) -> None:
        try:
            await self.reconfigure_assigned_slots()
        except Exception as e:
            logger.warning(
                f"reconfigure_assigned_slots failed for printer {self.printer_id}: {e}"
            )

    def _schedule_reconfigure_assigned_slots(self, delay: float = 7.0) -> None:
        if self._reconfigure_task and not self._reconfigure_task.done():
            self._reconfigure_task.cancel()
        self._reconfigure_task = asyncio.create_task(self._debounced_reconfigure(delay))
        self._reconfigure_task.add_done_callback(self._on_task_done)

    async def _debounced_reconfigure(self, delay: float) -> None:
        await asyncio.sleep(delay)
        await self._run_reconfigure_assigned_slots()
        self._pending_reconfigure_after_print = False

    async def _maybe_reconfigure_on_nozzle_change(
        self, status_data: dict[str, Any]
    ) -> None:
        if not self._per_printer_profiles or not self._slot_to_filaman_spool:
            return
        ctx = self._parse_nozzle_context(status_data)
        prev = self._last_nozzle_context
        self._last_nozzle_context = ctx
        if prev is None:
            return
        nozzle_changed = prev.get("nozzle_mm") != ctx.get("nozzle_mm")
        if not nozzle_changed:
            return
        if ctx.get("nozzle_mm") is not None:
            self._merge_printer_context_nozzle(
                self._bambuddy_printer_id, ctx.get("nozzle_mm")
            )
        print_state = ctx.get("print_state") or ""
        self._last_print_state = print_state
        if self._is_printing(print_state):
            self._pending_reconfigure_after_print = True
            logger.info(
                f"Nozzle change detected on printer {self.printer_id} during print; "
                f"deferring slot reconfigure until idle"
            )
            return
        self._schedule_reconfigure_assigned_slots()

    async def _legacy_fanout_mirrored_profiles(self) -> None:
        """Startup: infer bambu_profiles_by_model from legacy single-PFUS rows."""
        if not self._per_printer_profiles:
            return
        peers = self._peer_printer_ids()
        if len(peers) <= 1:
            return
        try:
            by_model = await self._model_printer_map()
            async with async_session_maker() as db:
                res = await db.execute(
                    select(SpoolPrinterParam).where(
                        SpoolPrinterParam.printer_id.in_(peers),
                        SpoolPrinterParam.param_key == "bambu_slicer_setting_id",
                    )
                )
                by_spool: dict[int, dict[int, str]] = {}
                for row in res.scalars().all():
                    if not row.param_value or not row.param_value.startswith("PFUS"):
                        continue
                    by_spool.setdefault(row.spool_id, {})[row.printer_id] = (
                        row.param_value
                    )
            updated = 0
            for spool_id, per_pid in by_spool.items():
                spool = None
                async with async_session_maker() as db:
                    spool = await db.get(Spool, spool_id)
                if not spool:
                    continue
                cf = dict(spool.custom_fields or {})
                existing = self._normalize_profiles_by_model(
                    cf.get(_PROFILES_BY_MODEL_KEY)
                )
                if existing:
                    continue
                base_name = cf.get("bambu_profile_base_name") or ""
                if not base_name:
                    codes = set(per_pid.values())
                    if len(codes) == 1:
                        raw_code = next(iter(codes))
                        preset_name = await self.resolve_preset_name(raw_code)
                        base_name = coerce_profile_base_name(preset_name, raw_code)
                if not base_name:
                    continue
                profiles: dict[str, dict[str, str]] = {}
                for model in by_model:
                    detail = await self._resolve_model_variant_detail(
                        base_name,
                        model,
                        spool_id=spool_id,
                        filament_id=spool.filament_id,
                    )
                    if detail.get("mapped"):
                        profiles[model] = {"base_name": base_name, "source": "legacy"}
                if not profiles:
                    continue
                await self._write_spool_profiles_by_model(spool_id, profiles)
                await self._resolve_and_mirror_profiles(
                    profiles,
                    spool_id=spool_id,
                    filament_id=spool.filament_id,
                    spool=True,
                )
                updated += 1
            if updated:
                logger.info(
                    f"Legacy profile migration updated {updated} spool(s) for URL "
                    f"{self._bambuddy_url}"
                )
        except Exception as e:
            logger.debug(f"Legacy profile fan-out skipped: {e}")

    def _is_full_preset(self, code: str | None) -> bool:
        """True wenn ``code`` ein echtes (selektierbares) Slicer-Preset ist.

        Volle Presets sind die setting_ids/Builtin-IDs aus dem Cloud-Katalog
        (PFUS…/GFxx). Generische AMS-Codes (z.B. "SUN20010") sind NICHT enthalten
        und gelten daher nicht als volles Preset.
        """
        if not code:
            return False
        return code.startswith("PFUS") or code in self._cloud_presets_by_code

    async def backfill_setting_ids(self) -> dict[str, Any]:
        """Einmaliger Backfill: kopiert volle Presets aus bambu_idx → bambu_slicer_setting_id.

        Bestehende Profile, die als volles Preset (PFUS…/GFxx) in bambu_idx oder
        in custom_fields.bambu_slicer_filament stehen, werden nach
        bambu_slicer_setting_id übernommen, wenn dort noch kein Wert existiert.
        Generische AMS-Codes werden
        übersprungen (der Nutzer wählt deren volles Variant-Preset im Picker neu).
        Idempotent.
        """
        await self._load_cloud_presets()
        filaments = 0
        spools = 0
        async with async_session_maker() as db:
            # Filament-Ebene
            res = await db.execute(
                select(FilamentPrinterParam).where(
                    FilamentPrinterParam.printer_id.in_(self._peer_printer_ids()),
                    FilamentPrinterParam.param_key.in_(
                        ["bambu_idx", "bambu_slicer_setting_id"]
                    ),
                )
            )
            fparams = res.scalars().all()
            have_setting: set[tuple[int, int]] = {
                (p.filament_id, p.printer_id)
                for p in fparams
                if p.param_key == "bambu_slicer_setting_id"
            }
            for p in fparams:
                if (
                    p.param_key == "bambu_idx"
                    and self._is_full_preset(p.param_value)
                    and (p.filament_id, p.printer_id) not in have_setting
                ):
                    db.add(
                        FilamentPrinterParam(
                            filament_id=p.filament_id,
                            printer_id=p.printer_id,
                            param_key="bambu_slicer_setting_id",
                            param_value=p.param_value,
                        )
                    )
                    have_setting.add((p.filament_id, p.printer_id))
                    filaments += 1

            # Spool-Ebene (bambu_idx + custom_fields.bambu_slicer_filament)
            res = await db.execute(
                select(SpoolPrinterParam).where(
                    SpoolPrinterParam.printer_id.in_(self._peer_printer_ids()),
                    SpoolPrinterParam.param_key.in_(
                        ["bambu_idx", "bambu_slicer_setting_id"]
                    ),
                )
            )
            sparams = res.scalars().all()
            shave_setting: set[tuple[int, int]] = {
                (p.spool_id, p.printer_id)
                for p in sparams
                if p.param_key == "bambu_slicer_setting_id"
            }
            for p in sparams:
                if (
                    p.param_key == "bambu_idx"
                    and self._is_full_preset(p.param_value)
                    and (p.spool_id, p.printer_id) not in shave_setting
                ):
                    db.add(
                        SpoolPrinterParam(
                            spool_id=p.spool_id,
                            printer_id=p.printer_id,
                            param_key="bambu_slicer_setting_id",
                            param_value=p.param_value,
                        )
                    )
                    shave_setting.add((p.spool_id, p.printer_id))
                    spools += 1

            if filaments or spools:
                await db.commit()
        logger.info(
            f"backfill_setting_ids: seeded {filaments} filament(s), "
            f"{spools} spool(s) with full slicer presets"
        )
        return {"filaments": filaments, "spools": spools}

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

    async def _sanitize_pfus_stored_as_bambu_idx(self) -> None:
        """Move PFUS/PFCN out of ``bambu_idx`` into ``bambu_slicer_setting_id``.

        Older reflect/set_profile paths used ``resolve() or pfus``, which stored
        cloud setting_ids as AMS tray codes. Fix in place on startup so Studio
        keeps getting a real tray code + a preserved PFUS setting_id.
        """
        try:
            peers = self._peer_printer_ids()
            fixed = 0
            async with async_session_maker() as db:
                # --- spools ---
                result = await db.execute(
                    select(SpoolPrinterParam).where(
                        SpoolPrinterParam.printer_id.in_(peers),
                        SpoolPrinterParam.param_key == "bambu_idx",
                    )
                )
                spool_idx_rows = list(result.scalars().all())
                for row in spool_idx_rows:
                    raw = (row.param_value or "").strip()
                    if not _is_cloud_setting_id(raw):
                        continue
                    # Ensure setting_id carries the PFUS.
                    existing_set = await db.execute(
                        select(SpoolPrinterParam).where(
                            SpoolPrinterParam.spool_id == row.spool_id,
                            SpoolPrinterParam.printer_id == row.printer_id,
                            SpoolPrinterParam.param_key == "bambu_slicer_setting_id",
                        )
                    )
                    set_row = existing_set.scalar_one_or_none()
                    if set_row is None:
                        db.add(
                            SpoolPrinterParam(
                                spool_id=row.spool_id,
                                printer_id=row.printer_id,
                                param_key="bambu_slicer_setting_id",
                                param_value=raw,
                            )
                        )
                    elif not set_row.param_value:
                        set_row.param_value = raw
                    ams = await self._resolved_ams_tray_code(raw)
                    if ams:
                        row.param_value = ams
                    else:
                        await db.delete(row)
                    fixed += 1

                # --- filaments ---
                result = await db.execute(
                    select(FilamentPrinterParam).where(
                        FilamentPrinterParam.printer_id.in_(peers),
                        FilamentPrinterParam.param_key == "bambu_idx",
                    )
                )
                for row in result.scalars().all():
                    raw = (row.param_value or "").strip()
                    if not _is_cloud_setting_id(raw):
                        continue
                    existing_set = await db.execute(
                        select(FilamentPrinterParam).where(
                            FilamentPrinterParam.filament_id == row.filament_id,
                            FilamentPrinterParam.printer_id == row.printer_id,
                            FilamentPrinterParam.param_key
                            == "bambu_slicer_setting_id",
                        )
                    )
                    set_row = existing_set.scalar_one_or_none()
                    if set_row is None:
                        db.add(
                            FilamentPrinterParam(
                                filament_id=row.filament_id,
                                printer_id=row.printer_id,
                                param_key="bambu_slicer_setting_id",
                                param_value=raw,
                            )
                        )
                    elif not set_row.param_value:
                        set_row.param_value = raw
                    ams = await self._resolved_ams_tray_code(raw)
                    if ams:
                        row.param_value = ams
                    else:
                        await db.delete(row)
                    fixed += 1

                if fixed:
                    await db.commit()
                    logger.info(
                        f"Sanitized {fixed} bambu_idx row(s) that held PFUS/PFCN "
                        f"on printers {peers}"
                    )
        except Exception as e:
            logger.warning(f"bambu_idx PFUS sanitize failed: {e}")

    async def _upsert_spool_bambu_idx(
        self, filaman_spool_id: int, code: str | dict[int, str]
    ) -> bool:
        """Set ``spool_printer_params.bambu_idx`` per peer printer.

        Accepts a single generic code (all peers) or ``{printer_id: generic}``.
        PFUS/PFCN values are rejected — those belong in ``bambu_slicer_setting_id``.
        """
        if isinstance(code, str):
            mapping = {pid: code for pid in self._peer_printer_ids()}
        else:
            mapping = dict(code)
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
                val = _ams_tray_code(mapping.get(pid))
                if not val:
                    raw = mapping.get(pid)
                    if raw and _is_cloud_setting_id(str(raw)):
                        logger.warning(
                            f"Refusing to store cloud setting_id {raw!r} as "
                            f"bambu_idx for spool {filaman_spool_id} "
                            f"(printer {pid}) — use bambu_slicer_setting_id"
                        )
                    continue
                existing = existing_by_pid.get(pid)
                if existing:
                    if existing.param_value != val:
                        existing.param_value = val
                        changed = True
                else:
                    db.add(
                        SpoolPrinterParam(
                            spool_id=filaman_spool_id,
                            printer_id=pid,
                            param_key="bambu_idx",
                            param_value=val,
                        )
                    )
                    changed = True
            if changed:
                await db.commit()
        return changed

    async def _upsert_spool_bambu_slicer_setting_id(
        self, filaman_spool_id: int, code: str | dict[int, str]
    ) -> bool:
        """Set ``spool_printer_params.bambu_slicer_setting_id`` per peer printer.

        Accepts either a single code (legacy: same value for all peers) or a
        ``{printer_id: pfus}`` mapping (per-printer variants). Printers absent from
        a mapping are left unchanged.
        """
        if isinstance(code, str):
            mapping = {pid: code for pid in self._peer_printer_ids()}
        else:
            mapping = dict(code)

        changed = False
        async with async_session_maker() as db:
            result = await db.execute(
                select(SpoolPrinterParam).where(
                    SpoolPrinterParam.spool_id == filaman_spool_id,
                    SpoolPrinterParam.printer_id.in_(self._peer_printer_ids()),
                    SpoolPrinterParam.param_key == "bambu_slicer_setting_id",
                )
            )
            existing_by_pid = {p.printer_id: p for p in result.scalars().all()}
            for pid, pfus in mapping.items():
                if pid not in self._peer_printer_ids() or not pfus:
                    continue
                existing = existing_by_pid.get(pid)
                if existing:
                    if existing.param_value != pfus:
                        existing.param_value = pfus
                        changed = True
                else:
                    db.add(
                        SpoolPrinterParam(
                            spool_id=filaman_spool_id,
                            printer_id=pid,
                            param_key="bambu_slicer_setting_id",
                            param_value=pfus,
                        )
                    )
                    changed = True
            if changed:
                await db.commit()
        return changed

    async def _upsert_spool_profile_base_name(
        self, filaman_spool_id: int, base_name: str
    ) -> None:
        """Store the logical profile base name (separate from bambu_slicer_filament code)."""
        if not filaman_spool_id or not base_name or is_cloud_setting_id(base_name):
            return
        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, filaman_spool_id)
                if spool is None:
                    return
                cf = dict(spool.custom_fields or {})
                if cf.get("bambu_profile_base_name") != base_name:
                    cf["bambu_profile_base_name"] = base_name
                    spool.custom_fields = cf
                    await db.commit()
        except Exception as e:
            logger.debug(
                f"Could not store profile base name for spool {filaman_spool_id}: {e}"
            )

    async def _upsert_filament_profile_base_name(
        self, filament_id: int, base_name: str
    ) -> None:
        if not filament_id or not base_name or is_cloud_setting_id(base_name):
            return
        try:
            async with async_session_maker() as db:
                filament = await db.get(Filament, filament_id)
                if filament is None:
                    return
                cf = dict(filament.custom_fields or {})
                if cf.get("bambu_profile_base_name") != base_name:
                    cf["bambu_profile_base_name"] = base_name
                    filament.custom_fields = cf
                    await db.commit()
        except Exception as e:
            logger.debug(
                f"Could not store profile base name for filament {filament_id}: {e}"
            )

    async def _fan_out_spool_profile_variants(
        self, spool_id: int, code: str, name: str | None
    ) -> dict[str, Any]:
        """Resolve and store per-model PFUS variants; return coverage metadata for UI."""
        base_name = coerce_profile_base_name(name, code)
        if not self._per_printer_profiles:
            await self._upsert_spool_bambu_slicer_setting_id(int(spool_id), code)
            peers = self._peer_printer_ids()
            return {
                "base_name": base_name,
                "coverage": {
                    pid: {"printer_id": pid, "mapped": True, "code": code}
                    for pid in peers
                },
            }

        filament_id: int | None = None
        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, int(spool_id))
                if spool:
                    filament_id = spool.filament_id
        except Exception:
            pass

        by_model = await self._model_printer_map()
        _, parsed_model, _ = _parse_cloud_preset_name(name or code)
        profiles = await self._read_spool_profiles_by_model(int(spool_id))
        if base_name:
            profiles = await self._link_default_to_models(
                profiles,
                base_name,
                spool_id=int(spool_id),
                filament_id=filament_id,
            )

        await self._write_spool_profiles_by_model(int(spool_id), profiles)
        variants_by_model, coverage_by_model = await self._resolve_and_mirror_profiles(
            profiles,
            spool_id=int(spool_id),
            filament_id=filament_id,
            spool=True,
        )
        await self._upsert_spool_profile_base_name(int(spool_id), base_name)

        coverage: dict[int, dict[str, Any]] = {}
        variants: dict[int, str] = {}
        for model, detail in coverage_by_model.items():
            code_v = detail.get("code")
            for pid in by_model.get(model, []):
                if code_v:
                    variants[pid] = code_v
                coverage[pid] = {**detail, "printer_id": pid}

        peers = self._peer_printer_ids()
        if len(peers) == 1 and not variants:
            variants[peers[0]] = code
            coverage[peers[0]] = {
                "printer_id": peers[0],
                "mapped": True,
                "code": code,
                "model": parsed_model
                or (await self._get_bambuddy_printer_context()).get("model"),
                "nozzle_mm": None,
            }
            await self._upsert_spool_bambu_slicer_setting_id(int(spool_id), variants)

        return {
            "base_name": base_name,
            "profiles_by_model": profiles,
            "coverage": coverage_by_model or coverage,
            "variants": variants,
        }

    async def _fan_out_filament_profile_variants(
        self, filament_id: int, code: str, name: str | None
    ) -> dict[str, Any]:
        """Resolve and store per-model PFUS defaults on a filament."""
        base_name = coerce_profile_base_name(name, code)
        if not self._per_printer_profiles:
            async with async_session_maker() as db:
                changed = await self._upsert_filament_bambu_slicer_setting_id(
                    db, int(filament_id), code
                )
                if changed:
                    await db.commit()
            peers = self._peer_printer_ids()
            return {
                "base_name": base_name,
                "coverage": {
                    pid: {"printer_id": pid, "mapped": True, "code": code}
                    for pid in peers
                },
            }

        by_model = await self._model_printer_map()
        _, parsed_model, _ = _parse_cloud_preset_name(name or code)
        profiles = await self._read_filament_profiles_by_model(int(filament_id))
        if base_name:
            profiles = await self._link_default_to_models(
                profiles,
                base_name,
                filament_id=int(filament_id),
            )

        await self._write_filament_profiles_by_model(int(filament_id), profiles)
        variants_by_model, coverage_by_model = await self._resolve_and_mirror_profiles(
            profiles,
            filament_id=int(filament_id),
            spool=False,
        )

        coverage: dict[int, dict[str, Any]] = {}
        variants: dict[int, str] = {}
        for model, detail in coverage_by_model.items():
            code_v = detail.get("code")
            for pid in by_model.get(model, []):
                if code_v:
                    variants[pid] = code_v
                coverage[pid] = {**detail, "printer_id": pid}

        peers = self._peer_printer_ids()
        if len(peers) == 1 and not variants:
            variants[peers[0]] = code
            coverage[peers[0]] = {
                "printer_id": peers[0],
                "mapped": True,
                "code": code,
                "model": parsed_model
                or (await self._get_bambuddy_printer_context()).get("model"),
                "nozzle_mm": None,
            }
            async with async_session_maker() as db:
                changed = await self._upsert_filament_bambu_slicer_setting_id(
                    db, int(filament_id), variants
                )
                if changed:
                    await db.commit()

        return {
            "base_name": base_name,
            "profiles_by_model": profiles,
            "coverage": coverage_by_model or coverage,
            "variants": variants,
        }

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

    async def _mirror_spool_generic_indices_from_variants(
        self, spool_id: int, variants_by_model: dict[str, str]
    ) -> None:
        """Map each model's PFUS to generic AMS codes on that model's printer(s)."""
        if not variants_by_model:
            return
        by_model = await self._model_printer_map()
        mapping: dict[int, str] = {}
        for model, pfus in variants_by_model.items():
            if not pfus:
                continue
            generic = await self._resolved_ams_tray_code(pfus)
            if not generic:
                continue
            for pid in by_model.get(model.upper(), []):
                mapping[pid] = generic
        if mapping:
            await self._upsert_spool_bambu_idx(int(spool_id), mapping)

    async def _sync_spool_inventory_display(
        self,
        spool_id: int,
        variants_by_model: dict[str, str],
    ) -> str | None:
        """Update shared Bambuddy inventory fields only when one PFUS fits all models."""
        rep_code = _uniform_variant_code(variants_by_model)
        if not rep_code:
            await self._debounced_sync()
            return None
        rep_name = await self.resolve_preset_name(rep_code)
        await self._upsert_spool_slicer_custom_fields(int(spool_id), rep_code, rep_name)
        bb_id = await self._get_bambuddy_spool_id(int(spool_id))
        if bb_id is not None and self._client:
            payload: dict[str, Any] = {"slicer_filament": rep_code}
            if rep_name:
                payload["slicer_filament_name"] = rep_name
            try:
                await self._bb_patch(
                    f"/api/v1/inventory/spools/{bb_id}", payload
                )
            except Exception as e:
                logger.warning(
                    f"Failed to push profile to Bambuddy spool {bb_id}: {e}"
                )
        else:
            await self._debounced_sync()
        return rep_code

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

    async def set_default_spool_profile(
        self,
        spool_id: int,
        *,
        base_name: str | None = None,
        code: str | None = None,
    ) -> dict[str, Any]:
        """Set the default slicer profile and link variants for all non-overridden models."""
        if not spool_id:
            raise ValueError("spool_id is required")
        if not base_name and not code:
            raise ValueError("base_name or code is required")
        if code and not base_name:
            name = await self.resolve_preset_name(code)
            base_name = coerce_profile_base_name(name, code)
        if not base_name:
            raise ValueError("Could not determine base_name")

        self._local_profile_writes[int(spool_id)] = time.monotonic()

        filament_id: int | None = None
        async with async_session_maker() as db:
            spool = await db.get(Spool, int(spool_id))
            if not spool:
                raise ValueError(f"Spool {spool_id} not found")
            filament_id = spool.filament_id

        profiles = await self._read_spool_profiles_by_model(int(spool_id))
        profiles = await self._link_default_to_models(
            profiles,
            base_name,
            spool_id=int(spool_id),
            filament_id=filament_id,
        )
        await self._write_spool_profiles_by_model(int(spool_id), profiles)
        await self._upsert_spool_profile_base_name(int(spool_id), base_name)
        variants_by_model, _ = await self._resolve_and_mirror_profiles(
            profiles,
            spool_id=int(spool_id),
            filament_id=filament_id,
            spool=True,
        )
        coverage = await self._resolve_model_coverage_map(
            profiles,
            spool_id=int(spool_id),
            filament_id=filament_id,
        )

        await self._mirror_spool_generic_indices_from_variants(
            int(spool_id), variants_by_model
        )
        rep_code = await self._sync_spool_inventory_display(
            int(spool_id), variants_by_model
        )

        return {
            "spool_id": int(spool_id),
            "base_name": base_name,
            "default_base_name": base_name,
            "code": rep_code,
            "profiles_by_model": profiles,
            "coverage": coverage,
            "variants": variants_by_model,
        }

    async def _load_spool_backfill_source(self, spool_id: int) -> dict[str, Any]:
        """Read persisted spool profile data for backfill (not inherited filament rows)."""
        async with async_session_maker() as db:
            spool = await db.get(Spool, int(spool_id))
            if not spool:
                raise ValueError(f"Spool {spool_id} not found")
            if not spool.filament_id:
                raise ValueError("Spool has no filament")
            filament_id = int(spool.filament_id)

        default_base = await self._read_spool_default_base_name(int(spool_id))
        profiles = await self._read_spool_profiles_by_model(int(spool_id))
        connected_models = sorted((await self._model_printer_map()).keys())

        if not default_base and not profiles:
            return {
                "can_backfill": False,
                "reason": "no_spool_profiles",
                "spool_id": int(spool_id),
                "filament_id": filament_id,
                "connected_models": connected_models,
            }

        if not default_base:
            default_base = self._infer_default_base_name(profiles, "")

        effective_profiles = normalize_profiles_for_filament_copy(profiles)
        if default_base:
            effective_profiles = await self._link_default_to_models(
                effective_profiles,
                default_base,
                spool_id=int(spool_id),
                filament_id=filament_id,
            )

        return {
            "can_backfill": True,
            "spool_id": int(spool_id),
            "filament_id": filament_id,
            "default_base_name": default_base,
            "profiles_by_model": profiles,
            "effective_profiles": effective_profiles,
            "connected_models": connected_models,
            "overrides": extract_profile_overrides(profiles),
        }

    async def preview_backfill_spool_profiles_to_filament(
        self, spool_id: int
    ) -> dict[str, Any]:
        """Read-only preview: spool profiles → parent filament (+ diff vs connected models)."""
        source = await self._load_spool_backfill_source(int(spool_id))
        if not source.get("can_backfill"):
            return source

        filament_id = int(source["filament_id"])
        target_default = await self._read_filament_default_base_name(filament_id)
        target_profiles = await self._read_filament_profiles_by_model(filament_id)
        if not target_default:
            target_default = self._infer_default_base_name(target_profiles, "")

        effective_target = normalize_profiles_for_filament_copy(target_profiles)
        if target_default:
            effective_target = await self._link_default_to_models(
                effective_target,
                target_default,
                filament_id=filament_id,
            )

        changes = compute_profile_backfill_diff(
            connected_models=source["connected_models"],
            source_default=source["default_base_name"],
            source_profiles=source["effective_profiles"],
            target_default=target_default,
            target_profiles=effective_target,
        )
        coverage = await self._resolve_model_coverage_map(
            source["effective_profiles"],
            spool_id=int(spool_id),
            filament_id=filament_id,
        )

        return {
            "can_backfill": True,
            "spool_id": int(spool_id),
            "filament_id": filament_id,
            "connected_models": source["connected_models"],
            "source": {
                "default_base_name": source["default_base_name"],
                "profiles_by_model": source["effective_profiles"],
                "overrides": source["overrides"],
            },
            "current_filament": {
                "default_base_name": target_default,
                "profiles_by_model": effective_target,
                "overrides": extract_profile_overrides(target_profiles),
            },
            "changes": changes,
            "filament_already_matches": changes.get("filament_already_matches", False),
            "coverage": coverage,
        }

    async def _apply_profiles_to_sibling_spools(
        self,
        filament_id: int,
        profiles: dict[str, dict[str, str]],
        default_base: str,
        variants_by_model: dict[str, str],
        rep_code: str | None,
    ) -> int:
        """Push profile map to all non-archived spools on a filament."""
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

        applied = 0
        for sid in spool_ids:
            try:
                await self._write_spool_profiles_by_model(sid, dict(profiles))
                await self._mirror_pfus_by_model(
                    variants_by_model, spool_id=sid, spool=True
                )
                await self._upsert_spool_profile_base_name(sid, default_base)
                await self._mirror_spool_generic_indices_from_variants(
                    sid, variants_by_model
                )
                if rep_code:
                    await self._sync_spool_inventory_display(sid, variants_by_model)
                applied += 1
            except Exception as e:
                logger.warning(
                    f"backfill sibling apply failed for spool {sid}: {e}"
                )
        return applied

    async def backfill_spool_profiles_to_filament(
        self,
        spool_id: int,
        *,
        apply_to_sibling_spools: bool = False,
    ) -> dict[str, Any]:
        """Copy spool default + per-model profiles to parent filament (optional sibling push)."""
        source = await self._load_spool_backfill_source(int(spool_id))
        if not source.get("can_backfill"):
            raise ValueError(source.get("reason") or "cannot backfill")

        filament_id = int(source["filament_id"])
        default_base = source["default_base_name"]
        profiles = normalize_profiles_for_filament_copy(source["profiles_by_model"])
        profiles = await self._link_default_to_models(
            profiles,
            default_base,
            filament_id=filament_id,
        )

        await self._write_filament_profiles_by_model(filament_id, profiles)
        await self._upsert_filament_profile_base_name(filament_id, default_base)
        variants_by_model, _ = await self._resolve_and_mirror_profiles(
            profiles, filament_id=filament_id, spool=False
        )
        coverage = await self._resolve_model_coverage_map(
            profiles, filament_id=filament_id
        )

        rep_code = _uniform_variant_code(variants_by_model)
        if rep_code:
            ams = await self._resolved_ams_tray_code(rep_code)
            if ams:
                async with async_session_maker() as db:
                    changed = await self._upsert_filament_bambu_idx(
                        db,
                        filament_id,
                        ams,
                    )
                    if changed:
                        await db.commit()

        applied = 0
        if apply_to_sibling_spools and variants_by_model:
            applied = await self._apply_profiles_to_sibling_spools(
                filament_id,
                profiles,
                default_base,
                variants_by_model,
                rep_code,
            )
        else:
            await self._debounced_sync()

        return {
            "spool_id": int(spool_id),
            "filament_id": filament_id,
            "default_base_name": default_base,
            "profiles_by_model": profiles,
            "coverage": coverage,
            "variants": variants_by_model,
            "applied_to_spools": applied,
            "connected_models": source["connected_models"],
        }

    async def set_default_filament_profile(
        self,
        filament_id: int,
        *,
        base_name: str | None = None,
        code: str | None = None,
        apply_to_existing: bool = False,
    ) -> dict[str, Any]:
        """Set the default slicer profile on a filament and link per-model variants."""
        if not filament_id:
            raise ValueError("filament_id is required")
        if not base_name and not code:
            raise ValueError("base_name or code is required")
        if code and not base_name:
            name = await self.resolve_preset_name(code)
            base_name = coerce_profile_base_name(name, code)
        if not base_name:
            raise ValueError("Could not determine base_name")

        profiles = await self._read_filament_profiles_by_model(int(filament_id))
        profiles = await self._link_default_to_models(
            profiles,
            base_name,
            filament_id=int(filament_id),
        )
        await self._write_filament_profiles_by_model(int(filament_id), profiles)
        await self._upsert_filament_profile_base_name(int(filament_id), base_name)
        variants_by_model, _ = await self._resolve_and_mirror_profiles(
            profiles, filament_id=int(filament_id), spool=False
        )
        coverage = await self._resolve_model_coverage_map(
            profiles, filament_id=int(filament_id)
        )

        rep_code = code or _uniform_variant_code(variants_by_model)
        if rep_code:
            ams = await self._resolved_ams_tray_code(rep_code)
            if ams:
                async with async_session_maker() as db:
                    changed = await self._upsert_filament_bambu_idx(
                        db, int(filament_id), ams
                    )
                    if changed:
                        await db.commit()

        applied = 0
        if apply_to_existing and variants_by_model:
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
                    await self._write_spool_profiles_by_model(sid, dict(profiles))
                    await self._mirror_pfus_by_model(
                        variants_by_model, spool_id=sid, spool=True
                    )
                    await self._upsert_spool_profile_base_name(sid, base_name)
                    await self._mirror_spool_generic_indices_from_variants(
                        sid, variants_by_model
                    )
                    if rep_code:
                        await self._sync_spool_inventory_display(
                            sid, variants_by_model
                        )
                    applied += 1
                except Exception as e:
                    logger.warning(
                        f"apply_to_existing: failed for spool {sid}: {e}"
                    )
        else:
            await self._debounced_sync()

        return {
            "filament_id": int(filament_id),
            "base_name": base_name,
            "default_base_name": base_name,
            "code": rep_code,
            "profiles_by_model": profiles,
            "coverage": coverage,
            "variants": variants_by_model,
            "applied_to_spools": applied,
        }

    async def set_spool_profile_for_model(
        self,
        spool_id: int,
        model: str,
        *,
        base_name: str | None = None,
        code: str | None = None,
        link_others: bool = False,
        clear_override: bool = False,
    ) -> dict[str, Any]:
        """Set slicer profile base name for one printer model on a spool."""
        if not spool_id:
            raise ValueError("spool_id is required")
        if not model:
            raise ValueError("model is required")
        model_key = model.strip().upper()

        if clear_override:
            self._local_profile_writes[int(spool_id)] = time.monotonic()
            filament_id: int | None = None
            async with async_session_maker() as db:
                spool = await db.get(Spool, int(spool_id))
                if spool:
                    filament_id = spool.filament_id
            profiles = await self._read_spool_profiles_by_model(int(spool_id))
            stored_default = await self._read_spool_default_base_name(int(spool_id))
            default_base = self._infer_default_base_name(profiles, stored_default)
            if default_base:
                detail = await self._resolve_model_variant_detail(
                    default_base,
                    model_key,
                    spool_id=int(spool_id),
                    filament_id=filament_id,
                )
                if detail.get("mapped"):
                    profiles[model_key] = {"base_name": default_base, "source": "linked"}
                else:
                    profiles.pop(model_key, None)
            else:
                profiles.pop(model_key, None)
            await self._write_spool_profiles_by_model(int(spool_id), profiles)
            await self._resolve_and_mirror_profiles(
                profiles,
                spool_id=int(spool_id),
                filament_id=filament_id,
                spool=True,
            )
            coverage = await self._resolve_model_coverage_map(
                profiles,
                spool_id=int(spool_id),
                filament_id=filament_id,
            )
            return {
                "spool_id": int(spool_id),
                "model": model_key,
                "profiles_by_model": profiles,
                "coverage": coverage,
            }

        if not base_name and not code:
            raise ValueError("base_name or code is required")
        if code and not base_name:
            name = await self.resolve_preset_name(code)
            base_name = coerce_profile_base_name(name, code)
        if not base_name:
            raise ValueError("Could not determine base_name")

        self._local_profile_writes[int(spool_id)] = time.monotonic()

        filament_id = None
        async with async_session_maker() as db:
            spool = await db.get(Spool, int(spool_id))
            if not spool:
                raise ValueError(f"Spool {spool_id} not found")
            filament_id = spool.filament_id

        profiles = await self._read_spool_profiles_by_model(int(spool_id))
        profiles[model_key] = {
            "base_name": base_name,
            "source": "manual" if link_others else "override",
        }

        if link_others:
            by_model = await self._model_printer_map()
            for other in by_model:
                if other == model_key:
                    continue
                if other in profiles and profiles[other].get("source") == "override":
                    continue
                detail = await self._resolve_model_variant_detail(
                    base_name,
                    other,
                    spool_id=int(spool_id),
                    filament_id=filament_id,
                )
                if detail.get("mapped"):
                    profiles[other] = {"base_name": base_name, "source": "linked"}

        await self._write_spool_profiles_by_model(int(spool_id), profiles)
        variants_by_model, _ = await self._resolve_and_mirror_profiles(
            profiles,
            spool_id=int(spool_id),
            filament_id=filament_id,
            spool=True,
        )
        coverage = await self._resolve_model_coverage_map(
            profiles,
            spool_id=int(spool_id),
            filament_id=filament_id,
        )
        await self._mirror_spool_generic_indices_from_variants(
            int(spool_id), variants_by_model
        )
        await self._debounced_sync()

        return {
            "spool_id": int(spool_id),
            "model": model_key,
            "base_name": base_name,
            "code": variants_by_model.get(model_key),
            "profiles_by_model": profiles,
            "coverage": coverage,
            "variants": variants_by_model,
        }

    async def set_filament_profile_for_model(
        self,
        filament_id: int,
        model: str,
        *,
        base_name: str | None = None,
        code: str | None = None,
        link_others: bool = False,
        clear_override: bool = False,
        apply_to_existing: bool = False,
    ) -> dict[str, Any]:
        """Set slicer profile base name for one printer model on a filament."""
        if not filament_id:
            raise ValueError("filament_id is required")
        if not model:
            raise ValueError("model is required")
        model_key = model.strip().upper()

        if clear_override:
            self._local_profile_writes[int(filament_id)] = time.monotonic()
            profiles = await self._read_filament_profiles_by_model(int(filament_id))
            stored_default = await self._read_filament_default_base_name(
                int(filament_id)
            )
            default_base = self._infer_default_base_name(profiles, stored_default)
            if default_base:
                detail = await self._resolve_model_variant_detail(
                    default_base,
                    model_key,
                    filament_id=int(filament_id),
                )
                if detail.get("mapped"):
                    profiles[model_key] = {"base_name": default_base, "source": "linked"}
                else:
                    profiles.pop(model_key, None)
            else:
                profiles.pop(model_key, None)
            await self._write_filament_profiles_by_model(int(filament_id), profiles)
            await self._resolve_and_mirror_profiles(
                profiles, filament_id=int(filament_id), spool=False
            )
            coverage = await self._resolve_model_coverage_map(
                profiles, filament_id=int(filament_id)
            )
            return {
                "filament_id": int(filament_id),
                "model": model_key,
                "profiles_by_model": profiles,
                "coverage": coverage,
            }

        if not base_name and not code:
            raise ValueError("base_name or code is required")
        if code and not base_name:
            name = await self.resolve_preset_name(code)
            base_name = coerce_profile_base_name(name, code)
        if not base_name:
            raise ValueError("Could not determine base_name")

        profiles = await self._read_filament_profiles_by_model(int(filament_id))
        profiles[model_key] = {
            "base_name": base_name,
            "source": "manual" if link_others else "override",
        }

        if link_others:
            by_model = await self._model_printer_map()
            for other in by_model:
                if other == model_key:
                    continue
                if other in profiles and profiles[other].get("source") == "override":
                    continue
                detail = await self._resolve_model_variant_detail(
                    base_name, other, filament_id=int(filament_id)
                )
                if detail.get("mapped"):
                    profiles[other] = {"base_name": base_name, "source": "linked"}

        await self._write_filament_profiles_by_model(int(filament_id), profiles)
        variants_by_model, _ = await self._resolve_and_mirror_profiles(
            profiles, filament_id=int(filament_id), spool=False
        )
        coverage = await self._resolve_model_coverage_map(
            profiles, filament_id=int(filament_id)
        )

        rep_code = code or _uniform_variant_code(variants_by_model)
        if link_others and rep_code:
            generic = await self._resolved_ams_tray_code(rep_code)
            if generic:
                async with async_session_maker() as db:
                    changed = await self._upsert_filament_bambu_idx(
                        db, int(filament_id), generic
                    )
                    if changed:
                        await db.commit()

        applied = 0
        if apply_to_existing and variants_by_model:
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
                    await self._write_spool_profiles_by_model(sid, dict(profiles))
                    await self._mirror_pfus_by_model(
                        variants_by_model, spool_id=sid, spool=True
                    )
                    if link_others:
                        await self._upsert_spool_profile_base_name(sid, base_name)
                    await self._mirror_spool_generic_indices_from_variants(
                        sid, variants_by_model
                    )
                    uniform = _uniform_variant_code(variants_by_model)
                    if uniform:
                        await self._sync_spool_inventory_display(
                            sid, variants_by_model
                        )
                    applied += 1
                except Exception as e:
                    logger.warning(
                        f"apply_to_existing: failed for spool {sid}: {e}"
                    )
        else:
            await self._debounced_sync()

        return {
            "filament_id": int(filament_id),
            "model": model_key,
            "base_name": base_name,
            "code": variants_by_model.get(model_key) or rep_code,
            "profiles_by_model": profiles,
            "coverage": coverage,
            "variants": variants_by_model,
            "applied_to_existing": applied,
        }

    async def set_spool_profile(self, spool_id: int, code: str) -> dict[str, Any]:
        """Set slicer profile on a spool (wrapper → per-model with link_others)."""
        if not spool_id or not code:
            raise ValueError("spool_id and code are required")
        name = await self.resolve_preset_name(code)
        base_name = coerce_profile_base_name(name, code)
        if self._per_printer_profiles:
            return await self.set_default_spool_profile(
                int(spool_id), base_name=base_name, code=code
            )

        # Legacy single-profile path
        self._local_profile_writes[int(spool_id)] = time.monotonic()

        # FilaMan-Seite persistieren:
        #  - bambu_slicer_setting_id: das VOLLE Slicer-Preset (PFUS…@…nozzle) —
        #    der maßgebliche Slicer-Profil-Wert, der nach Bambuddy slicer_filament
        #    synchronisiert wird. Eigener Key, damit er NICHT in den AMS-configure-
        #    Call (setting_id) einfließt.
        #  - bambu_idx: der GENERISCHE AMS-Code (z.B. SUN20010/GFxx) für die
        #    physische AMS-Slot-Konfiguration. Aus dem vollen Preset aufgelöst,
        #    damit die AMS-Logik unverändert weiterläuft.
        #  - custom_fields (Spoolman-Sicht): voller Code + voller Name.
        fanout = await self._fan_out_spool_profile_variants(int(spool_id), code, name)
        generic = await self._resolved_ams_tray_code(code)
        if generic:
            await self._upsert_spool_bambu_idx(int(spool_id), generic)
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
        return {
            "code": code,
            "name": name,
            "bambuddy_spool_id": bb_id,
            **fanout,
        }

    async def set_filament_profile(
        self,
        filament_id: int,
        code: str,
        apply_to_existing: bool = False,
    ) -> dict[str, Any]:
        """Set default slicer profile on a filament (wrapper → per-model)."""
        if not filament_id or not code:
            raise ValueError("filament_id and code are required")

        name = await self.resolve_preset_name(code)
        base_name = coerce_profile_base_name(name, code)
        if self._per_printer_profiles:
            return await self.set_default_filament_profile(
                int(filament_id),
                base_name=base_name,
                code=code,
                apply_to_existing=apply_to_existing,
            )

        fanout = await self._fan_out_filament_profile_variants(
            int(filament_id), code, name
        )

        # bambu_idx = generischer AMS-Code (physische AMS-Konfiguration).
        # Never fall back to PFUS — that belongs only in bambu_slicer_setting_id.
        generic = await self._resolved_ams_tray_code(code)
        if generic:
            async with async_session_maker() as db:
                changed = await self._upsert_filament_bambu_idx(
                    db, int(filament_id), generic
                )
                if changed:
                    await db.commit()

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
            variant_map = fanout.get("variants") or {}
            base_name = fanout.get("base_name") or coerce_profile_base_name(name, code)
            for sid in spool_ids:
                try:
                    if self._per_printer_profiles and variant_map:
                        await self._upsert_spool_bambu_slicer_setting_id(sid, variant_map)
                        await self._upsert_spool_profile_base_name(sid, base_name)
                        generic_sp = await self._resolved_ams_tray_code(code)
                        if generic_sp:
                            await self._upsert_spool_bambu_idx(sid, generic_sp)
                        await self._upsert_spool_slicer_custom_fields(sid, code, name)
                        bb_id = await self._get_bambuddy_spool_id(sid)
                        if bb_id is not None and self._client:
                            payload: dict[str, Any] = {"slicer_filament": code}
                            if name:
                                payload["slicer_filament_name"] = name
                            await self._bb_patch(
                                f"/api/v1/inventory/spools/{bb_id}", payload
                            )
                    else:
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
            **fanout,
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

        PFUS/PFCN are rejected — they are slicer setting_ids, not AMS tray codes.

        Returns True wenn ein Wert neu geschrieben/geändert wurde, sonst False.
        Commit erfolgt durch den Aufrufer.
        """
        tray_info_idx_clean = _ams_tray_code(tray_info_idx)
        if not tray_info_idx_clean:
            if tray_info_idx and _is_cloud_setting_id(tray_info_idx):
                logger.warning(
                    f"Refusing to store cloud setting_id {tray_info_idx!r} as "
                    f"bambu_idx for filament {filament_id}"
                )
            return False
        tray_info_idx = tray_info_idx_clean
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

    async def _upsert_filament_bambu_slicer_setting_id(
        self, db: Any, filament_id: int, code: str | dict[int, str]
    ) -> bool:
        """Set ``bambu_slicer_setting_id`` for a filament per peer printer (open session).

        Accepts a single code or a ``{printer_id: pfus}`` mapping. Printers absent from
        a mapping are left unchanged.
        """
        if isinstance(code, str):
            mapping = {pid: code for pid in self._peer_printer_ids()}
        else:
            mapping = dict(code)

        printer_ids = self._peer_printer_ids()
        result = await db.execute(
            select(FilamentPrinterParam).where(
                FilamentPrinterParam.filament_id == filament_id,
                FilamentPrinterParam.printer_id.in_(printer_ids),
                FilamentPrinterParam.param_key == "bambu_slicer_setting_id",
            )
        )
        existing_by_pid = {p.printer_id: p for p in result.scalars().all()}
        changed = False
        for pid, pfus in mapping.items():
            if pid not in printer_ids or not pfus:
                continue
            existing = existing_by_pid.get(pid)
            if existing:
                if existing.param_value != pfus:
                    existing.param_value = pfus
                    changed = True
            else:
                db.add(
                    FilamentPrinterParam(
                        filament_id=filament_id,
                        printer_id=pid,
                        param_key="bambu_slicer_setting_id",
                        param_value=pfus,
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
        if _is_cloud_setting_id(tray_info_idx):
            logger.debug(
                f"Skip learning AMS profile: {tray_info_idx!r} is a setting_id, "
                f"not a tray code (spool {filaman_spool_id})"
            )
            return
        learn_key = (filaman_spool_id, tray_info_idx)
        if learn_key in self._learn_inflight:
            return  # identical learn already running (burst of MQTT events)
        self._learn_inflight.add(learn_key)
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
        finally:
            self._learn_inflight.discard(learn_key)

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
        self, filaman_spool_id: int, delta_g: float, *, source_event_key: str | None = None
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
                source_event_key=source_event_key,
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

        Multi-peer URLs: each driver builds the payload from ``self.printer_id`` params.
        The shared Bambuddy inventory ``slicer_filament`` is last-writer-wins across peer
        drivers; per-printer ``bambu_slicer_setting_id`` params and ``_send_assignment``
        are authoritative for each printer.
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
        # Volles Slicer-Preset (PFUS…@…nozzle): Spool-Override schlägt Filament-
        # Default. Dies ist der maßgebliche, vom Nutzer gewählte Slicer-Profil-
        # Wert (getrennt vom generischen AMS-Code in bambu_idx).
        full_setting_id = pp.get("bambu_slicer_setting_id") or fpp.get(
            "bambu_slicer_setting_id"
        )

        if existing_slicer:
            payload["slicer_filament"] = existing_slicer
            if existing_name:
                payload["slicer_filament_name"] = existing_name
        elif full_setting_id:
            payload["slicer_filament"] = full_setting_id
            name = self._cloud_presets_by_code.get(full_setting_id, {}).get(
                "name"
            ) or self._cloud_idmap_forward.get(full_setting_id)
            if name:
                payload["slicer_filament_name"] = name
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
                        # Differential sync: only PATCH when payload actually changed.
                        last_payload = self._url_last_payloads.get(
                            self._bambuddy_url, {}
                        ).get(fm_id)
                        if payload != last_payload:
                            await self._bb_patch(
                                f"/api/v1/inventory/spools/{bb_id}", payload
                            )
                            self._url_last_payloads.setdefault(
                                self._bambuddy_url, {}
                            )[fm_id] = dict(payload)
                            # Throttle only for actual API calls.
                            await asyncio.sleep(0.05)
                        # Always keep FilaMan's bambuddy_spool_id in sync (idempotent).
                        await self._store_bambuddy_id_db(fm_id, bb_id)
                    else:
                        response = await self._bb_post(
                            "/api/v1/inventory/spools", payload
                        )
                        bb_id = response["id"]
                        await self._store_bambuddy_id_db(fm_id, bb_id)
                        self._url_last_payloads.setdefault(
                            self._bambuddy_url, {}
                        )[fm_id] = dict(payload)
                        logger.info(
                            f"Created Bambuddy spool {bb_id} for FilaMan spool {fm_id}"
                        )
                        await asyncio.sleep(0.05)
                    # Must add unconditionally — orphan-deletion uses this set.
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

            # Keep Bambuddy spool_assignment rows in sync with FilaMan AMS locations.
            # Bambuddy can auto-unlink on fingerprint/empty-tray flaps without
            # notifying FilaMan; periodic sync restores missing links.
            await self._backfill_missing_ams_assignments()

        except Exception as e:
            self._last_sync_error = str(e)
            logger.error(f"Inventory sync failed for printer {self.printer_id}: {e}")
        finally:
            self._syncing = False

    async def _reflect_spool_profile(
        self, filaman_spool_id: int, existing_slicer: str | None
    ) -> None:
        """Spiegelt das in Bambuddy gesetzte Profil zurück nach FilaMan (LWW).

        Bambuddy inventory has a *single* shared ``slicer_filament`` per spool.
        With per-model profiles that value is one cloud variant (one model +
        nozzle). Reflect must therefore stay **model-scoped** and work for any
        connected mix (X1C+P1S, two A1s, three identical models, …):

          - update ``bambu_slicer_setting_id`` / ``bambu_idx`` only for printers
            whose model matches the parsed preset (never fan one model's
            inventory value onto other models)
          - when several printers share that model, re-resolve the base name
            per printer nozzle instead of cloning one PFUS onto all of them
          - only seed/update that model's ``profiles_by_model`` row; never
            replace override/manual rows or clear other models
          - do not run full-map ``_resolve_and_mirror_profiles`` (that can clear
            other models' params on a transient cloud miss)

        Guards: generic/empty codes ignored; pending assign pauses reflect;
        recent local FilaMan writes win (LWW, 300s).
        """
        if not existing_slicer or existing_slicer in _GENERIC_SLICER_ID_SET:
            return
        if self._pending_spool_id == filaman_spool_id:
            return
        last = self._local_profile_writes.get(filaman_spool_id)
        if last is not None and (time.monotonic() - last) < 300.0:
            return  # FilaMan war der jüngere Writer
        try:
            if self._per_printer_profiles:
                preset_name = await self.resolve_preset_name(existing_slicer)
                base_name = coerce_profile_base_name(preset_name, existing_slicer)
                _, parsed_model, _ = _parse_cloud_preset_name(preset_name or "")
                by_model = await self._model_printer_map()
                peers = self._peer_printer_ids()
                peer_set = set(peers)
                target_model = (parsed_model or "").upper()
                target_pids = (
                    [pid for pid in by_model.get(target_model, []) if pid in peer_set]
                    if target_model
                    else []
                )

                profiles = await self._read_spool_profiles_by_model(filaman_spool_id)
                profiles_changed = False
                existing_model = (
                    profiles.get(target_model) or {} if target_model else {}
                )
                explicit_model = existing_model.get("source") in (
                    "override",
                    "manual",
                )
                # Inventory must not undo an explicit per-model choice when the
                # reflected base name disagrees with that override.
                if (
                    explicit_model
                    and base_name
                    and (existing_model.get("base_name") or "") != base_name
                ):
                    return

                if base_name and target_model and target_model in by_model:
                    # Never overwrite an explicit per-model choice.
                    if not explicit_model:
                        new_entry = {
                            "base_name": base_name,
                            "source": "reflect",
                        }
                        if existing_model != new_entry:
                            profiles[target_model] = new_entry
                            profiles_changed = True
                elif base_name and not target_model and not profiles:
                    # Unknown model token: seed only models that actually map.
                    for model in by_model:
                        detail = await self._resolve_model_variant_detail(
                            base_name,
                            model,
                            spool_id=filaman_spool_id,
                        )
                        if detail.get("mapped"):
                            profiles[model] = {
                                "base_name": base_name,
                                "source": "reflect",
                            }
                            profiles_changed = True
                if profiles_changed:
                    await self._write_spool_profiles_by_model(
                        filaman_spool_id, profiles
                    )

                changed = False
                if target_pids and base_name and target_model:
                    setting_map: dict[int, str] = {}
                    idx_map: dict[int, str] = {}
                    for pid in target_pids:
                        nozzle = await self._nozzle_for_printer(
                            pid, spool_id=filaman_spool_id
                        )
                        detail = await self._resolve_model_variant_detail(
                            base_name,
                            target_model,
                            spool_id=filaman_spool_id,
                            nozzle_mm=nozzle,
                        )
                        code = str(detail.get("code") or existing_slicer)
                        setting_map[pid] = code
                        ams = await self._resolved_ams_tray_code(code)
                        if ams:
                            idx_map[pid] = ams
                    await self._upsert_spool_bambu_slicer_setting_id(
                        filaman_spool_id, setting_map
                    )
                    await self._upsert_spool_bambu_idx(filaman_spool_id, idx_map)
                    changed = True
                elif target_pids:
                    # Have model printers but no parseable base — write the raw
                    # inventory code only to that model group.
                    await self._upsert_spool_bambu_slicer_setting_id(
                        filaman_spool_id,
                        {pid: existing_slicer for pid in target_pids},
                    )
                    generic = await self._resolved_ams_tray_code(existing_slicer)
                    if generic:
                        await self._upsert_spool_bambu_idx(
                            filaman_spool_id,
                            {pid: generic for pid in target_pids},
                        )
                    changed = True
                elif len(peers) == 1:
                    # Single-printer URL: safe to write the only peer.
                    await self._upsert_spool_bambu_slicer_setting_id(
                        filaman_spool_id, {peers[0]: existing_slicer}
                    )
                    generic = await self._resolved_ams_tray_code(existing_slicer)
                    if generic:
                        await self._upsert_spool_bambu_idx(
                            filaman_spool_id, {peers[0]: generic}
                        )
                    changed = True
                # Default base name: fill when empty only. Never let one model's
                # inventory value continuously rewrite the spool default over an
                # explicit override for another connected model.
                if base_name and not is_cloud_setting_id(base_name):
                    current_default = await self._read_spool_default_base_name(
                        filaman_spool_id
                    )
                    if not current_default or is_cloud_setting_id(current_default):
                        await self._upsert_spool_profile_base_name(
                            filaman_spool_id, base_name
                        )
            else:
                changed = await self._upsert_spool_bambu_slicer_setting_id(
                    filaman_spool_id, existing_slicer
                )
                generic = await self._resolved_ams_tray_code(existing_slicer)
                if generic:
                    await self._upsert_spool_bambu_idx(filaman_spool_id, generic)
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
            # 3. Differential-sync cache leeren, damit der erzwungene Resync alle
            #    Spulen neu zu Bambuddy schickt (kein Skip durch alten Cache-Stand).
            self._url_last_payloads.pop(self._bambuddy_url, None)
            # 4. Neu synchronisieren (Lock bereits gehalten, direkt _do_sync_inner aufrufen)
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
        if self._pending_poll_task and not self._pending_poll_task.done():
            self._pending_poll_task.cancel()

        rfid_hex: str | None = None
        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, spool_id)
                if spool and spool.rfid_uid:
                    rfid_hex = self._to_hex_tag(spool.rfid_uid)
        except Exception as e:
            logger.warning(f"Could not load rfid_uid for pending spool {spool_id}: {e}")

        self._pending_spool_id = spool_id
        self._pending_filament_data = {**filament_data, "id": spool_id}
        self._pending_rfid_hex = rfid_hex
        await self._capture_pending_snapshot()

        effective_timeout = timeout_seconds if timeout_seconds is not None else 300
        self._pending_timer = asyncio.create_task(
            self._pending_timeout(effective_timeout)
        )
        self._pending_timer.add_done_callback(self._on_task_done)
        self._pending_poll_task = asyncio.create_task(self._pending_poll_loop())
        self._pending_poll_task.add_done_callback(self._on_task_done)
        logger.info(
            f"Pending spool {spool_id} set for printer {self.printer_id} "
            f"(rfid={rfid_hex}, timeout={effective_timeout}s)"
        )

    async def _pending_poll_loop(self) -> None:
        """Poll AMS status while a spool is pending insertion.

        Auto-assign previously relied solely on WebSocket slot events; missed
        events during reconnects or Bambuddy stalls meant configure never ran.
        """
        try:
            while self._running and self._pending_spool_id is not None:
                try:
                    await self._fetch_and_emit_status()
                except Exception as e:
                    logger.debug(f"Pending poll status fetch failed: {e}")
                if self._pending_spool_id is None:
                    break
                await asyncio.sleep(2)
        except asyncio.CancelledError:
            pass

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
        self._pending_slot_snapshot = None
        self._pending_timer = None
        if self._pending_poll_task and not self._pending_poll_task.done():
            self._pending_poll_task.cancel()
        self._pending_poll_task = None

    def _clear_pending(self) -> None:
        """Clear pending spool state and cancel timeout."""
        if self._pending_timer and not self._pending_timer.done():
            self._pending_timer.cancel()
        if self._pending_poll_task and not self._pending_poll_task.done():
            self._pending_poll_task.cancel()
        self._pending_spool_id = None
        self._pending_filament_data = None
        self._pending_rfid_hex = None
        self._pending_slot_snapshot = None
        self._pending_timer = None
        self._pending_poll_task = None

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

                # Keepalive tuning: Bambuddy's event loop periodically stalls for
                # several seconds while it polls the printer / rebuilds inventory,
                # so it occasionally fails to answer a keepalive ping within a
                # short window. A 10s ping_timeout caused frequent false
                # disconnects ("sent 1011 keepalive ping timeout"). We keep
                # sending pings (so genuinely dead TCP connections are still
                # detected and trigger a reconnect) but give the server a much
                # more generous window to respond before tearing the socket down.
                connected_at = 0.0
                async with websockets.connect(
                    uri,
                    additional_headers={"X-API-Key": self._api_key},
                    ping_interval=30,
                    ping_timeout=90,
                ) as ws:
                    self._ws_connected = True
                    connected_at = time.monotonic()
                    self._ws_last_connected_at = connected_at
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
                    # Bambuddy closes idle-looking client sockets with 1011
                    # ("keepalive ping timeout") whenever its own event loop
                    # stalls — common during active prints (MQTT K-profile
                    # polling, per-layer tray changes). A fixed 30s reconnect
                    # turned every hiccup into a 30s blind window. Reconnect
                    # fast after a stable session, but back off (1,2,4,…→cap)
                    # if connections keep dropping immediately, so we don't
                    # hammer Bambuddy while it is genuinely unavailable.
                    if connected_at and (time.monotonic() - connected_at) >= 30:
                        self._ws_reconnect_attempt = 0
                    delay = min(
                        2 ** self._ws_reconnect_attempt,
                        self._reconnect_interval,
                    )
                    self._ws_reconnect_attempt += 1
                    logger.warning(
                        f"WebSocket disconnected ({e}). "
                        f"Reconnecting in {delay}s…"
                    )
                    await asyncio.sleep(delay)

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
                await self._maybe_reconfigure_on_nozzle_change(data)
                print_state = self._parse_print_state(data)
                if self._pending_reconfigure_after_print and not self._is_printing(
                    print_state
                ):
                    self._pending_reconfigure_after_print = False
                    self._schedule_reconfigure_assigned_slots()

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
                event_id = str(event.get("event_id") or data.get("event_id") or "")
                await self._handle_print_complete(data, event_id=event_id)

        elif event_type == "spool_usage_logged":
            if event.get("printer_id") == self._bambuddy_printer_id:
                await self._handle_spool_usage_logged(event)

        elif event_type == "inventory_changed":
            # Refresh-on-save: ein Profil-/Inventory-Wechsel in Bambuddy stößt
            # einen (debounced) Sync an, damit FilaMan zeitnah nachzieht.
            # Grace period: skip for 120s after a WS reconnect. Bambuddy fires
            # a burst of inventory_changed events to every new WS client; acting
            # on them triggers a 50-spool PATCH storm that freezes Bambuddy's UI.
            # The startup _fetch_and_emit_status already got current state.
            since_connect = time.monotonic() - self._ws_last_connected_at
            if since_connect < 120.0:
                logger.debug(
                    f"Skipping inventory_changed sync: {since_connect:.0f}s since "
                    f"WS reconnect (grace window 120s)"
                )
            else:
                await self._debounced_sync()

    async def _handle_print_complete(self, data: dict, *, event_id: str = "") -> None:
        """Compatibility path for older Bambuddy print_complete payloads."""
        if self._spoolman_enabled:
            return
        # A run_event_id marks modern Bambuddy; only spool_usage_logged may consume it.
        if event_id:
            return
        weight_used = data.get("weight_used")
        if not weight_used:
            return
        fallback_key = event_id or f"legacy:{self._bambuddy_printer_id}:{data.get('subtask_name') or data.get('filename') or 'unknown'}"
        if isinstance(weight_used, dict):
            for slot_key, raw_weight in weight_used.items():
                weight_g = _float_or_none(raw_weight)
                if weight_g is None or not math.isfinite(weight_g) or weight_g <= 0:
                    continue
                filaman_spool_id = self._slot_to_filaman_spool.get(str(slot_key))
                if filaman_spool_id:
                    await self._report_consumption(filaman_spool_id, weight_g, source_event_key=f"{fallback_key}:{slot_key}")
        elif isinstance(weight_used, (int, float)) and math.isfinite(float(weight_used)) and float(weight_used) > 0:
            active_slots = list(self._slot_to_filaman_spool.items())
            if len(active_slots) == 1:
                await self._report_consumption(active_slots[0][1], float(weight_used), source_event_key=fallback_key)
            elif len(active_slots) > 1:
                logger.warning("legacy print_complete has multiple active slots; skipping ambiguous consumption")

    async def _handle_spool_usage_logged(self, event: dict) -> None:
        """Consume Bambuddy's already-calculated per-spool usage payload."""
        if self._spoolman_enabled:
            logger.info("Skipping internal consumption: Bambuddy Spoolman mode is enabled")
            return
        event_id = str(event.get("event_id") or "").strip()
        usage = event.get("usage")
        if not isinstance(usage, list):
            logger.warning("Ignoring malformed spool_usage_logged without usage list")
            return
        for item in usage:
            if not isinstance(item, dict):
                continue
            weight = _float_or_none(item.get("weight_used"))
            if weight is None or not math.isfinite(weight) or weight <= 0:
                continue
            bb_id = _int_or_none(item.get("spool_id"))
            ams_id = _int_or_none(item.get("ams_id"))
            tray_id = _int_or_none(item.get("tray_id"))
            filaman_id = await self._resolve_bambuddy_spool_id(bb_id) if bb_id is not None else None
            if filaman_id is None and ams_id is not None and tray_id is not None:
                filaman_id = self._slot_to_filaman_spool.get(f"{ams_id}-{tray_id}")
                if filaman_id:
                    logger.warning("Using slot fallback for Bambuddy spool %s (event=%s)", bb_id, event_id)
            if filaman_id is None:
                logger.warning("Unknown Bambuddy spool %s; skipping usage (event=%s)", bb_id, event_id)
                continue
            key = f"{event_id or 'legacy'}:{bb_id}:{ams_id if ams_id is not None else ''}:{tray_id if tray_id is not None else ''}"
            await self._report_consumption(filaman_id, weight, source_event_key=key)

    async def _resolve_bambuddy_spool_id(self, bambuddy_spool_id: int) -> int | None:
        async with async_session_maker() as db:
            result = await db.execute(select(SpoolPrinterParam).where(
                SpoolPrinterParam.printer_id == self.printer_id,
                SpoolPrinterParam.param_key == "bambuddy_spool_id",
                SpoolPrinterParam.param_value == str(bambuddy_spool_id),
            ))
            param = result.scalars().first()
            return param.spool_id if param else None

    async def _report_consumption(
        self, filaman_spool_id: int, delta_g: float, *, source_event_key: str | None = None
    ) -> None:
        try:
            await self._report_consumption_db(filaman_spool_id, delta_g, source_event_key=source_event_key)
        except Exception as e:
            logger.warning(f"Failed to report consumption for FilaMan spool {filaman_spool_id}: {e}")

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

        # Win the slot immediately: cancel any sticky reassert of the previous
        # occupant and bump the configure generation so a late SUN*/GF* MQTT
        # from that reassert cannot overwrite this assign.
        self._cancel_sticky_task(slot_key)
        assign_gen = self._bump_slot_configure_gen(slot_key)

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
            # Note: we intentionally do NOT record the spool's prior location as a
            # "home" to restore to later. A spool that leaves a slot is set to *no*
            # location (see _restore_spool_location). Remembering the prior location
            # was buggy when that location was itself a slot (a spool moved slot→slot
            # would be "restored" back into the stale slot instead of being freed).
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
                        "apply_to_printer": self._apply_to_printer,
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
        if not self._slot_configure_gen_matches(slot_key, assign_gen):
            logger.info(
                f"Skip configure for AMS {ams_id}/{tray_id}: assign superseded "
                f"before MQTT (gen {assign_gen})"
            )
            return
        if self._apply_to_printer:
            await self._send_assignment(
                ams_id, tray_id, filament_data, expected_gen=assign_gen
            )

        if filaman_spool_id:
            await self._update_spool_location(filaman_spool_id, ams_id, tray_id)
            self._slot_to_filaman_spool[slot_key] = filaman_spool_id

        _t = asyncio.create_task(_delayed_refetch())
        _t.add_done_callback(self._on_task_done)

        # One-shot verification: Bambuddy arms a deferred "configure on
        # insert" when an assignment is POSTed for an empty tray (sticky
        # empty-reassert of the previous occupant does exactly that). That
        # callback can fire milliseconds AFTER this assign's configure and
        # revert the tray to the old spool's colour/profile. Verify and
        # re-push once if the tray no longer reflects what we sent.
        if self._apply_to_printer and self._slot_configure_gen_matches(
            slot_key, assign_gen
        ):
            _vt = asyncio.create_task(
                self._verify_assign_configure(
                    ams_id, tray_id, dict(filament_data), assign_gen
                )
            )
            _vt.add_done_callback(self._on_task_done)

    async def _verify_assign_configure(
        self,
        ams_id: int,
        tray_id: int,
        filament_data: dict,
        assign_gen: int,
        delay: float = 6.0,
    ) -> None:
        """Re-push configure once if a stale deferred config overwrote ours.

        When the previous occupant's assignment is reasserted while a tray is
        empty (sticky empty-reassert), Bambuddy stores it as "pre-configured:
        will configure on insert". Inserting the NEXT spool then fires that
        stale callback milliseconds after the new spool's own configure,
        reverting the tray to the old spool's colour/profile. Check the live
        tray against what we sent and re-send once if it was overwritten.
        """
        await asyncio.sleep(delay)
        slot_key = f"{ams_id}-{tray_id}"
        if not self._slot_configure_gen_matches(slot_key, assign_gen):
            return  # a newer assign/configure owns the slot
        fm_id = _int_or_none(filament_data.get("id"))
        if fm_id and self._slot_to_filaman_spool.get(slot_key) != fm_id:
            return  # slot ownership changed
        slot = next(
            (s for s in self._current_slots if s["slot_index"] == slot_key),
            None,
        )
        if not slot or not slot.get("present"):
            return
        sent_color = self._norm_tray_color(filament_data.get("color"))
        live_color = self._norm_tray_color(slot.get("tray_color"))
        if not sent_color or not live_color or live_color == sent_color:
            return
        logger.warning(
            f"AMS {ams_id}/{tray_id} tray overwritten after assign of "
            f"spool {fm_id} (live colour {live_color!r} != sent "
            f"{sent_color!r}, likely a stale deferred configure) — "
            f"re-sending configure"
        )
        try:
            await self._send_assignment(
                ams_id, tray_id, filament_data, expected_gen=assign_gen
            )
        except Exception as e:
            logger.warning(
                f"Configure re-push failed for AMS {ams_id}/{tray_id}: {e}"
            )

    # -- Direkter configure-Call (Fallback) ----------------------------------

    async def _send_assignment(
        self,
        ams_id: int,
        tray_id: int,
        filament_data: dict,
        *,
        expected_gen: int | None = None,
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

        ``expected_gen``: when set, abort if another assign bumped the slot's
        configure generation (prevents stale sticky MQTT from winning a swap).
        """
        if not self._apply_to_printer:
            return
        if not self._client:
            logger.error("Cannot send assignment: HTTP client not initialized")
            return

        slot_key = f"{ams_id}-{tray_id}"
        if expected_gen is None:
            # Callers that don't pass a gen (late-NFC, manual) still claim the
            # slot so a concurrent sticky cannot overwrite them mid-flight.
            expected_gen = self._bump_slot_configure_gen(slot_key)
        elif not self._slot_configure_gen_matches(slot_key, expected_gen):
            logger.info(
                f"Skip configure AMS {ams_id}/{tray_id}: stale gen "
                f"(have={self._slot_configure_gen.get(slot_key, 0)} "
                f"expected={expected_gen})"
            )
            return

        # -- Farbe normalisieren: Bambuddy erwartet 8-stelliges RRGGBBAA --
        color = filament_data.get("color", "FFFFFFFF")
        if len(color) == 6:
            color = color + "FF"
        elif len(color) != 8:
            color = "FFFFFFFF"
        color = color.upper()

        # Resolve the slicer preset (setting_id) up front so it can also seed the
        # AMS material code below — important for a newly connected printer model
        # whose per-printer bambu_idx has not been mirrored yet.
        #
        # Policy: never clear a known PFUS/PFCN on the slot. Recover from spool
        # params / slot cache when resolve returns empty so Studio keeps the
        # custom ABS/ASA profile instead of falling back to tray_info_idx alone.
        cached_slot = self._slot_params_cache.get(slot_key, {})
        prior_setting = (
            (filament_data.get("bambu_slicer_setting_id") or "").strip()
            or (filament_data.get("bambu_setting_id") or "").strip()
            or (cached_slot.get("bambu_slicer_setting_id") or "").strip()
            or (cached_slot.get("bambu_setting_id") or "").strip()
        )
        setting_id = await self._resolve_setting_id_for_assign(filament_data)
        if not setting_id and _is_cloud_setting_id(prior_setting):
            setting_id = prior_setting
            logger.info(
                f"Preserving prior PFUS setting_id {setting_id!r} for slot "
                f"{ams_id}/{tray_id} (resolve returned empty)"
            )

        # -- Bambu Material Index (slicer_filament = tray_info_idx) --
        material_raw = filament_data.get("material_type", "PLA")
        material = _normalize_tray_type(material_raw)  # z.B. "PLA+" → "PLA"

        # Priority 1: a previously resolved/learned AMS code in the filament's
        # printer params (durable, fed in via enrich_filament_data as bambu_idx).
        # Ignore PFUS/PFCN wrongly stored as bambu_idx (they are setting_ids).
        bambu_idx_hint = _ams_tray_code(
            filament_data.get("bambu_idx") or filament_data.get("bambu_tray_idx")
        )
        raw_idx = filament_data.get("bambu_idx") or filament_data.get("bambu_tray_idx")
        if raw_idx and _is_cloud_setting_id(str(raw_idx)):
            if not setting_id:
                setting_id = str(raw_idx)
            bambu_idx_hint = None

        # Priority 2: full cloud preset (bambu_slicer_setting_id, the variant just
        # resolved for this model, or custom_fields) → generic AMS code (e.g.
        # "SUN20012").
        if not bambu_idx_hint:
            fm_spool_id = _int_or_none(filament_data.get("id"))
            preset_id = filament_data.get("bambu_slicer_setting_id") or setting_id
            if not preset_id and fm_spool_id:
                preset_id = await self._spool_cloud_preset(fm_spool_id)
            if preset_id:
                resolved = await self._resolve_cloud_preset(str(preset_id))
                if resolved:
                    bambu_idx_hint = resolved
                    if fm_spool_id:
                        await self._persist_filament_bambu_idx(fm_spool_id, resolved)
                    logger.info(
                        f"Using cloud preset {preset_id!r} → {resolved!r} "
                        f"for slot {ams_id}/{tray_id}"
                    )

        # Priority 3: Bambuddy inventory spool's slicer_filament — also a cloud
        # preset (PFUS…), must be resolved to an AMS code; never pass PFUS through
        # to _resolve_slicer_id (mixed-case PFUS falls back to generic GFL99).
        if not bambu_idx_hint:
            bb_spool_id = _int_or_none(filament_data.get("bambuddy_spool_id"))
            if bb_spool_id and self._client:
                try:
                    bb_spool = await self._bb_get(f"/api/v1/inventory/spools/{bb_spool_id}")
                    preset = bb_spool.get("slicer_filament") or None
                    if preset:
                        resolved = await self._resolve_cloud_preset(str(preset))
                        if resolved:
                            bambu_idx_hint = resolved
                            fm_spool_id = _int_or_none(filament_data.get("id"))
                            if fm_spool_id:
                                await self._persist_filament_bambu_idx(
                                    fm_spool_id, resolved
                                )
                            logger.info(
                                f"Using Bambuddy spool {bb_spool_id} preset "
                                f"{preset!r} → {resolved!r} for slot {ams_id}/{tray_id}"
                            )
                except Exception as e:
                    logger.debug(f"Could not fetch Bambuddy spool {bb_spool_id}: {e}")

        # When no model-specific slicer preset resolved for this printer
        # (setting_id is empty), the slot would otherwise rely on whatever the raw
        # material code maps to — which can be a blank/unrecognized profile in
        # Bambu Studio for third-party codes (no proper settings applied). Honor
        # the Slicer Profile Fallback setting and pin the AMS material code to a
        # guaranteed built-in system profile (Generic <material>, or the Bambu-
        # brand basic) so a valid, named profile is always used. Models that DO
        # have a matching profile keep their resolved setting_id and are untouched.
        if not setting_id and self._per_printer_profiles:
            fallback_pref = await self._get_unmatched_profile_fallback()
            generic_code = _GENERIC_SLICER_IDS.get(material.upper())
            if fallback_pref == "bambu":
                fallback_code = (
                    _BAMBU_BRAND_SLICER_IDS.get(material.upper()) or generic_code
                )
            else:
                fallback_code = generic_code
            if fallback_code and fallback_code != bambu_idx_hint:
                logger.info(
                    f"No per-model slicer profile for {material!r}; applying "
                    f"{fallback_pref} fallback {fallback_code!r} "
                    f"(was {bambu_idx_hint or 'unset'}) for slot {ams_id}/{tray_id}"
                )
                bambu_idx_hint = fallback_code

        slicer_filament = _resolve_slicer_id(bambu_idx_hint, material)
        # Belt-and-suspenders: never send PFUS as tray_info_idx.
        if _is_cloud_setting_id(slicer_filament):
            bad_tray = slicer_filament
            if not setting_id:
                setting_id = bad_tray
            slicer_filament = _GENERIC_SLICER_IDS.get(material.upper(), "GFL99")
            logger.warning(
                f"Refusing tray_info_idx={bad_tray!r} (cloud setting_id) for "
                f"slot {ams_id}/{tray_id}; using AMS fallback {slicer_filament!r}"
            )

        # Final guard: never POST an empty setting_id over a known PFUS on this slot.
        if not setting_id and _is_cloud_setting_id(prior_setting):
            setting_id = prior_setting
            logger.warning(
                f"Refusing to clear PFUS on slot {ams_id}/{tray_id}; "
                f"re-sending setting_id={setting_id!r}"
            )

        # tray_sub_brands: human-readable filament name on the AMS slot.
        # Studio's Device tab often shows this string. For custom ABS/ASA the
        # tray code is GFB00/GFB01 ("Bambu ABS/ASA") — if we use that label,
        # Studio shows "Bambu ASA" even when setting_id is the correct PFUS.
        # Prefer the cloud preset base name (e.g. "Overture ASA") instead.
        #
        # Do NOT prefer material_subgroup here when a PFUS is known: values like
        # "basic"/"matte" are finish tags, not the profile name Studio should
        # display.
        await self._get_cloud_idmap_reverse()
        tray_sub_brands = ""
        if _is_cloud_setting_id(setting_id):
            preset_name = await self.resolve_preset_name(setting_id)
            if preset_name:
                tray_sub_brands = _extract_profile_base_name(preset_name)
        if not tray_sub_brands:
            stored_base = (filament_data.get("bambu_profile_base_name") or "").strip()
            if stored_base:
                tray_sub_brands = stored_base
            else:
                stored_full = (
                    filament_data.get("bambu_slicer_filament_name") or ""
                ).strip()
                if stored_full:
                    tray_sub_brands = _extract_profile_base_name(stored_full)
        if not tray_sub_brands:
            # Finish/subgroup only as a last resort before the tray-code label.
            subgroup = (filament_data.get("material_subgroup") or "").strip()
            if subgroup and subgroup.lower() not in {
                "basic",
                "standard",
                "generic",
                material.lower(),
                material_raw.lower(),
            }:
                tray_sub_brands = subgroup
        if not tray_sub_brands:
            tray_sub_brands = (
                _FILAMENT_IDX_TO_NAME.get(slicer_filament)
                or self._cloud_idmap_forward.get(slicer_filament)
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
            "setting_id": setting_id,
            "kprofile_filament_id": slicer_filament,
            "kprofile_setting_id": setting_id,
            "k_value": k_value,  # 0.0 = skip
        }

        # Final race check after awaits (cloud resolve / Bambuddy GET): a newer
        # assign or sticky cancel may have claimed the slot while we were resolving.
        if not self._slot_configure_gen_matches(slot_key, expected_gen):
            logger.info(
                f"Skip configure POST AMS {ams_id}/{tray_id}: superseded after "
                f"resolve (tray_info_idx={slicer_filament!r}, gen={expected_gen})"
            )
            return

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
            # Prefer keeping a known PFUS in cache even if this configure sent
            # only temps (empty setting_id) — avoids later late-NFC wipes.
            cached_setting = setting_id or prior_setting or None
            self._slot_params_cache[f"{ams_id}-{tray_id}"] = {
                "nozzle_temp_min": nozzle_temp_min,
                "nozzle_temp_max": nozzle_temp_max,
                "bambu_setting_id": cached_setting or setting_id,
                "bambu_slicer_setting_id": (
                    filament_data.get("bambu_slicer_setting_id")
                    or cached_setting
                    or None
                ),
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
                f"(material={material}, slicer_filament={slicer_filament}, "
                f"setting_id={setting_id!r})"
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

    async def _is_own_slot_location(self, db, location_id: int | None) -> bool:
        """True if location_id is an AMS-slot location managed by THIS printer.

        Printer-scoped on purpose. Each driver tracks only its own
        _slot_to_filaman_spool, so when a spool moves from printer A to printer B
        the empty-slot cleanup on printer A must NOT clear the spool's freshly
        created printer-B assignment. We only clear a spool whose location is one
        of *our* slots.
        """
        if location_id is None:
            return False
        loc = await db.get(Location, location_id)
        if not loc:
            return False
        cf = loc.custom_fields
        if isinstance(cf, str):
            try:
                cf = json.loads(cf)
            except Exception:
                cf = {}
        if not isinstance(cf, dict) or cf.get("managed_by") != "bambuddy_plugin":
            return False
        return str(cf.get("printer_id")) == str(self.printer_id)

    async def _restore_spool_location(self, filaman_spool_id: int) -> None:
        """Clears a spool's location when it is replaced on an AMS tray.

        Physical empty trays no longer clear locations (sticky assignment). This
        runs when another spool is explicitly assigned to the same slot (scan or
        manual assign), so the displaced spool is set to *no* location. Only
        locations this plugin manages (AMS slots) are cleared.
        """
        # Stale original-location bookkeeping is no longer used; drop any leftover.
        self._spool_original_location.pop(filaman_spool_id, None)

        # Still assigned to another slot → that assignment wins, clear nothing.
        if filaman_spool_id in list(self._slot_to_filaman_spool.values()):
            return

        try:
            async with async_session_maker() as db:
                spool = await db.get(Spool, filaman_spool_id)
                if (
                    spool
                    and spool.location_id is not None
                    and await self._is_own_slot_location(db, spool.location_id)
                ):
                    await SpoolService(db).move_location(
                        spool,
                        None,  # no location — replaced by another spool on the AMS
                        datetime.now(timezone.utc),
                        source="driver",
                        note="Removed from AMS tray (replaced)",
                    )
            # Persistierten (jetzt ungenutzten) Original-Location-Eintrag aufräumen
            await self._delete_original_location_db(filaman_spool_id)
        except Exception as e:
            logger.warning(f"Failed to clear spool {filaman_spool_id} location: {e}")

    def _bump_slot_configure_gen(self, slot_key: str) -> int:
        """Invalidate in-flight sticky/late configures for this slot."""
        nxt = self._slot_configure_gen.get(slot_key, 0) + 1
        self._slot_configure_gen[slot_key] = nxt
        return nxt

    def _slot_configure_gen_matches(self, slot_key: str, expected_gen: int) -> bool:
        return self._slot_configure_gen.get(slot_key, 0) == expected_gen

    def _cancel_sticky_task(self, slot_key: str) -> None:
        task = self._sticky_tasks.pop(slot_key, None)
        if task is not None and not task.done():
            task.cancel()

    @staticmethod
    def _norm_tray_color(color: str | None) -> str:
        c = (color or "").strip().lstrip("#").upper()
        if len(c) == 8:
            return c[:6]
        if len(c) == 6:
            return c
        return c

    @staticmethod
    def _norm_material(material: str | None) -> str:
        return (material or "").strip().upper().replace(" ", "")

    async def _sticky_still_matches_tray(
        self, fm_id: int, tray: dict[str, Any] | None
    ) -> bool:
        """True if the sticky spool plausibly matches the live tray (RFID/color/type).

        Used to abort sticky configure when a *different* spool was inserted into
        a slot that still had the previous sticky owner in memory.
        """
        if not tray:
            return True  # empty-path reassert; no tray content to compare

        tray_tag = (tray.get("tag_uid") or "").strip().upper().replace(":", "")
        if tray_tag in ("", "0000000000000000"):
            tray_tag = ""
        tray_uuid = (tray.get("tray_uuid") or "").strip().upper()
        if tray_uuid in ("", "00000000000000000000000000000000"):
            tray_uuid = ""
        tray_type = self._norm_material(tray.get("tray_type"))
        tray_color = self._norm_tray_color(tray.get("tray_color"))

        try:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(Spool)
                    .where(Spool.id == fm_id)
                    .options(
                        selectinload(Spool.filament)
                        .selectinload(Filament.filament_colors)
                        .selectinload(FilamentColor.color),
                    )
                )
                spool = result.scalar_one_or_none()
        except Exception as e:
            logger.debug(f"Sticky match check failed for FM#{fm_id}: {e}")
            return True  # fail open — preserve sticky recovery

        if not spool:
            return False

        # FilaMan spools may carry two chips (one per side, spools.rfid_uid_2);
        # the AMS reports whichever side faces the reader, so accept either.
        # getattr keeps this working against a FilaMan without the column.
        spool_tags = {
            self._to_hex_tag(uid)
            for uid in (spool.rfid_uid, getattr(spool, "rfid_uid_2", None))
            if uid
        }
        spool_tags.discard("")
        spool_tag = next(iter(spool_tags), "")
        if tray_tag and spool_tags:
            return tray_tag in spool_tags
        # Bambu RFID uuid path when tag_uid absent
        if tray_uuid and spool_tag and len(tray_uuid) >= 16:
            # Can't reliably map uuid↔FilaMan rfid without Bambuddy; fall through
            # to color/type.
            pass

        fil = spool.filament
        if not fil:
            return True
        spool_type = self._norm_material(fil.material_type)
        spool_color = ""
        colors = sorted(fil.filament_colors or [], key=lambda fc: fc.position)
        if colors and colors[0].color and colors[0].color.hex_code:
            spool_color = self._norm_tray_color(colors[0].color.hex_code)

        if tray_type and spool_type and tray_type != spool_type:
            # PLA vs PLA+ often differ only by subgroup — treat base family match
            # as ok when one contains the other (PLA+ / PLA).
            if not (
                tray_type.startswith(spool_type)
                or spool_type.startswith(tray_type)
                or tray_type.replace("+", "") == spool_type.replace("+", "")
            ):
                return False
        if tray_color and spool_color and tray_color != spool_color:
            return False
        return True

    async def _reassert_sticky_assignment(
        self,
        ams_id: int,
        tray_id: int,
        *,
        configure: bool = False,
        expected_gen: int | None = None,
        tray: dict[str, Any] | None = None,
    ) -> None:
        """Re-POST Bambuddy assignment (and optionally configure) for a sticky slot.

        Bambuddy auto-unlinks ``spool_assignment`` on empty trays / fingerprint
        flaps. Sticky mode keeps FilaMan ownership and reasserts the inventory
        link so usage tracking and the Bambuddy UI stay aligned until a scanned
        or explicit assign replaces the occupant.

        A pending RFID scan, a newer slot configure generation, or a tray that
        no longer matches the sticky spool aborts this path — that is the
        swap race where the previous occupant's SUN*/GF* MQTT was winning.
        """
        slot_key = f"{ams_id}-{tray_id}"
        if expected_gen is None:
            expected_gen = self._slot_configure_gen.get(slot_key, 0)

        # Let concurrent pending-assign / RFID paths bump gen first.
        try:
            await asyncio.sleep(self._STICKY_REASSERT_SETTLE)
        except asyncio.CancelledError:
            raise

        if self._pending_spool_id is not None:
            logger.info(
                f"Skip sticky reassert AMS {ams_id}/{tray_id}: pending scan "
                f"FM#{self._pending_spool_id} active"
            )
            return
        if not self._slot_configure_gen_matches(slot_key, expected_gen):
            logger.info(
                f"Skip sticky reassert AMS {ams_id}/{tray_id}: superseded by "
                f"newer configure gen "
                f"(have={self._slot_configure_gen.get(slot_key, 0)} "
                f"expected={expected_gen})"
            )
            return

        # Empty-path reassert: if the tray is occupied again after settle, a
        # swap is in progress — do not re-POST the previous occupant.
        if not configure:
            cur = next(
                (s for s in self._current_slots if s["slot_index"] == slot_key),
                None,
            )
            if cur and cur.get("present"):
                logger.info(
                    f"Skip sticky empty-reassert AMS {ams_id}/{tray_id}: "
                    f"slot occupied again during settle (swap in progress)"
                )
                return

        now = time.monotonic()
        last = self._sticky_reassert_ts.get(slot_key, 0.0)
        if (now - last) < self._STICKY_REASSERT_COOLDOWN:
            return
        self._sticky_reassert_ts[slot_key] = now

        fm_id = self._slot_to_filaman_spool.get(slot_key)
        if not fm_id:
            return

        if configure:
            # Prefer live tray from the latest status if caller didn't pass one
            # (empty→present path always passes tray).
            live_tray = tray
            if live_tray is None:
                prev = next(
                    (s for s in self._current_slots if s["slot_index"] == slot_key),
                    None,
                )
                if prev and prev.get("present"):
                    live_tray = {
                        "tray_type": prev.get("tray_type"),
                        "tray_color": prev.get("tray_color"),
                        "tag_uid": prev.get("tag_uid"),
                        "tray_uuid": prev.get("tray_uuid"),
                    }
            if not await self._sticky_still_matches_tray(fm_id, live_tray):
                logger.info(
                    f"Clearing sticky FM#{fm_id} on AMS {ams_id}/{tray_id}: "
                    f"live tray no longer matches previous occupant "
                    f"(avoid stale ams_filament_setting)"
                )
                self._slot_to_filaman_spool.pop(slot_key, None)
                self._sticky_reassert_ts.pop(slot_key, None)
                return

        if not self._slot_configure_gen_matches(slot_key, expected_gen):
            return

        bb_spool_id = await self._get_bambuddy_spool_id(fm_id)
        if not self._slot_configure_gen_matches(slot_key, expected_gen):
            return
        if self._pending_spool_id is not None:
            return

        if bb_spool_id and self._client and self._sync_enabled:
            try:
                response = await self._bb_post(
                    "/api/v1/inventory/assignments",
                    {
                        "spool_id": bb_spool_id,
                        "printer_id": self._bambuddy_printer_id,
                        "ams_id": ams_id,
                        "tray_id": tray_id,
                        "apply_to_printer": self._apply_to_printer,
                    },
                )
                logger.info(
                    f"Sticky reassert FM#{fm_id}/BB#{bb_spool_id} → "
                    f"AMS {ams_id}/{tray_id} "
                    f"(configured={response.get('configured')}, "
                    f"configure_push={configure})"
                )
            except Exception as e:
                logger.warning(
                    f"Sticky assignment reassert failed for FM#{fm_id} "
                    f"AMS {ams_id}/{tray_id}: {e}"
                )

        if not self._apply_to_printer or not configure:
            return
        if not self._slot_configure_gen_matches(slot_key, expected_gen):
            logger.info(
                f"Skip sticky configure AMS {ams_id}/{tray_id}: gen superseded "
                f"after assignment POST"
            )
            return
        if self._pending_spool_id is not None:
            return

        try:
            from app.plugins.manager import plugin_manager

            filament_data = await plugin_manager.enrich_filament_data(
                fm_id, self.printer_id, {"id": fm_id}
            )
            if not self._slot_configure_gen_matches(slot_key, expected_gen):
                return
            await self._send_assignment(
                ams_id, tray_id, filament_data, expected_gen=expected_gen
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.warning(
                f"Sticky configure reassert failed for FM#{fm_id} "
                f"AMS {ams_id}/{tray_id}: {e}"
            )

    def _schedule_sticky_reassert(
        self,
        ams_id: int,
        tray_id: int,
        *,
        configure: bool,
        tray: dict[str, Any] | None = None,
    ) -> None:
        slot_key = f"{ams_id}-{tray_id}"
        if self._pending_spool_id is not None:
            logger.info(
                f"Skip scheduling sticky reassert AMS {ams_id}/{tray_id}: "
                f"pending scan FM#{self._pending_spool_id}"
            )
            return
        self._cancel_sticky_task(slot_key)
        expected_gen = self._slot_configure_gen.get(slot_key, 0)

        async def _run() -> None:
            try:
                await self._reassert_sticky_assignment(
                    ams_id,
                    tray_id,
                    configure=configure,
                    expected_gen=expected_gen,
                    tray=tray,
                )
            finally:
                if self._sticky_tasks.get(slot_key) is asyncio.current_task():
                    self._sticky_tasks.pop(slot_key, None)

        _t = asyncio.create_task(_run())
        self._sticky_tasks[slot_key] = _t
        _t.add_done_callback(self._on_task_done)

    async def _reconcile_sticky_slot_map(self) -> None:
        """Drop in-memory sticky owners that no longer have this AMS location in DB.

        Manual location clears (FilaMan UI) do not go through empty-tray handling,
        so without this the driver would keep reasserting a stale sticky spool.
        """
        if not self._slot_to_filaman_spool:
            return
        prefix = f"bambuddy_{self.printer_id}_"
        try:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(Spool.id, Location.identifier)
                    .outerjoin(Location, Spool.location_id == Location.id)
                    .where(Spool.id.in_(list(self._slot_to_filaman_spool.values())))
                )
                loc_by_spool: dict[int, str | None] = {
                    sid: ident for sid, ident in result.all()
                }
        except Exception as e:
            logger.debug(f"Sticky map reconcile failed: {e}")
            return

        dropped: list[str] = []
        for slot_key, fm_id in list(self._slot_to_filaman_spool.items()):
            parts = slot_key.split("-", 1)
            if len(parts) != 2:
                continue
            try:
                ams_id, tray_id = int(parts[0]), int(parts[1])
            except ValueError:
                continue
            expected = f"{prefix}{ams_id}_{tray_id}"
            ident = loc_by_spool.get(fm_id)
            if ident != expected:
                self._slot_to_filaman_spool.pop(slot_key, None)
                self._sticky_reassert_ts.pop(slot_key, None)
                dropped.append(f"{slot_key}(FM#{fm_id})")

        if dropped:
            logger.info(
                f"Cleared stale sticky slot owner(s) on printer {self.printer_id}: "
                + ", ".join(dropped)
            )

    async def _reconfigure_slot_with_profile(
        self, ams_id: int, tray_id: int, tray_info_idx: str, tray: dict
    ) -> None:
        """Re-push slot config when AMS NFC read completes with a specific profile.

        Called when _process_slots detects a tray_info_idx transition from
        generic/empty → specific (e.g. "" → "GFA01") on an already-assigned slot.

        Prefer the assigned spool's persisted printer params (esp.
        ``bambu_slicer_setting_id``) over ``_slot_params_cache``. Manual Bambuddy
        assigns leave the cache empty, and rebuilding from cache alone wipes the
        PFUS setting_id (Studio then shows Bambu ABS from tray_info_idx=GFB00).

        Skip the MQTT push entirely when we still have no setting_id after spool
        load — an empty push is what made this ABS slot an outlier vs SUN*-coded
        PLA/PETG slots that never lost their profile.
        """
        slot_key = f"{ams_id}-{tray_id}"
        cached = self._slot_params_cache.get(slot_key, {})
        fm_id = self._slot_to_filaman_spool.get(slot_key)

        filament_data: dict[str, Any] = {
            "color": tray.get("tray_color", "FFFFFFFF"),
            "material_type": tray.get("tray_type", "PLA"),
            "bambu_idx": tray_info_idx,
            "bambu_nozzle_temp_min": cached.get("nozzle_temp_min"),
            "bambu_nozzle_temp_max": cached.get("nozzle_temp_max"),
            "bambu_k_value": cached.get("bambu_k_value"),
            "bambu_cali_idx": cached.get("bambu_cali_idx"),
            "bambu_setting_id": cached.get("bambu_setting_id"),
            "bambu_slicer_setting_id": cached.get("bambu_slicer_setting_id"),
        }

        if fm_id:
            try:
                spool_data = await self._filament_data_for_spool(int(fm_id))
                # Spool/filament params win for slicer setting + temps/cali.
                # Keep the live tray AMS material code and color/type from the
                # tray report so we do not fight Bambuddy's NFC read.
                for key, value in spool_data.items():
                    if value is None or value == "":
                        continue
                    if key in ("color", "material_type", "bambu_idx", "bambu_tray_idx"):
                        continue
                    filament_data[key] = value
                filament_data["id"] = int(fm_id)
                if spool_data.get("filament_id") is not None:
                    filament_data["filament_id"] = spool_data["filament_id"]
                if tray.get("tray_color"):
                    filament_data["color"] = tray["tray_color"]
                if tray.get("tray_type"):
                    filament_data["material_type"] = tray["tray_type"]
                filament_data["bambu_idx"] = tray_info_idx
            except Exception as e:
                logger.warning(
                    f"Late NFC reconfigure: could not load spool {fm_id} for "
                    f"slot {slot_key}, using cache only: {e}"
                )

        resolved_setting = await self._resolve_setting_id_for_assign(filament_data)
        if not resolved_setting and not (
            filament_data.get("bambu_slicer_setting_id")
            or filament_data.get("bambu_setting_id")
        ):
            logger.warning(
                f"Late NFC reconfigure skipped for slot {slot_key}: "
                f"no setting_id available (would wipe Studio profile; "
                f"tray_info_idx={tray_info_idx!r})"
            )
            return

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
            slot_key = f"{ams_id}-{tray_id}"

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

                # 2b. Stale occupant(s) aus DB evakuieren, bevor die neue Spule
                # zugewiesen wird. Die In-Memory-Map (_slot_to_filaman_spool)
                # überlebt keinen Driver-/Container-Neustart, daher darf sich die
                # Eviction NICHT allein auf sie verlassen — sonst bleiben alte
                # Spulen an einem AMS-Slot "kleben" und mehrere Spulen zeigen auf
                # dieselbe Location (nur eine Spule kann physisch in einem Slot
                # stecken). Die DB ist hier die verlässliche Quelle der Wahrheit.
                stale_result = await db.execute(
                    select(Spool).where(
                        Spool.location_id == location.id,
                        Spool.id != filaman_spool_id,
                    )
                )
                evicted_stale = False
                for stale_spool in stale_result.scalars().all():
                    await SpoolService(db).move_location(
                        stale_spool,
                        None,
                        datetime.now(timezone.utc),
                        source="driver",
                        note="Removed from AMS tray (replaced)",
                    )
                    logger.info(
                        f"Evicted stale spool {stale_spool.id} from location "
                        f"'{slot_location_name}' (replaced by spool "
                        f"{filaman_spool_id})"
                    )
                    self._slot_to_filaman_spool.pop(slot_key, None)
                    evicted_stale = True

                # 3. Spule zur Location bewegen (wenn nicht bereits dort)
                spool = await db.get(Spool, filaman_spool_id)
                if not spool:
                    logger.warning(
                        f"Spool {filaman_spool_id} not found, cannot update location"
                    )
                    return

                # Already at this slot without replacing anyone: no-op (sticky /
                # reconfigure). If we just evicted a stale occupant, still emit
                # "Assigned to …" even when location_id was pre-set (e.g. duplicate
                # copied an AMS location) so the spool log reflects the real place.
                if spool.location_id == location.id and not evicted_stale:
                    logger.debug(
                        f"Spool {filaman_spool_id} already at location '{slot_location_name}'"
                    )
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

    async def _backfill_missing_ams_assignments(self) -> None:
        """POST Bambuddy assignments for FilaMan AMS locations that lack one.

        Bambuddy deletes ``spool_assignment`` rows on Spoolman-mode toggles and on
        AMS fingerprint / empty-tray auto-unlink — without clearing FilaMan's
        ``location_id``. Inventory sync alone does not recreate those rows.
        This posts only **missing** slot links for this printer so usage tracking
        and the Bambuddy UI stay aligned without re-POSTing every sync.
        """
        if not self._client or not self._bambuddy_printer_id or not self._sync_enabled:
            return

        prefix = f"bambuddy_{self.printer_id}_"
        try:
            async with async_session_maker() as db:
                result = await db.execute(
                    select(Spool, Location, SpoolPrinterParam)
                    .join(Location, Spool.location_id == Location.id)
                    .join(SpoolStatus, Spool.status_id == SpoolStatus.id)
                    .join(
                        SpoolPrinterParam,
                        (SpoolPrinterParam.spool_id == Spool.id)
                        & (SpoolPrinterParam.printer_id == self.printer_id)
                        & (SpoolPrinterParam.param_key == "bambuddy_spool_id"),
                    )
                    .where(
                        SpoolStatus.key != "archived",
                        Location.identifier.like(f"{prefix}%"),
                    )
                )
                rows = result.all()
        except Exception as e:
            logger.warning(f"Assignment backfill: failed to load FilaMan AMS locs: {e}")
            return

        if not rows:
            return

        targets: list[tuple[int, int, int, int]] = []  # fm_id, bb_id, ams, tray
        for spool, loc, param in rows:
            ident = loc.identifier or ""
            parts = ident.split("_")
            if len(parts) != 4 or parts[0] != "bambuddy":
                continue
            try:
                ams_id, tray_id = int(parts[2]), int(parts[3])
                bb_spool_id = int(param.param_value)
            except (TypeError, ValueError):
                continue
            targets.append((spool.id, bb_spool_id, ams_id, tray_id))

        if not targets:
            return

        try:
            existing = await self._bb_get(
                "/api/v1/inventory/assignments",
                params={"printer_id": self._bambuddy_printer_id},
            )
        except Exception as e:
            logger.warning(f"Assignment backfill: failed to list Bambuddy assigns: {e}")
            return

        have: set[tuple[int, int]] = set()
        if isinstance(existing, list):
            for a in existing:
                try:
                    have.add((int(a["ams_id"]), int(a["tray_id"])))
                except (KeyError, TypeError, ValueError):
                    continue

        posted = 0
        for fm_id, bb_spool_id, ams_id, tray_id in targets:
            if (ams_id, tray_id) in have:
                continue
            try:
                await self._bb_post(
                    "/api/v1/inventory/assignments",
                    {
                        "spool_id": bb_spool_id,
                        "printer_id": self._bambuddy_printer_id,
                        "ams_id": ams_id,
                        "tray_id": tray_id,
                        "apply_to_printer": self._apply_to_printer,
                    },
                )
                self._slot_to_filaman_spool[f"{ams_id}-{tray_id}"] = fm_id
                posted += 1
                logger.info(
                    f"Backfilled Bambuddy assignment FM#{fm_id}/BB#{bb_spool_id} "
                    f"→ AMS {ams_id}/{tray_id}"
                )
                await asyncio.sleep(0.1)
            except Exception as e:
                logger.warning(
                    f"Assignment backfill failed for FM#{fm_id} "
                    f"AMS {ams_id}/{tray_id}: {e}"
                )

        if posted:
            logger.info(
                f"Assignment backfill restored {posted} missing Bambuddy slot link(s) "
                f"for printer {self.printer_id}"
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

    # -- Slot-Verarbeitung (AMS-Status → FilaMan Slots) ----------------------

    @staticmethod
    def _tray_is_loaded(tray: dict) -> bool:
        """True when Bambuddy/AMS reports a spool present in this tray."""
        if tray.get("tray_type"):
            return True
        state = tray.get("state")
        return state in (11, 12)

    @staticmethod
    def _tray_snapshot(tray: dict) -> dict[str, Any]:
        """Compact tray state for pending-insert detection."""
        return {
            "present": Driver._tray_is_loaded(tray),
            "tray_type": tray.get("tray_type") or "",
            "tray_color": tray.get("tray_color") or "",
            "state": tray.get("state"),
            "tag_uid": (tray.get("tag_uid") or "").upper(),
        }

    async def _capture_pending_snapshot(self) -> None:
        """Record AMS tray state at scan time (baseline for insert detection)."""
        if not self._client or not self._bambuddy_printer_id:
            self._pending_slot_snapshot = {
                s["slot_index"]: {
                    "present": s.get("present", False),
                    "tray_type": s.get("tray_type") or "",
                    "tray_color": s.get("tray_color") or "",
                    "state": None,
                    "tag_uid": "",
                }
                for s in self._current_slots
            }
            return
        try:
            r = await self._client.get(
                f"{self._bambuddy_url}/api/v1/printers/{self._bambuddy_printer_id}/status"
            )
            if r.status_code != 200:
                raise RuntimeError(f"status HTTP {r.status_code}")
            status = r.json()
            snap: dict[str, dict[str, Any]] = {}
            for ams_unit in status.get("ams") or []:
                ams_id = int(ams_unit.get("id", 0))
                for tray in ams_unit.get("tray") or []:
                    tray_id = int(tray.get("id", 0))
                    snap[f"{ams_id}-{tray_id}"] = self._tray_snapshot(tray)
            for vt in status.get("vt_tray") or []:
                vt_id = int(vt.get("id", 254))
                snap[f"255-{vt_id}"] = self._tray_snapshot(vt)
            self._pending_slot_snapshot = snap
            logger.info(
                f"Pending snapshot captured for printer {self.printer_id}: "
                f"{sum(1 for v in snap.values() if v.get('present'))} loaded slot(s)"
            )
        except Exception as e:
            logger.debug(f"Could not capture pending slot snapshot: {e}")
            self._pending_slot_snapshot = {
                s["slot_index"]: {
                    "present": s.get("present", False),
                    "tray_type": s.get("tray_type") or "",
                    "tray_color": s.get("tray_color") or "",
                    "state": None,
                    "tag_uid": "",
                }
                for s in self._current_slots
            }

    def _try_match_pending_tray(
        self, slot_index: str, tray: dict
    ) -> tuple[bool, str]:
        """Return (matched, reason) if this tray was inserted since scan."""
        if self._pending_spool_id is None:
            return False, ""
        if not self._tray_is_loaded(tray):
            return False, ""

        tray_tag_uid = (tray.get("tag_uid") or "").upper()
        baseline = (self._pending_slot_snapshot or {}).get(slot_index)
        prev_slot = next(
            (s for s in self._current_slots if s["slot_index"] == slot_index),
            None,
        )
        ref = baseline if baseline is not None else (
            {
                "present": prev_slot.get("present", False) if prev_slot else False,
                "tray_type": (prev_slot or {}).get("tray_type") or "",
                "tray_color": (prev_slot or {}).get("tray_color") or "",
                "state": None,
                "tag_uid": "",
            }
            if prev_slot
            else None
        )

        rfid_matched = bool(
            tray_tag_uid
            and self._pending_rfid_hex
            and tray_tag_uid == self._pending_rfid_hex
        )
        if rfid_matched:
            return True, "rfid"

        was_empty = ref is None or not ref.get("present", False)
        ref_state = ref.get("state") if ref else None
        now_state = tray.get("state")
        state_loaded = now_state in (11, 12) and ref_state in (9, 0, None, "") and was_empty

        tray_type = tray.get("tray_type") or ""
        tray_color = tray.get("tray_color") or ""
        prev_tray_type = (ref.get("tray_type") or "") if ref else ""
        prev_tray_color = (ref.get("tray_color") or "") if ref else ""
        content_changed = (
            not was_empty
            and ref is not None
            and ref.get("present", False)
            and (tray_type != prev_tray_type or tray_color != prev_tray_color)
        )

        if was_empty or state_loaded:
            return True, "slot-appeared"
        if content_changed:
            return True, "direct-swap"
        return False, ""

    def _fire_pending_assignment(self, ams_id: int, tray_id: int, reason: str) -> None:
        logger.info(
            f"Pending spool {self._pending_spool_id} matched "
            f"AMS {ams_id}/{tray_id} ({reason})"
        )
        self.send_filament_to_tray(
            ams_id, tray_id, {**(self._pending_filament_data or {})}
        )
        self._clear_pending_peers()

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

                prev_slot = next(
                    (s for s in self._current_slots if s["slot_index"] == slot_index),
                    None,
                )
                was_present = bool(prev_slot and prev_slot.get("present"))
                now_present = bool(tray_type)
                sticky_fm_id = self._slot_to_filaman_spool.get(slot_index)

                # Sticky assignments: physical empty does NOT clear FilaMan location
                # or slot ownership. Bambuddy may auto-unlink — reassert the link.
                # Never reassert while a pending RFID scan is active (swap race:
                # old occupant SUN*/GF* MQTT was overwriting the new assign).
                if not now_present:
                    if (
                        sticky_fm_id
                        and was_present
                        and self._pending_spool_id is None
                    ):
                        self._schedule_sticky_reassert(
                            ams_id, tray_id, configure=False
                        )
                else:
                    # Pending scan wins over sticky.
                    matched, reason = self._try_match_pending_tray(slot_index, tray)
                    matched_spool_id: int | None = None
                    if matched:
                        matched_spool_id = self._pending_spool_id
                        # Claim the slot synchronously so a concurrent sticky
                        # reassert cannot re-bind the previous occupant.
                        if matched_spool_id is not None:
                            prev_owner = self._slot_to_filaman_spool.get(slot_index)
                            self._cancel_sticky_task(slot_index)
                            self._bump_slot_configure_gen(slot_index)
                            if prev_owner and prev_owner != matched_spool_id:
                                self._slot_to_filaman_spool.pop(slot_index, None)
                                _rt = asyncio.create_task(
                                    self._restore_spool_location(prev_owner)
                                )
                                _rt.add_done_callback(self._on_task_done)
                            self._slot_to_filaman_spool[slot_index] = matched_spool_id
                            sticky_fm_id = matched_spool_id
                        self._fire_pending_assignment(ams_id, tray_id, reason)
                    elif (
                        sticky_fm_id
                        and self._pending_spool_id is None
                        and not was_present
                    ):
                        # Unscanned reinsert only — not mid-tray content flaps.
                        # Pass live tray so mismatch vs previous occupant clears
                        # sticky instead of pushing the old spool's profile.
                        self._schedule_sticky_reassert(
                            ams_id, tray_id, configure=True, tray=tray
                        )

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
                        "present": now_present,
                        "sticky_spool_id": sticky_fm_id,
                        # Plugin manager persists PrinterSlotAssignment.spool_id
                        # from this field — without it the UI shows an unlinked
                        # slot even when Bambuddy/sticky ownership is known.
                        "spool_id": sticky_fm_id if now_present else None,
                        "tag_uid": tray.get("tag_uid") or "",
                        "tray_uuid": tray_uuid or "",
                    }
                )

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
                    # Skip when the "specific" code is only a Bambu-brand basic
                    # (e.g. GFB00) — third-party ABS/ASA cloud presets often inherit
                    # that base, and re-pushing from an empty cache wiped PFUS and
                    # made Studio show "Bambu ABS" while SUN*-coded spools were fine.
                    prev_idx = (
                        (prev_slot.get("tray_info_idx") or "") if prev_slot else ""
                    )
                    bambu_brand_codes = frozenset(_BAMBU_BRAND_SLICER_IDS.values())
                    if (
                        (not prev_idx or prev_idx in _GENERIC_SLICER_ID_SET)
                        and tray_info_idx not in bambu_brand_codes
                    ):
                        logger.info(
                            f"Late NFC read on slot {slot_index}: "
                            f"{prev_idx!r} → {tray_info_idx!r}, reconfiguring"
                        )
                        _t = asyncio.create_task(
                            self._reconfigure_slot_with_profile(
                                ams_id, tray_id, tray_info_idx, tray
                            )
                        )
                        _t.add_done_callback(self._on_task_done)
                    elif (
                        (not prev_idx or prev_idx in _GENERIC_SLICER_ID_SET)
                        and tray_info_idx in bambu_brand_codes
                    ):
                        logger.info(
                            f"Late NFC read on slot {slot_index}: "
                            f"{prev_idx!r} → {tray_info_idx!r} (Bambu brand basic) "
                            f"— skip reconfigure to preserve spool PFUS setting_id"
                        )

        ext_slots: list[dict[str, Any]] = []
        for vt in vt_tray_list:
            vt_id = int(vt.get("id", 254))
            vt_type = vt.get("tray_type", "")
            vt_color = vt.get("tray_color", "")
            vt_tray_id = canonical_external_tray_id(vt_id)
            vt_idx = external_slot_index(vt_id)
            vt_cached = self._slot_params_cache.get(vt_idx, {})

            vt_preset_id = vt.get("preset_id", "")
            vt_tray_info_idx = _extract_bambu_idx(vt_preset_id) or vt.get(
                "tray_info_idx", ""
            )

            prev_vt = next(
                (s for s in self._current_slots if s["slot_index"] == vt_idx),
                None,
            )
            vt_was_present = bool(prev_vt and prev_vt.get("present"))
            vt_now_present = bool(vt_type)
            sticky_vt_id = self._slot_to_filaman_spool.get(vt_idx)

            if not vt_now_present:
                if (
                    sticky_vt_id
                    and vt_was_present
                    and self._pending_spool_id is None
                ):
                    self._schedule_sticky_reassert(255, vt_tray_id, configure=False)
            else:
                matched, reason = self._try_match_pending_tray(vt_idx, vt)
                if matched:
                    matched_spool_id = self._pending_spool_id
                    if matched_spool_id is not None:
                        prev_owner = self._slot_to_filaman_spool.get(vt_idx)
                        self._cancel_sticky_task(vt_idx)
                        self._bump_slot_configure_gen(vt_idx)
                        if prev_owner and prev_owner != matched_spool_id:
                            self._slot_to_filaman_spool.pop(vt_idx, None)
                            _rt = asyncio.create_task(
                                self._restore_spool_location(prev_owner)
                            )
                            _rt.add_done_callback(self._on_task_done)
                        self._slot_to_filaman_spool[vt_idx] = matched_spool_id
                        sticky_vt_id = matched_spool_id
                    self._fire_pending_assignment(255, vt_tray_id, reason)
                elif (
                    sticky_vt_id
                    and self._pending_spool_id is None
                    and not vt_was_present
                ):
                    self._schedule_sticky_reassert(
                        255, vt_tray_id, configure=True, tray=vt
                    )

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
                    "present": vt_now_present,
                    "sticky_spool_id": sticky_vt_id,
                    "spool_id": sticky_vt_id if vt_now_present else None,
                    "tag_uid": vt.get("tag_uid") or "",
                    "tray_uuid": vt.get("tray_uuid") or "",
                }
            )

            # Late-NFC reconfigure for external tray
            if (
                vt_type
                and vt_idx in self._slot_to_filaman_spool
                and vt_tray_info_idx
                and vt_tray_info_idx not in _GENERIC_SLICER_ID_SET
            ):
                prev_vt_idx = (
                    (prev_vt.get("tray_info_idx") or "") if prev_vt else ""
                )
                bambu_brand_codes = frozenset(_BAMBU_BRAND_SLICER_IDS.values())
                if (
                    (not prev_vt_idx or prev_vt_idx in _GENERIC_SLICER_ID_SET)
                    and vt_tray_info_idx not in bambu_brand_codes
                ):
                    logger.info(
                        f"Late NFC read on external tray {vt_idx}: "
                        f"{prev_vt_idx!r} → {vt_tray_info_idx!r}, reconfiguring"
                    )
                    _t = asyncio.create_task(
                        self._reconfigure_slot_with_profile(
                            255, vt_tray_id, vt_tray_info_idx, vt
                        )
                    )
                    _t.add_done_callback(self._on_task_done)
                elif (
                    (not prev_vt_idx or prev_vt_idx in _GENERIC_SLICER_ID_SET)
                    and vt_tray_info_idx in bambu_brand_codes
                ):
                    logger.info(
                        f"Late NFC read on external tray {vt_idx}: "
                        f"{prev_vt_idx!r} → {vt_tray_info_idx!r} "
                        f"(Bambu brand basic) — skip reconfigure"
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
