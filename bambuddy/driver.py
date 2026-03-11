import asyncio
import json
import logging
from contextlib import suppress
from datetime import datetime
from typing import Any, Callable

import httpx
import websockets
import websockets.exceptions

from app.plugins.base import BaseDriver

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 60
DEFAULT_RECONNECT_INTERVAL = 30

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


class PendingSpool:
    def __init__(self, spool_id: int, filament_data: dict, slot_index: str | None = None):
        self.spool_id = spool_id
        self.filament_data = filament_data
        self.slot_index = slot_index  # z.B. "0-1" oder None für beliebigen Slot
        self.started_at = datetime.utcnow()
        self.timer: asyncio.Task | None = None


class Driver(BaseDriver):
    driver_key = "bambuddy"

    def __init__(
        self,
        printer_id: int,
        config: dict[str, Any],
        emitter: Callable[[dict[str, Any]], None],
    ):
        super().__init__(printer_id, config, emitter)
        self._bambuddy_url = config.get("bambuddy_url", "").rstrip("/")
        self._api_key = config.get("api_key", "")
        self._bambuddy_printer_id = config.get("printer_id")
        self._reconnect_interval = config.get("reconnect_interval_seconds", DEFAULT_RECONNECT_INTERVAL)

        self._headers = {"Authorization": f"ApiKey {self._api_key}"}
        self._client: httpx.AsyncClient | None = None
        self._ws_task: asyncio.Task | None = None

        self._pending: PendingSpool | None = None
        self._timeout_seconds = DEFAULT_TIMEOUT
        self._current_slots: list[dict[str, Any]] = []
        self._current_ams_units: list[dict[str, Any]] = []
        self._connected = False

    # -- Lifecycle ------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._client = httpx.AsyncClient(headers=self._headers, timeout=10.0)

        # Initialen Status sofort laden (ohne auf WebSocket-Event zu warten)
        await self._fetch_and_emit_status()

        # WebSocket-Listener als dauerhafter Background-Task
        self._ws_task = asyncio.create_task(self._ws_listener())
        logger.info(f"Bambuddy driver started for printer {self.printer_id} "
                    f"(Bambuddy printer_id={self._bambuddy_printer_id})")

    async def stop(self) -> None:
        self._running = False
        if self._pending and self._pending.timer:
            self._pending.timer.cancel()
            self._pending = None
        if self._ws_task:
            self._ws_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._ws_task
        if self._client:
            await self._client.aclose()
            self._client = None
        self._connected = False
        logger.info(f"Bambuddy driver stopped for printer {self.printer_id}")

    async def reconnect(self) -> None:
        logger.info(f"Reconnecting Bambuddy driver for printer {self.printer_id}")
        if self._ws_task:
            self._ws_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._ws_task
        self._current_slots = []  # Force full re-sync on reconnect
        self._connected = False
        self._ws_task = asyncio.create_task(self._ws_listener())

    # -- WebSocket Listener ---------------------------------------------------

    async def _ws_listener(self) -> None:
        """Verbindet sich zum Bambuddy WebSocket und empfängt printer_status Events.
        Reconnect-Loop: bei Verbindungsabbruch automatisch neu verbinden."""
        ws_url = self._bambuddy_url.replace("http://", "ws://").replace("https://", "wss://")
        ws_url = f"{ws_url}/api/v1/ws"

        while self._running:
            try:
                async with websockets.connect(
                    ws_url,
                    additional_headers={"Authorization": f"ApiKey {self._api_key}"},
                    ping_interval=30,
                    ping_timeout=10,
                ) as ws:
                    self._connected = True
                    logger.info(f"Bambuddy WebSocket connected for printer {self.printer_id}")
                    self.log_debug("event", "websocket", {"event": "connected", "url": ws_url})

                    async for raw_msg in ws:
                        try:
                            event = json.loads(raw_msg)
                        except json.JSONDecodeError:
                            continue

                        event_type = event.get("type", "")

                        # Nur Events für unseren Drucker verarbeiten
                        if event.get("printer_id") != self._bambuddy_printer_id:
                            continue

                        self.log_debug("in", "websocket", event)

                        if event_type == "printer_status":
                            self._process_slots(event.get("data", {}))

            except (
                websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException,
                OSError,
            ) as e:
                self._connected = False
                self._current_slots = []  # Force full re-sync on reconnect
                if self._running:
                    logger.warning(
                        f"Bambuddy WebSocket disconnected for printer {self.printer_id}: {e}. "
                        f"Reconnecting in {self._reconnect_interval}s"
                    )
                    self.log_debug("event", "websocket", {"event": "disconnected", "error": str(e)})
                    await asyncio.sleep(self._reconnect_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                if self._running:
                    logger.error(f"Unexpected error in Bambuddy WebSocket for printer {self.printer_id}: {e}")
                    await asyncio.sleep(self._reconnect_interval)

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
                self._process_slots(r.json())
                logger.info(f"Initial status fetched for Bambuddy printer {self._bambuddy_printer_id}")
        except Exception as e:
            logger.warning(f"Could not fetch initial Bambuddy status: {e}")

    # -- Slot-Verarbeitung ----------------------------------------------------

    def _process_slots(self, printer_status: dict) -> None:
        """AMS-Daten aus Bambuddy printer_status verarbeiten und slots_update emittieren.

        Merge-Strategie: Wie im BambuLab-Plugin werden nur vorhandene Daten aktualisiert.
        Auto-Set: Erkennt Slot-Änderungen via Feldvergleich (tray_type, tray_color).
        Bambuddy liefert den externen Slot als separates `vt_tray`-Array (wie BambuLab).
        """
        ams_list = printer_status.get("ams", [])
        vt_tray_list = printer_status.get("vt_tray", [])
        if not ams_list and not vt_tray_list:
            return

        # -- AMS-Einheiten Metadaten + AMS-Slots --
        ams_units: list[dict[str, Any]] = []
        ams_slots: list[dict[str, Any]] = []

        for ams_unit in ams_list:
            ams_id = int(ams_unit.get("id", 0))
            trays = ams_unit.get("tray", ams_unit.get("trays", []))
            ams_units.append({
                "ams_id": ams_id,
                "humidity": ams_unit.get("humidity"),
                "temp": ams_unit.get("temp", ams_unit.get("temperature")),
                "tray_count": len(trays),
                "is_ams_ht": ams_unit.get("is_ams_ht", False),
            })

            for tray in trays:
                tray_id = int(tray.get("id", 0))
                slot_index = f"{ams_id}-{tray_id}"
                tray_type  = tray.get("tray_type", "")
                tray_color = tray.get("tray_color", "")

                ams_slots.append({
                    "slot_index": slot_index,
                    "slot_name": f"AMS {ams_id + 1} - Slot {tray_id + 1}",
                    "tray_type": tray_type,
                    "tray_color": tray_color,
                    "remain": tray.get("remain", 0),
                    "present": bool(tray_type),
                })

        # -- Externer Slot (vt_tray) — analog BambuLab-Plugin (slot_index "255-{id}") --
        ext_slots: list[dict[str, Any]] = []
        for vt in vt_tray_list:
            vt_id      = int(vt.get("id", 254))
            vt_type    = vt.get("tray_type", "")
            vt_color   = vt.get("tray_color", "")
            ext_slots.append({
                "slot_index": f"255-{vt_id}",
                "slot_name":  "External Tray",
                "tray_type":  vt_type,
                "tray_color": vt_color,
                "remain":     vt.get("remain", 0),
                "present":    bool(vt_type),
            })

        self._current_ams_units = ams_units
        has_external = len(ext_slots) > 0
        slots = ams_slots + ext_slots

        # -- Auto-Set: Slot-Änderung erkennen (analog BambuLab-Plugin) --
        # Vergleicht tray_type und tray_color gegen vorherige Slot-Daten.
        if self._pending and self._current_slots:
            _compare_fields = ("tray_type", "tray_color")
            for new_slot in slots:
                sid = new_slot.get("slot_index", "")
                if not new_slot.get("tray_type"):
                    continue  # Leerer Slot

                old_slot = next(
                    (s for s in self._current_slots if s.get("slot_index") == sid), None
                )
                if old_slot is None:
                    continue  # Kein Vergleich möglich (erster Sync)

                has_changed = any(
                    new_slot.get(f, "") != old_slot.get(f, "")
                    for f in _compare_fields
                )
                if not has_changed:
                    continue

                # Slot-Filter: Pending kann auf bestimmten Slot begrenzt sein
                if self._pending.slot_index is not None and self._pending.slot_index != sid:
                    continue

                try:
                    ams_id_parsed, tray_id_parsed = map(int, sid.split("-"))
                except (ValueError, IndexError):
                    continue

                logger.info(f"Tray data changed at slot {sid}: "
                            f"assigning pending spool {self._pending.spool_id}")
                asyncio.create_task(
                    self._send_assignment(ams_id_parsed, tray_id_parsed, self._pending.filament_data)
                )
                if self._pending.timer:
                    self._pending.timer.cancel()
                self._pending = None
                break  # Nur erste Änderung zuweisen

        # Nur emittieren wenn sich Slot-Daten geändert haben
        if slots == self._current_slots:
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

        logger.info(f"Slot data changed for printer {self.printer_id}, emitting slots_update")
        self.emit({"event_type": "slots_update", "slots": slots, "ams_info": ams_info})

    # -- Spule in Bambuddy zuweisen -------------------------------------------

    async def _send_assignment(self, ams_id: int, tray_id: int, filament_data: dict) -> None:
        """Erstellt Spule in Bambuddy und weist sie einem AMS-Slot zu.
        Bambuddy übernimmt die MQTT-Kommunikation zum Drucker.

        Bambu-spezifische Felder (aus printer_params via enrich_filament_data):
        - bambu_idx → slicer_filament (tray_info_idx im Bambu-MQTT)
        - bambu_tray_idx → Fallback für slicer_filament
        - bambu_nozzle_temp_min/max → nozzle_temp_min/max
        - material_subgroup → subtype
        - bambu_k_value → K-Profil (separater API-Call)
        - bambu_cali_idx, bambu_setting_id → K-Profil
        """
        if not self._client:
            logger.error("Cannot send assignment: HTTP client not initialized")
            return

        # -- Farbe normalisieren: FilaMan liefert 8-stelliges RGBA, Bambuddy erwartet RRGGBBAA --
        color = filament_data.get("color", "FFFFFFFF")
        if len(color) == 6:
            color = color + "FF"
        elif len(color) != 8:
            color = "FFFFFFFF"

        # -- Bambu Material Index (slicer_filament = tray_info_idx) --
        # Priorität: bambu_idx (aus printer_params) → bambu_tray_idx → generischer Fallback
        material = filament_data.get("material_type", "PLA")
        slicer_filament = (
            filament_data.get("bambu_idx")
            or filament_data.get("bambu_tray_idx")
            or _GENERIC_SLICER_IDS.get(material.upper(), "GFL99")
        )

        # -- Temperaturen: Bambu-spezifisch hat Priorität über generische FilaMan-Felder --
        nozzle_temp_min = (
            _int_or_none(filament_data.get("bambu_nozzle_temp_min"))
            or _int_or_none(filament_data.get("nozzle_temp_min"))
        )
        nozzle_temp_max = (
            _int_or_none(filament_data.get("bambu_nozzle_temp_max"))
            or _int_or_none(filament_data.get("nozzle_temp_max"))
        )

        spool_payload = {
            "material":       material,
            "subtype":        filament_data.get("material_subgroup") or None,
            "color_name":     filament_data.get("color_name", ""),
            "rgba":           color,
            "brand":          filament_data.get("brand", ""),
            "label_weight":   int(filament_data.get("label_weight", 1000)),
            "weight_used":    int(filament_data.get("weight_used", 0)),
            "slicer_filament": slicer_filament,
            "nozzle_temp_min": nozzle_temp_min,
            "nozzle_temp_max": nozzle_temp_max,
            # Rückreferenz zu FilaMan für spätere Verbrauchsmeldungen
            "note": f"filaman:{filament_data.get('spool_id', '')}",
        }

        try:
            # 1. Spule in Bambuddy anlegen
            r = await self._client.post(
                f"{self._bambuddy_url}/api/v1/inventory/spools",
                json=spool_payload,
            )
            r.raise_for_status()
            bambuddy_spool_id = r.json()["id"]
            self.log_debug("out", "POST /api/v1/inventory/spools", {
                "bambuddy_spool_id": bambuddy_spool_id,
                "filaman_spool_id": filament_data.get("spool_id"),
                "slicer_filament": slicer_filament,
            })

            # 2. K-Profil anlegen (optional — nur wenn bambu_k_value gesetzt)
            k_value = _float_or_none(filament_data.get("bambu_k_value"))
            if k_value is not None:
                k_payload = {
                    "printer_id":      self._bambuddy_printer_id,
                    "k_value":         k_value,
                    "cali_idx":        _int_or_none(filament_data.get("bambu_cali_idx")),
                    "setting_id":      filament_data.get("bambu_setting_id") or None,
                    "nozzle_diameter": "0.4",
                }
                try:
                    rk = await self._client.post(
                        f"{self._bambuddy_url}/api/v1/inventory/spools/{bambuddy_spool_id}/k-profiles",
                        json=k_payload,
                    )
                    rk.raise_for_status()
                    self.log_debug("out", f"POST /api/v1/inventory/spools/{bambuddy_spool_id}/k-profiles", k_payload)
                except httpx.HTTPStatusError as e:
                    # K-Profil-Fehler nicht fatal — Assignment trotzdem durchführen
                    logger.warning(
                        f"Could not create K-profile for Bambuddy spool {bambuddy_spool_id}: "
                        f"{e.response.status_code} {e.response.text}"
                    )

            # 3. Slot in Bambuddy zuweisen → Bambuddy konfiguriert AMS via MQTT
            assignment_payload = {
                "spool_id":   bambuddy_spool_id,
                "printer_id": self._bambuddy_printer_id,
                "ams_id":     ams_id,
                "tray_id":    tray_id,
            }
            r2 = await self._client.post(
                f"{self._bambuddy_url}/api/v1/inventory/assignments",
                json=assignment_payload,
            )
            r2.raise_for_status()
            self.log_debug("out", "POST /api/v1/inventory/assignments", assignment_payload)
            logger.info(
                f"Assigned FilaMan spool {filament_data.get('spool_id')} "
                f"(slicer_filament={slicer_filament}) to "
                f"Bambuddy printer {self._bambuddy_printer_id} slot {ams_id}-{tray_id}"
            )
        except httpx.HTTPStatusError as e:
            logger.error(f"Bambuddy API error during assignment: {e.response.status_code} {e.response.text}")
        except Exception as e:
            logger.error(f"Failed to send assignment to Bambuddy: {e}")

    # -- Öffentliche API (aufgerufen vom PluginManager) -----------------------

    def send_filament_to_tray(self, ams_id: int, tray_id: int, filament_data: dict) -> None:
        """Direkte manuelle Zuweisung ohne Pending-Mechanismus."""
        asyncio.create_task(self._send_assignment(ams_id, tray_id, filament_data))

    async def assign_pending_spool(
        self,
        spool_id: int,
        filament_data: dict,
        slot_index: str | None = None,
        timeout_seconds: int | None = None,
    ) -> None:
        """Spule für automatische Zuweisung vormerken.
        Beim nächsten erkannten Slot-Wechsel (via Bambuddy WebSocket)
        wird die Spule automatisch in den geänderten Slot konfiguriert."""
        if self._pending and self._pending.timer:
            self._pending.timer.cancel()

        self._pending = PendingSpool(spool_id, filament_data, slot_index)
        effective_timeout = timeout_seconds if timeout_seconds is not None else self._timeout_seconds
        self._pending.timer = asyncio.create_task(self._timeout_task(effective_timeout))
        logger.info(
            f"Pending spool {spool_id} for printer {self.printer_id} "
            f"(slot: {slot_index}, timeout: {effective_timeout}s)"
        )

    async def _timeout_task(self, timeout: int) -> None:
        """Verwirft das Pending nach Ablauf des Timeouts."""
        await asyncio.sleep(timeout)
        if self._pending:
            logger.info(f"Pending spool {self._pending.spool_id} timed out after {timeout}s")
            self._pending = None

    # -- Health ---------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        total_slots = sum(u.get("tray_count", 0) for u in self._current_ams_units)
        return {
            "driver_key": self.driver_key,
            "printer_id": self.printer_id,
            "running": self._running,
            "connected": self._connected,
            "pending": self._pending is not None,
            "bambuddy_printer_id": self._bambuddy_printer_id,
            "ams_count": len(self._current_ams_units),
            "slot_count": total_slots,
            "ams_units": self._current_ams_units,
            "slots": self._current_slots,
        }
