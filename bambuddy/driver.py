import asyncio
import logging
from typing import Any, Callable

import httpx

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
    """Vereinfachter Bambuddy-Driver: FilaMan → Bambuddy Push via HTTP.

    Sendet AMS-Konfiguration direkt über Bambuddys /configure-Endpunkt,
    ohne Spool-Anlegen, Assignment oder WebSocket-Verbindung.
    Die bidirektionale Synchronisierung (Bambuddy ↔ FilaMan) erfolgt
    über die native FilaMan-Integration in Bambuddy (PR).
    """

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

        self._headers = {"X-API-Key": f"{self._api_key}"}
        self._client: httpx.AsyncClient | None = None

        self._current_slots: list[dict[str, Any]] = []
        self._current_ams_units: list[dict[str, Any]] = []
        # Cache für Bambu-Parameter (nozzle temps, k_value etc.) pro Slot
        self._slot_params_cache: dict[str, dict] = {}

    # -- Lifecycle ------------------------------------------------------------

    async def start(self) -> None:
        self._running = True
        self._client = httpx.AsyncClient(headers=self._headers, timeout=10.0)

        # Initialen Status einmalig via REST laden
        await self._fetch_and_emit_status()

        logger.info(
            f"Bambuddy driver started for printer {self.printer_id} "
            f"(Bambuddy printer_id={self._bambuddy_printer_id})"
        )

    async def stop(self) -> None:
        self._running = False
        if self._client:
            await self._client.aclose()
            self._client = None
        logger.info(f"Bambuddy driver stopped for printer {self.printer_id}")

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
                cached     = self._slot_params_cache.get(slot_index, {})

                preset_id     = tray.get("preset_id", "")
                tray_info_idx = _extract_bambu_idx(preset_id) or tray.get("tray_info_idx", "")

                if not tray_type:
                    self._slot_params_cache.pop(slot_index, None)

                ams_slots.append({
                    "slot_index":    slot_index,
                    "slot_name":     f"AMS {ams_id + 1} - Slot {tray_id + 1}",
                    "tray_info_idx": tray_info_idx,
                    "tray_type":     tray_type,
                    "tray_color":    tray_color,
                    "nozzle_temp_min": (tray.get("nozzle_temp_min")
                                        if tray.get("nozzle_temp_min") is not None
                                        else cached.get("nozzle_temp_min")),
                    "nozzle_temp_max": (tray.get("nozzle_temp_max")
                                        if tray.get("nozzle_temp_max") is not None
                                        else cached.get("nozzle_temp_max")),
                    "setting_id": tray.get("setting_id") or cached.get("bambu_setting_id", ""),
                    "cali_idx":   (tray.get("cali_idx")
                                   if tray.get("cali_idx") is not None
                                   else cached.get("bambu_cali_idx")),
                    "bambu_k_value":              cached.get("bambu_k_value"),
                    "bambu_bed_temp":             cached.get("bambu_bed_temp"),
                    "bambu_flow_ratio":           cached.get("bambu_flow_ratio"),
                    "bambu_max_volumetric_speed": cached.get("bambu_max_volumetric_speed"),
                    "remain":  tray.get("remain", 0),
                    "present": bool(tray_type),
                })

        ext_slots: list[dict[str, Any]] = []
        for vt in vt_tray_list:
            vt_id      = int(vt.get("id", 254))
            vt_type    = vt.get("tray_type", "")
            vt_color   = vt.get("tray_color", "")
            vt_idx     = f"255-{vt_id}"
            vt_cached  = self._slot_params_cache.get(vt_idx, {})

            vt_preset_id     = vt.get("preset_id", "")
            vt_tray_info_idx = _extract_bambu_idx(vt_preset_id) or vt.get("tray_info_idx", "")

            if not vt_type:
                self._slot_params_cache.pop(vt_idx, None)

            ext_slots.append({
                "slot_index":    vt_idx,
                "slot_name":     "External Tray",
                "tray_info_idx": vt_tray_info_idx,
                "tray_type":     vt_type,
                "tray_color":    vt_color,
                "nozzle_temp_min": (vt.get("nozzle_temp_min")
                                    if vt.get("nozzle_temp_min") is not None
                                    else vt_cached.get("nozzle_temp_min")),
                "nozzle_temp_max": (vt.get("nozzle_temp_max")
                                    if vt.get("nozzle_temp_max") is not None
                                    else vt_cached.get("nozzle_temp_max")),
                "setting_id": vt.get("setting_id") or vt_cached.get("bambu_setting_id", ""),
                "cali_idx":   (vt.get("cali_idx")
                               if vt.get("cali_idx") is not None
                               else vt_cached.get("bambu_cali_idx")),
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
            "ams_count": len(ams_units),
            "ams_type": "AMS",
            "slot_count": total_slots,
            "external_spool": has_external,
            "ams_units": ams_units,
        }

        logger.info(f"Slot data changed for printer {self.printer_id}, emitting slots_update")
        self.emit({"event_type": "slots_update", "slots": slots, "ams_info": ams_info})

    # -- Spule in Bambuddy konfigurieren (FilaMan → Bambuddy Push) ------------

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

    # -- Öffentliche API (aufgerufen vom PluginManager) -----------------------

    def send_filament_to_tray(self, ams_id: int, tray_id: int, filament_data: dict) -> None:
        """Direkte manuelle Zuweisung: konfiguriert AMS-Slot über Bambuddy configure-Endpoint."""
        asyncio.create_task(self._send_assignment(ams_id, tray_id, filament_data))

    # -- Health ---------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        total_slots = sum(u.get("tray_count", 0) for u in self._current_ams_units)
        return {
            "driver_key": self.driver_key,
            "printer_id": self.printer_id,
            "running": self._running,
            "bambuddy_printer_id": self._bambuddy_printer_id,
            "ams_count": len(self._current_ams_units),
            "slot_count": total_slots,
            "ams_units": self._current_ams_units,
            "slots": self._current_slots,
        }
