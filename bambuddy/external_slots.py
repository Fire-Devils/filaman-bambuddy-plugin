"""Canonical X2D/H2 external-tray identifiers shared with Bambuddy assignments."""

from __future__ import annotations


def canonical_external_tray_id(vt_id: int) -> int:
    """Map printer VT ids 254/255 to Bambuddy assignment tray ids 0/1.

    Bambuddy's inventory model represents both external feeds under the sentinel
    ``ams_id=255`` and uses ``tray_id=0`` / ``tray_id=1``.  The printer status
    payload instead names the physical virtual trays 254 / 255.  Accepting
    already-canonical 0 / 1 keeps the helper tolerant of normalized API data.
    """

    value = int(vt_id)
    if value in (0, 1):
        return value
    if value in (254, 255):
        return value - 254
    raise ValueError(f"Unsupported external tray id: {value}")


def external_slot_index(vt_id: int) -> str:
    """FilaMan/Bambuddy slot cache key for a physical or canonical VT id."""

    return f"255-{canonical_external_tray_id(vt_id)}"
