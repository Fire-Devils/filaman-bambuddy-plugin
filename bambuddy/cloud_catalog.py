"""Pure helpers for the Bambu cloud preset catalog cache (unit-testable).

The driver keeps the merged ``/cloud/filaments`` + ``/cloud/builtin-filaments``
catalog in memory. These helpers decide when a fresh (possibly degraded) fetch
may replace the last-good catalog, and pick a tray_info_idx that never falls
back to a Generic code while a real slicer preset is known.
"""

from __future__ import annotations

from typing import Any, Iterable


def is_cloud_setting_id(code: str | None) -> bool:
    """PFUS/PFCN cloud setting_ids are slicer presets, never AMS tray codes."""
    if not code:
        return False
    return str(code).strip().upper().startswith(("PFUS", "PFCN"))


def catalog_has_cloud_presets(presets: Iterable[dict[str, Any]]) -> bool:
    """True when the catalog contains at least one cloud (PFUS/PFCN) preset."""
    return any(is_cloud_setting_id(p.get("code")) for p in presets)


def should_replace_catalog(
    previous: list[dict[str, Any]],
    fresh: list[dict[str, Any]],
    cloud_ok: bool,
) -> bool:
    """Decide whether *fresh* may overwrite the last-good *previous* catalog.

    ``cloud_ok`` is False when ``/cloud/filaments`` failed or returned nothing.
    A degraded fetch (builtins only) must never clobber a catalog that already
    holds real cloud presets — that is exactly what breaks PFUS → AMS-code
    resolution while Bambuddy's cloud API is flapping.
    """
    if not fresh:
        return False
    if cloud_ok:
        return True
    if not previous:
        return True
    return not catalog_has_cloud_presets(previous)


def preset_name_from_catalog(
    presets_by_code: dict[str, dict[str, Any]], preset_id: str | None
) -> str:
    """Display name for *preset_id* from the cached catalog ('' when unknown)."""
    if not preset_id:
        return ""
    entry = presets_by_code.get(str(preset_id).strip())
    if not isinstance(entry, dict):
        return ""
    return str(entry.get("name") or "").strip()


def pick_vendor_tray_code(
    candidates: Iterable[str | None],
    generic_codes: Iterable[str],
) -> str | None:
    """First candidate that is a usable, non-generic AMS tray code.

    Skips empty values, PFUS/PFCN setting_ids and any code in *generic_codes*
    (GFL99, GFG99, …). Used so a slot that already carries a vendor code such as
    ``SUN22001`` is not demoted to ``Generic PETG`` when the live cloud lookup
    happens to miss.
    """
    generic = {str(g).strip().upper() for g in generic_codes if g}
    for cand in candidates:
        if not cand:
            continue
        code = str(cand).strip()
        if not code or is_cloud_setting_id(code):
            continue
        if code.upper() in generic:
            continue
        return code
    return None
