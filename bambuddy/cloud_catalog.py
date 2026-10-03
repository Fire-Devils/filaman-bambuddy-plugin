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


def slot_preset_record(
    setting_id: str | None, preset_name: str | None
) -> dict[str, str] | None:
    """Query params for Bambuddy's slot-preset PUT, or None to leave the row.

    Inventory assignment labels the slot from the spool's single
    ``slicer_filament_name``. That field is shared by every printer, so an
    H2D slot is often labeled with the H2C preset and the tray code
    (``SUN22001``) instead of the PFUS configure just sent. Only a cloud
    setting id with its own display name may replace that row.
    """
    code = (setting_id or "").strip()
    name = (preset_name or "").strip()
    if not is_cloud_setting_id(code) or not name:
        return None
    return {
        "preset_id": code,
        "preset_name": name,
        "preset_source": "cloud",
    }


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


# Vendor AMS families (SUNLU: SUN20xxx = PLA, SUN22xxx = PETG).
_VENDOR_PREFIX_FAMILY: dict[str, str] = {
    "SUN20": "PLA",
    "SUN22": "PETG",
}

# Official Bambu GF* third-letter families. GFB is ABS *or* ASA — only
# resolved via an exact/name map. GFL is usually PLA; Fiberon PETG/PA SKUs
# (GFL06, GFL55, …) are overridden by the display-name lookup.
_GF_LETTER_FAMILY: dict[str, str] = {
    "A": "PLA",
    "L": "PLA",
    "G": "PETG",
    "U": "TPU",
    "N": "PA",
    "C": "PC",
    "P": "PP",
}

_FAMILY_ALIASES: dict[str, str] = {
    "PLA+": "PLA",
    "PLA-CF": "PLA",
    "PLA-PLUS": "PLA",
    "PETG-CF": "PETG",
    "PETG-PLUS": "PETG",
    "PETG HF": "PETG",
    "PCTG": "PETG",
    "NYLON": "PA",
    "PA-CF": "PA",
    "PA6": "PA",
    "PA6-CF": "PA",
    "PA12": "PA",
}

_NAME_FAMILY_TOKENS: tuple[str, ...] = (
    "PETG",
    "PLA",
    "ASA",
    "ABS",
    "TPU",
    "PVA",
    "HIPS",
    "NYLON",
    "PA",
    "PC",
    "PP",
)


def normalize_material_family(material: str | None) -> str:
    """Collapse FilaMan types (PLA+, PETG-CF, NYLON) to a Bambu base family."""
    upper = (material or "").upper().strip()
    if not upper:
        return ""
    if upper in _FAMILY_ALIASES:
        return _FAMILY_ALIASES[upper]
    for token in _NAME_FAMILY_TOKENS:
        if upper == token or upper.startswith(token):
            return "PA" if token == "NYLON" else token
    return upper


def preset_material_fits(name: str | None, material: str | None) -> bool | None:
    """Whether a preset's name is the same material family as *material*.

    Returns None when either side cannot be classified. A known mismatch
    (PETG preset on a PLA filament) is False.
    """
    family = family_from_display_name(name)
    mat = normalize_material_family(material)
    if not family or not mat:
        return None
    if family == mat:
        return True
    # Blends and compounds name more than one family ("PC-ABS", "PPA-CF").
    # The filament's own family anywhere in the name counts as a match.
    if mat in str(name).upper():
        return True
    return False


def presets_from_saved_names(names: dict[str, str] | None) -> list[dict[str, Any]]:
    """Rebuild cloud-preset entries from the last healthy ``code → name`` cache.

    Names are enough to recover model and nozzle, so variant lookup still
    works when ``/cloud/filaments`` is down. Only PFUS/PFCN codes are kept;
    builtins come from their own endpoint.
    """
    entries: list[dict[str, Any]] = []
    for code, name in (names or {}).items():
        setting_id = str(code or "").strip()
        label = str(name or "").strip()
        if not setting_id or not label or not is_cloud_setting_id(setting_id):
            continue
        entries.append(
            {
                "code": setting_id,
                "name": label,
                "displayName": label,
                "isCustom": True,
            }
        )
    return entries


def family_from_display_name(name: str | None) -> str | None:
    """Infer PLA/PETG/… from a catalog or id-map display name."""
    if not name:
        return None
    upper = str(name).upper()
    if "SUPPORT" in upper:
        return None
    if "PCTG" in upper:
        return "PETG"
    if "NYLON" in upper:
        return "PA"
    for token in _NAME_FAMILY_TOKENS:
        if token in upper:
            return "PA" if token == "NYLON" else token
    return None


def infer_tray_code_family(
    code: str | None,
    *,
    names: dict[str, str] | None = None,
    exact: dict[str, str] | None = None,
) -> str | None:
    """Return PLA/PETG/… for an AMS tray code, or None when unknown.

    Order: exact builtin/brand map, display name (catches Fiberon GFL PETG),
    vendor prefix (SUN20/SUN22), then GF third-letter heuristic.
    """
    if not code:
        return None
    raw = str(code).strip()
    if not raw or is_cloud_setting_id(raw):
        return None
    if exact and raw in exact:
        return normalize_material_family(exact[raw])
    if names and raw in names:
        from_name = family_from_display_name(names[raw])
        if from_name:
            return from_name
    upper = raw.upper()
    if upper.startswith("SUN") and len(upper) >= 5:
        return _VENDOR_PREFIX_FAMILY.get(upper[:5])
    if upper.startswith("GF") and len(upper) >= 3:
        return _GF_LETTER_FAMILY.get(upper[2])
    return None


def tray_code_matches_material(
    code: str | None,
    material: str | None,
    *,
    names: dict[str, str] | None = None,
    exact: dict[str, str] | None = None,
) -> bool:
    """True when *code* is usable for *material*.

    Unknown codes (Studio custom ``Pxxxxxxx``, unmapped vendor SKUs) are
    allowed so we do not block legitimate trays we cannot classify.
    """
    if not code:
        return True
    family = infer_tray_code_family(code, names=names, exact=exact)
    if family is None:
        return True
    mat = normalize_material_family(material)
    if not mat:
        return True
    return family == mat


def _rgb6(color: str | None) -> str:
    raw = (color or "").strip().lstrip("#").upper()
    if len(raw) >= 6:
        return raw[:6]
    return raw


def late_nfc_conflicts_with_recent_send(
    sent: dict[str, Any] | None,
    live_code: str | None,
    live_color: str | None,
    *,
    now: float,
    window: float,
    generic_codes: Iterable[str] = (),
) -> bool:
    """True when a stale tray report must not overwrite a configure just sent.

    Right after an assign the AMS often still reports the previous spool.
    Late-NFC treats generic/empty → specific as a finished chip read and would
    push that stale colour and SKU onto the new spool.

    A real chip read is still allowed: we sent a generic code, the live code
    is specific, and the colour matches what we just configured.
    """
    if not sent:
        return False
    try:
        age = now - float(sent.get("ts") or 0.0)
    except (TypeError, ValueError):
        return False
    if age < 0 or age >= window:
        return False
    sent_color = _rgb6(sent.get("color"))
    live_rgb = _rgb6(live_color)
    if sent_color and live_rgb and sent_color != live_rgb:
        return True
    sent_code = str(sent.get("code") or "").strip()
    live = str(live_code or "").strip()
    generic = {str(g).strip().upper() for g in generic_codes if g}
    if (
        sent_code
        and live
        and sent_code != live
        and sent_code.upper() not in generic
        and not is_cloud_setting_id(sent_code)
    ):
        return True
    return False


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
