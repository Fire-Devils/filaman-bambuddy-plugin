"""Pure helpers for per-model Bambu cloud profile variant resolution (unit-testable)."""

from __future__ import annotations

import re
from typing import Any

# Stock/newer presets: ``BASE @BBL H2C 0.4 nozzle``
# Custom/legacy presets: ``BASE @Bambu Lab P2S 0.4 nozzle``
_CLOUD_PRESET_SUFFIX_RE = re.compile(
    r" @(?:Bambu Lab|BBL) (.+?) (\d+(?:\.\d+)?) nozzle(?: .+)?$",
    re.IGNORECASE,
)
_CLOUD_PRESET_MODEL_RE = re.compile(
    r" @(?:Bambu Lab|BBL) (.+?)$",
    re.IGNORECASE,
)

_KNOWN_PRINTER_MODEL_TOKENS: tuple[str, ...] = (
    "P2S",
    "H2C",
    "X1C",
    "X1",
    "A1MINI",
    "A1",
    "P1S",
    "P1P",
    "P1",
    "P2",
)

# Multi-word names Bambu/Bambuddy use before canonical token matching.
_SPACED_MODEL_ALIASES: dict[str, str] = {
    "A1 MINI": "A1MINI",
    "X1 CARBON": "X1C",
}

STANDARD_NOZZLE_MM: tuple[float, ...] = (0.2, 0.4, 0.6, 0.8)
_STOCK_DEFAULT_NOZZLE_MM = 0.4


def _float_or_none(v: Any) -> float | None:
    try:
        return float(v) if v not in (None, "") else None
    except (ValueError, TypeError):
        return None


def _split_trailing_model_token(name: str) -> tuple[str, str]:
    """Split custom names like ``Mads TPU95 P2S`` (no ``@Bambu Lab`` suffix)."""
    stripped = (name or "").strip()
    if not stripped or " @" in stripped:
        return stripped, ""
    base, _, tail = stripped.rpartition(" ")
    if not base or not tail:
        return stripped, ""
    token = canonical_printer_model_token(tail)
    if not token or token.upper() != tail.strip().upper():
        return stripped, ""
    return base.strip(), token


def extract_profile_base_name(code_or_name: str | None) -> str:
    """Strip `` @Bambu Lab/BBL …`` suffix from a cloud preset name or code label."""
    if not code_or_name:
        return ""
    name = str(code_or_name).strip()
    if " @" in name:
        return name.split(" @", 1)[0].strip()
    base, _model = _split_trailing_model_token(name)
    return base


def is_cloud_setting_id(code: str | None) -> bool:
    """True for Bambu cloud slicer setting ids (PFUS/PFCN), not human base names."""
    if not code:
        return False
    upper = str(code).strip().upper()
    return upper.startswith(("PFUS", "PFCN"))


def coerce_profile_base_name(name: str | None, code: str | None = None) -> str:
    """Return a human profile base name for storage/display; never a PFUS/PFCN id."""
    if name:
        base = extract_profile_base_name(name)
        if base and not is_cloud_setting_id(base):
            return base
    if code and not is_cloud_setting_id(code):
        base = extract_profile_base_name(code)
        if base and not is_cloud_setting_id(base):
            return base
    return ""


def infer_default_base_name(
    profiles: dict[str, dict[str, str]], stored_default: str = ""
) -> str:
    """Pick the default profile base name shown in the UI.

    Stored defaults that are raw cloud setting ids (PFUS/PFCN) are ignored so a
    stale code cannot mask linked per-model human names.
    """
    stored = (stored_default or "").strip()
    if stored and not is_cloud_setting_id(stored):
        return stored
    linked = [
        e["base_name"]
        for e in profiles.values()
        if e.get("base_name")
        and not is_cloud_setting_id(e.get("base_name"))
        and e.get("source") != "override"
    ]
    if linked:
        counts: dict[str, int] = {}
        for name in linked:
            counts[name] = counts.get(name, 0) + 1
        return max(counts, key=counts.get)
    for entry in profiles.values():
        base = (entry.get("base_name") or "").strip()
        if base and not is_cloud_setting_id(base):
            return base
    return ""


def canonical_printer_model_token(raw: str | None) -> str:
    """Normalize a Bambuddy/machine string to an exact model token (e.g. P2S, H2C)."""
    if not raw:
        return ""
    s = str(raw).strip()
    for prefix in ("Bambu Lab ", "Bambu ", "BBL "):
        if s.upper().startswith(prefix.upper()):
            s = s[len(prefix) :].strip()
    # Strip trailing nozzle fragments (e.g. "H2C 0.4" or "H2C 0.4 nozzle").
    s = re.sub(
        r"\s+\d+(?:\.\d+)?(?:\s*mm)?(?:\s*nozzle)?\s*$",
        "",
        s,
        flags=re.IGNORECASE,
    ).strip()
    upper = s.upper()
    if upper in _SPACED_MODEL_ALIASES:
        return _SPACED_MODEL_ALIASES[upper]
    # Collapse spaces so "A1 Mini" → A1MINI before shorter tokens (e.g. A1) match.
    nospace = re.sub(r"\s+", "", upper)
    for token in sorted(_KNOWN_PRINTER_MODEL_TOKENS, key=len, reverse=True):
        if nospace == token.upper():
            return token
    known = {t.upper() for t in _KNOWN_PRINTER_MODEL_TOKENS}
    for token in sorted(_KNOWN_PRINTER_MODEL_TOKENS, key=len, reverse=True):
        if upper == token:
            return token
        if re.search(rf"(?:^|\s){re.escape(token)}(?:\s|$)", upper):
            return token
    parts = upper.split()
    if parts:
        last = parts[-1]
        if last in known:
            return last
    return upper if upper in known else ""


def parse_cloud_preset_name(name: str) -> tuple[str, str, float | None]:
    """Parse cloud preset names with ``@BBL`` or ``@Bambu Lab`` model suffixes."""
    base = extract_profile_base_name(name)
    stripped = name.strip()
    m = _CLOUD_PRESET_SUFFIX_RE.search(stripped)
    if m:
        model_token = canonical_printer_model_token(m.group(1))
        nozzle = _float_or_none(m.group(2))
        return base, model_token, nozzle
    m2 = _CLOUD_PRESET_MODEL_RE.search(stripped)
    if m2:
        model_token = canonical_printer_model_token(m2.group(1))
        return base, model_token, None
    trailing_base, trailing_model = _split_trailing_model_token(stripped)
    if trailing_model:
        return trailing_base, trailing_model, None
    return base, "", None


def _known_model_tokens() -> set[str]:
    return {t.upper() for t in _KNOWN_PRINTER_MODEL_TOKENS}


def _models_from_compatible_printers(preset: dict[str, Any]) -> set[str]:
    """Infer printer models from Bambu ``compatible_printers`` metadata."""
    compat = preset.get("compatible_printers")
    if not isinstance(compat, list):
        setting = preset.get("setting")
        if isinstance(setting, dict):
            compat = setting.get("compatible_printers")
    models: set[str] = set()
    if not isinstance(compat, list):
        return models
    known = _known_model_tokens()
    for entry in compat:
        token = canonical_printer_model_token(str(entry))
        if token and token.upper() in known:
            models.add(token.upper())
    return models


def infer_preset_models(preset: dict[str, Any]) -> set[str]:
    """Models this cloud preset applies to (name suffix + compatible_printers)."""
    name = (preset.get("name") or preset.get("displayName") or "").strip()
    models: set[str] = set()
    if name:
        _, preset_model, _ = parse_cloud_preset_name(name)
        if preset_model:
            models.add(preset_model.upper())
    models.update(_models_from_compatible_printers(preset))
    return models


def preset_applies_to_model(preset: dict[str, Any], model_token: str) -> bool:
    model_key = model_token.strip().upper()
    if not model_key:
        return True
    return model_key in infer_preset_models(preset)


def _parse_compatible_printer_nozzle(entry: str) -> float | None:
    """Extract nozzle mm from e.g. ``Bambu Lab H2C 0.4 nozzle``."""
    m = re.search(r"(\d+(?:\.\d+)?)\s*nozzle", str(entry), re.IGNORECASE)
    return _float_or_none(m.group(1)) if m else None


def effective_nozzle_mm(nozzle: float | None) -> float:
    """Stock model-only presets store ``None`` but mean the default 0.4 mm size."""
    return _STOCK_DEFAULT_NOZZLE_MM if nozzle is None else nozzle


def _model_only_preset_name(name: str) -> bool:
    stripped = name.strip()
    return " @" in stripped and not _CLOUD_PRESET_SUFFIX_RE.search(stripped)


def resolve_preset_nozzle(
    preset: dict[str, Any],
    name: str,
    model: str,
    parsed_nozzle: float | None,
) -> float | None:
    """Resolve nozzle for a cloud preset (name suffix, compatible_printers, stock default)."""
    if parsed_nozzle is not None:
        return parsed_nozzle
    compat = preset.get("compatible_printers")
    if not isinstance(compat, list):
        setting = preset.get("setting")
        if isinstance(setting, dict):
            compat = setting.get("compatible_printers")
    if isinstance(compat, list):
        for entry in compat:
            nz = _parse_compatible_printer_nozzle(str(entry))
            if nz is not None:
                return nz
    if model and _model_only_preset_name(name):
        code = (preset.get("code") or "").strip()
        # Stock builtins (GF*) on every model omit "0.4 nozzle" in the display name.
        if code.startswith("GF"):
            return _STOCK_DEFAULT_NOZZLE_MM
    return None


def _preset_index_order(preset: dict[str, Any]) -> int:
    """Process model-only names first; explicit nozzle names win on duplicate 0.4 keys."""
    name = (preset.get("name") or "").strip()
    return 1 if _CLOUD_PRESET_SUFFIX_RE.search(name) else 0


def _is_model_variant_code(code: str, name: str) -> bool:
    """True for per-model cloud entries (custom PFUS/GF or any @BBL/@Bambu Lab suffix)."""
    if " @Bambu" in name or " @BBL" in name:
        return True
    if code.startswith("PFUS"):
        return True
    return code.startswith("GF") and " @" in name


def build_variant_index_from_presets(
    presets: list[dict[str, Any]],
) -> dict[tuple[str, str, float | None], str]:
    """Build ``(base, model, nozzle) -> setting_id`` from cached cloud presets."""
    index: dict[tuple[str, str, float | None], str] = {}
    for preset in sorted(presets, key=_preset_index_order):
        code = (preset.get("code") or "").strip()
        name = (preset.get("name") or "").strip()
        if not code or not name or not _is_model_variant_code(code, name):
            continue
        base, name_model, nozzle = parse_cloud_preset_name(name)
        nozzle = resolve_preset_nozzle(preset, name, name_model, nozzle)
        if not base:
            continue
        models = infer_preset_models(preset) or ({name_model.upper()} if name_model else set())
        for model in models:
            index[(base.upper(), model, nozzle)] = code
    return index


def build_variant_groups_from_index(
    index: dict[tuple[str, str, float | None], str],
) -> dict[tuple[str, str], list[tuple[float | None, str]]]:
    """Group index entries by ``(base, model)`` with nozzles sorted ascending."""
    groups: dict[tuple[str, str], list[tuple[float | None, str]]] = {}
    for (base, model, nozzle), code in index.items():
        groups.setdefault((base, model), []).append((nozzle, code))
    for variants in groups.values():
        variants.sort(key=lambda x: (x[0] is None, x[0] if x[0] is not None else 0.0))
    return groups


def _nozzle_matches(a: float, b: float) -> bool:
    return a == b or round(a, 1) == round(b, 1)


def _nearest_standard_nozzle(nozzle_mm: float | None) -> float | None:
    if nozzle_mm is None:
        return _STOCK_DEFAULT_NOZZLE_MM
    for std in STANDARD_NOZZLE_MM:
        if _nozzle_matches(nozzle_mm, std):
            return std
    return None


def standard_nozzle_availability(
    groups: dict[tuple[str, str], list[tuple[float | None, str]]],
    base_name: str,
    model_token: str,
) -> dict[str, bool]:
    """Which standard sizes (0.2/0.4/0.6/0.8) exist in cloud for ``(base, model)``."""
    variants = groups.get(
        (base_name.strip().upper(), model_token.strip().upper()), []
    )
    available: set[float] = set()
    for raw_n, _ in variants:
        eff = effective_nozzle_mm(raw_n)
        for std in STANDARD_NOZZLE_MM:
            if _nozzle_matches(eff, std):
                available.add(std)
    return {f"{std:g}": std in available for std in STANDARD_NOZZLE_MM}


def _pick_closest_nozzle_variant(
    variants: list[tuple[float | None, str]],
    target: float | None,
) -> tuple[str | None, float | None, bool]:
    """Return ``(code, resolved_nozzle, exact_match)``."""
    if not variants:
        return None, None, False
    # Treat unresolved None nozzles as 0.4 mm stock default for matching.
    normalized = [
        (effective_nozzle_mm(n), c, n) for n, c in variants
    ]
    with_nozzle = [(eff, c, raw) for eff, c, raw in normalized if eff is not None]
    if target is not None and with_nozzle:
        for eff, c, raw in with_nozzle:
            if _nozzle_matches(eff, target):
                return c, raw if raw is not None else eff, True
        best = min(with_nozzle, key=lambda x: abs(x[0] - target))
        return best[1], best[2] if best[2] is not None else best[0], False
    for pref in (0.4, 0.2, 0.6, 0.8):
        for eff, c, raw in with_nozzle:
            if _nozzle_matches(eff, pref):
                return c, raw if raw is not None else eff, True
    if with_nozzle:
        eff, c, raw = with_nozzle[0]
        return c, raw if raw is not None else eff, False
    return variants[0][1], variants[0][0], False


def resolve_cloud_variant_detailed(
    index: dict[tuple[str, str, float | None], str],
    groups: dict[tuple[str, str], list[tuple[float | None, str]]],
    base_name: str,
    model_token: str,
    nozzle_mm: float | None = None,
) -> dict[str, Any]:
    """Resolve a cloud preset code with metadata for UI coverage badges."""
    empty: dict[str, Any] = {
        "code": None,
        "mapped": False,
        "nozzle_requested": nozzle_mm,
        "nozzle_resolved": None,
        "exact_nozzle": False,
        "fallback_nozzle": False,
        "standard_nozzles": standard_nozzle_availability(
            groups, base_name, model_token
        )
        if base_name and model_token
        else {f"{s:g}": False for s in STANDARD_NOZZLE_MM},
        "requested_nozzle_in_cloud": False,
    }
    if not base_name or not model_token:
        return empty
    base_key = base_name.strip().upper()
    model_key = model_token.strip().upper()
    group_key = (base_key, model_key)
    variants = groups.get(group_key, [])
    std_nozzles = standard_nozzle_availability(groups, base_name, model_token)
    req_std = _nearest_standard_nozzle(nozzle_mm)
    req_key = f"{req_std:g}" if req_std is not None else ""
    requested_in_cloud = bool(req_key and std_nozzles.get(req_key))
    if not variants:
        empty["standard_nozzles"] = std_nozzles
        empty["requested_nozzle_in_cloud"] = requested_in_cloud
        return empty
    code, resolved_nozzle, exact = _pick_closest_nozzle_variant(variants, nozzle_mm)
    if not code:
        empty["standard_nozzles"] = std_nozzles
        empty["requested_nozzle_in_cloud"] = requested_in_cloud
        return empty
    return {
        "code": code,
        "mapped": True,
        "nozzle_requested": nozzle_mm,
        "nozzle_resolved": resolved_nozzle,
        "exact_nozzle": exact,
        "fallback_nozzle": not exact and nozzle_mm is not None,
        "standard_nozzles": std_nozzles,
        "requested_nozzle_in_cloud": requested_in_cloud,
    }


def resolve_cloud_variant_from_index(
    index: dict[tuple[str, str, float | None], str],
    base_name: str,
    model_token: str,
    nozzle_mm: float | None = None,
    *,
    groups: dict[tuple[str, str], list[tuple[float | None, str]]] | None = None,
) -> str | None:
    """Look up a cloud variant; falls back to closest nozzle for the same model."""
    if groups is None:
        groups = build_variant_groups_from_index(index)
    detail = resolve_cloud_variant_detailed(
        index, groups, base_name, model_token, nozzle_mm
    )
    return detail.get("code")


def group_presets_by_base_name(
    presets: list[dict[str, Any]],
    *,
    model_token: str | None = None,
) -> list[dict[str, Any]]:
    """Dedupe cloud presets to one row per logical base name (optionally per model)."""
    model_key = model_token.strip().upper() if model_token else ""
    seen: dict[str, dict[str, Any]] = {}
    for preset in presets:
        code = (preset.get("code") or "").strip()
        name = (preset.get("name") or preset.get("displayName") or "").strip()
        if not code or not name:
            continue
        base = extract_profile_base_name(name)
        if not base:
            continue
        models_for = infer_preset_models(preset)
        if model_key:
            if not models_for or model_key not in models_for:
                continue
            preset_model = model_key
        else:
            _, parsed_model, _ = parse_cloud_preset_name(name)
            preset_model = next(iter(models_for), parsed_model or "")
        if not model_key and preset_model:
            dedupe_key = f"{base.upper()}|{preset_model.upper()}"
        else:
            dedupe_key = base.upper()
        if dedupe_key in seen:
            continue
        seen[dedupe_key] = {
            "code": code,
            "name": name,
            "displayName": base,
            "baseName": base,
            "model": preset_model or model_key,
            "isCustom": bool(preset.get("isCustom")),
        }
    return list(seen.values())


def filter_grouped_presets_for_model(
    grouped_presets: list[dict[str, Any]],
    groups: dict[tuple[str, str], list[tuple[float | None, str]]],
    model_token: str,
) -> list[dict[str, Any]]:
    """Keep only base names that have at least one indexed variant for ``model_token``."""
    model_key = model_token.strip().upper()
    compatible = {base for (base, model) in groups if model == model_key}
    return [
        p
        for p in grouped_presets
        if (p.get("baseName") or p.get("displayName") or "").upper() in compatible
        and (not p.get("model") or str(p.get("model")).upper() == model_key)
    ]


def uniform_variant_code(variants_by_model: dict[str, str]) -> str | None:
    """Return one PFUS only when every resolved model variant is identical."""
    codes = {str(c).strip() for c in variants_by_model.values() if c}
    if len(codes) == 1:
        return next(iter(codes))
    return None


def expected_cloud_preset_name(
    base_name: str, model_token: str, nozzle_mm: float | None = None
) -> str:
    """Cloud preset name the user should create when a variant is missing."""
    nozzle = nozzle_mm if nozzle_mm is not None else 0.4
    if nozzle == 0.4:
        return (
            f"{base_name} @BBL {model_token} 0.4 nozzle "
            f"(stock presets on any model may use @BBL {model_token} only)"
        )
    return f"{base_name} @BBL {model_token} {nozzle:g} nozzle"


def is_override_profile_source(source: str | None) -> bool:
    """True when a per-model row is an explicit override (not linked to default)."""
    return (source or "").strip().lower() in ("override", "manual")


def normalize_profile_source_for_filament(source: str | None) -> str:
    """Map spool/filament profile sources to filament storage conventions."""
    return "override" if is_override_profile_source(source) else "linked"


def normalize_profiles_for_filament_copy(
    profiles: dict[str, dict[str, str]],
) -> dict[str, dict[str, str]]:
    """Copy profile map for filament storage, normalizing override/manual sources."""
    out: dict[str, dict[str, str]] = {}
    for model, entry in profiles.items():
        base = (entry.get("base_name") or "").strip()
        if not base:
            continue
        model_key = str(model).strip().upper()
        out[model_key] = {
            "base_name": base,
            "source": normalize_profile_source_for_filament(entry.get("source")),
        }
    return out


def extract_profile_overrides(
    profiles: dict[str, dict[str, str]],
) -> dict[str, str]:
    """Per-model override base names keyed by canonical model token."""
    return {
        str(model).strip().upper(): str(entry["base_name"]).strip()
        for model, entry in profiles.items()
        if is_override_profile_source(entry.get("source"))
        and (entry.get("base_name") or "").strip()
    }


def effective_profile_base_for_model(
    profiles: dict[str, dict[str, str]],
    model: str,
    default_base: str,
) -> tuple[str, str]:
    """Return ``(base_name, kind)`` for one connected model token."""
    model_key = model.strip().upper()
    entry = profiles.get(model_key) or {}
    base = (entry.get("base_name") or "").strip()
    source = (entry.get("source") or "").strip()
    if is_override_profile_source(source) and base:
        return base, "override"
    if base:
        return base, "linked" if source != "legacy" else "linked"
    if default_base:
        return default_base.strip(), "linked"
    return "", "none"


def compute_profile_backfill_diff(
    *,
    connected_models: list[str],
    source_default: str,
    source_profiles: dict[str, dict[str, str]],
    target_default: str,
    target_profiles: dict[str, dict[str, str]],
) -> dict[str, Any]:
    """Compare spool source vs filament target across all connected printer models."""
    src_default = (source_default or "").strip()
    tgt_default = (target_default or "").strip()
    default_changes = bool(src_default and src_default != tgt_default)

    model_changes: list[dict[str, str]] = []
    for model in sorted({m.strip().upper() for m in connected_models if m}):
        src_base, src_kind = effective_profile_base_for_model(
            source_profiles, model, src_default
        )
        tgt_base, tgt_kind = effective_profile_base_for_model(
            target_profiles, model, tgt_default
        )
        if not src_base:
            continue
        if src_base != tgt_base or src_kind != tgt_kind:
            model_changes.append(
                {
                    "model": model,
                    "from_base": tgt_base,
                    "from_kind": tgt_kind,
                    "to_base": src_base,
                    "to_kind": src_kind,
                }
            )

    filament_already_matches = not default_changes and not model_changes
    return {
        "default_changes": default_changes,
        "model_changes": model_changes,
        "filament_already_matches": filament_already_matches,
    }
