"""Unit tests for per-model cloud profile variant resolution."""

from profile_variants import (
    STANDARD_NOZZLE_MM,
    build_variant_groups_from_index,
    build_variant_index_from_presets,
    canonical_printer_model_token,
    compute_profile_backfill_diff,
    expected_cloud_preset_name,
    extract_profile_base_name,
    extract_profile_overrides,
    filter_grouped_presets_for_model,
    group_presets_by_base_name,
    infer_preset_models,
    is_override_profile_source,
    normalize_profiles_for_filament_copy,
    parse_cloud_preset_name,
    preset_applies_to_model,
    resolve_cloud_variant_detailed,
    resolve_cloud_variant_from_index,
    standard_nozzle_availability,
    uniform_variant_code,
)


SAMPLE_PRESETS = [
    {
        "code": "PFUS_P2S",
        "name": "SUNLU PLA PLUS GEN2 @Bambu Lab P2S 0.4 nozzle",
        "displayName": "SUNLU PLA PLUS GEN2 @Bambu Lab P2S 0.4 nozzle",
        "isCustom": False,
    },
    {
        "code": "PFUS_H2C",
        "name": "SUNLU PLA PLUS GEN2 @Bambu Lab H2C 0.4 nozzle",
        "displayName": "SUNLU PLA PLUS GEN2 @Bambu Lab H2C 0.4 nozzle",
        "isCustom": False,
    },
    {
        "code": "PFUS_P2_06",
        "name": "Generic PLA @Bambu Lab P2 0.6 nozzle",
        "displayName": "Generic PLA @Bambu Lab P2 0.6 nozzle",
        "isCustom": False,
    },
    {
        "code": "PFUS_P2S_06",
        "name": "SUNLU PLA PLUS GEN2 @Bambu Lab P2S 0.6 nozzle",
        "displayName": "SUNLU PLA PLUS GEN2 @Bambu Lab P2S 0.6 nozzle",
        "isCustom": False,
    },
]


def _index_and_groups():
    index = build_variant_index_from_presets(SAMPLE_PRESETS)
    groups = build_variant_groups_from_index(index)
    return index, groups


def test_extract_profile_base_name_strips_suffix() -> None:
    name = "SUNLU PLA PLUS GEN2 @Bambu Lab P2S 0.4 nozzle"
    assert extract_profile_base_name(name) == "SUNLU PLA PLUS GEN2"
    assert extract_profile_base_name("Generic PLA") == "Generic PLA"


def test_canonical_printer_model_token_exact() -> None:
    assert canonical_printer_model_token("Bambu Lab P2S") == "P2S"
    assert canonical_printer_model_token("H2C") == "H2C"
    assert canonical_printer_model_token("P2") == "P2"


def test_model_collision_p2_not_p2s() -> None:
    index, groups = _index_and_groups()
    base = "Generic PLA"
    assert resolve_cloud_variant_from_index(index, base, "P2", 0.6, groups=groups) == "PFUS_P2_06"
    assert resolve_cloud_variant_from_index(index, base, "P2S", 0.4) is None


def test_resolve_cloud_variant_exact_nozzle() -> None:
    index, groups = _index_and_groups()
    base = "SUNLU PLA PLUS GEN2"
    assert resolve_cloud_variant_from_index(index, base, "P2S", 0.4, groups=groups) == "PFUS_P2S"
    assert resolve_cloud_variant_from_index(index, base, "H2C", 0.4, groups=groups) == "PFUS_H2C"


def test_resolve_cloud_variant_closest_nozzle() -> None:
    index, groups = _index_and_groups()
    base = "SUNLU PLA PLUS GEN2"
    detail = resolve_cloud_variant_detailed(index, groups, base, "P2S", 0.45)
    assert detail["code"] == "PFUS_P2S"
    assert detail["exact_nozzle"] is False
    assert detail["fallback_nozzle"] is True
    detail06 = resolve_cloud_variant_detailed(index, groups, base, "P2S", 0.6)
    assert detail06["code"] == "PFUS_P2S_06"
    assert detail06["exact_nozzle"] is True


def test_resolve_cloud_variant_fallback_any_nozzle_same_model() -> None:
    index, groups = _index_and_groups()
    base = "SUNLU PLA PLUS GEN2"
    detail = resolve_cloud_variant_detailed(index, groups, base, "P2S", 0.8)
    assert detail["code"] == "PFUS_P2S_06"
    assert detail["fallback_nozzle"] is True


BBL_H2C_PRESETS = [
    {
        "code": "GFSA00_22",
        "name": "Bambu PLA Basic @BBL H2C",
        "displayName": "Bambu PLA Basic @BBL H2C",
        "isCustom": False,
        "setting": {"compatible_printers": ["Bambu Lab H2C 0.4 nozzle"]},
    },
    {
        "code": "GFSA00_02",
        "name": "Bambu PLA Basic @BBL H2C 0.2 nozzle",
        "displayName": "Bambu PLA Basic @BBL H2C 0.2 nozzle",
        "isCustom": False,
    },
    {
        "code": "GFSA00_16",
        "name": "Bambu PLA Basic @BBL H2C 0.4 nozzle",
        "displayName": "Bambu PLA Basic @BBL H2C 0.4 nozzle",
        "isCustom": False,
    },
    {
        "code": "PFUS_H2C_06",
        "name": "SUNLU PLA PLUS GEN2 @BBL H2C 0.6 nozzle",
        "displayName": "SUNLU PLA PLUS GEN2 @BBL H2C 0.6 nozzle",
        "isCustom": True,
    },
]


def test_parse_bbl_h2c_preset_name() -> None:
    base, model, nozzle = parse_cloud_preset_name("Bambu PLA Basic @BBL H2C 0.4 nozzle")
    assert base == "Bambu PLA Basic"
    assert model == "H2C"
    assert nozzle == 0.4
    base2, model2, nozzle2 = parse_cloud_preset_name("Bambu PLA Basic @BBL H2C")
    assert base2 == "Bambu PLA Basic"
    assert model2 == "H2C"
    assert nozzle2 is None


def test_group_presets_includes_bbl_stock_and_builtins() -> None:
    presets = SAMPLE_PRESETS + BBL_H2C_PRESETS + [
        {"code": "GFL99", "name": "Generic PLA", "displayName": "Generic PLA", "isCustom": False},
    ]
    grouped_h2c = group_presets_by_base_name(presets, model_token="H2C")
    names = {p["baseName"] for p in grouped_h2c}
    assert "Bambu PLA Basic" in names
    assert "SUNLU PLA PLUS GEN2" in names
    assert "Generic PLA" not in names
    grouped_p2s = group_presets_by_base_name(presets, model_token="P2S")
    assert any(p["baseName"] == "SUNLU PLA PLUS GEN2" for p in grouped_p2s)
    assert "Generic PLA" not in {p["baseName"] for p in grouped_p2s}


def test_variant_index_includes_gf_bbl_codes() -> None:
    presets = BBL_H2C_PRESETS
    index = build_variant_index_from_presets(presets)
    groups = build_variant_groups_from_index(index)
    detail = resolve_cloud_variant_detailed(
        index, groups, "Bambu PLA Basic", "H2C", 0.4
    )
    assert detail["code"] == "GFSA00_16"
    assert detail["exact_nozzle"] is True
    assert resolve_cloud_variant_from_index(
        index, "SUNLU PLA PLUS GEN2", "H2C", 0.6, groups=groups
    ) == "PFUS_H2C_06"


def test_stock_bbl_without_nozzle_suffix_resolves_as_04() -> None:
    """Real stock presets omit 0.4 mm from the name (see compatible_printers)."""
    presets = [
        {
            "code": "GFSA00_22",
            "name": "Bambu PLA Basic @BBL H2C",
            "setting": {"compatible_printers": ["Bambu Lab H2C 0.4 nozzle"]},
        },
        {
            "code": "GFSA00_02",
            "name": "Bambu PLA Basic @BBL H2C 0.2 nozzle",
        },
        {
            "code": "GFSA00_06",
            "name": "Bambu PLA Basic @BBL H2C 0.6 nozzle",
        },
    ]
    index = build_variant_index_from_presets(presets)
    groups = build_variant_groups_from_index(index)
    detail = resolve_cloud_variant_detailed(
        index, groups, "Bambu PLA Basic", "H2C", 0.4
    )
    assert detail["code"] == "GFSA00_22"
    assert detail["exact_nozzle"] is True
    assert detail["nozzle_resolved"] == 0.4
    assert detail["standard_nozzles"]["0.4"] is True
    assert detail["standard_nozzles"]["0.2"] is True
    assert detail["standard_nozzles"]["0.6"] is True
    assert detail["standard_nozzles"]["0.8"] is False
    assert detail["requested_nozzle_in_cloud"] is True


def test_stock_p2s_without_nozzle_suffix_same_as_h2c() -> None:
    presets = [
        {
            "code": "GFL00_99",
            "name": "Bambu PLA Basic @BBL P2S",
            "setting": {"compatible_printers": ["Bambu Lab P2S 0.4 nozzle"]},
        },
        {
            "code": "GFL00_06",
            "name": "Bambu PLA Basic @BBL P2S 0.6 nozzle",
        },
    ]
    index = build_variant_index_from_presets(presets)
    groups = build_variant_groups_from_index(index)
    detail = resolve_cloud_variant_detailed(
        index, groups, "Bambu PLA Basic", "P2S", 0.4
    )
    assert detail["code"] == "GFL00_99"
    assert detail["exact_nozzle"] is True
    assert detail["standard_nozzles"]["0.4"] is True
    assert detail["standard_nozzles"]["0.6"] is True
    assert detail["standard_nozzles"]["0.2"] is False


def test_standard_nozzle_availability_all_missing_without_variants() -> None:
    groups: dict = {}
    avail = standard_nozzle_availability(groups, "Custom PLA", "H2C")
    assert avail == {f"{s:g}": False for s in STANDARD_NOZZLE_MM}


def test_filter_grouped_presets_for_model() -> None:
    presets = BBL_H2C_PRESETS + SAMPLE_PRESETS
    grouped = group_presets_by_base_name(presets, model_token="H2C")
    index = build_variant_index_from_presets(presets)
    groups = build_variant_groups_from_index(index)
    filtered = filter_grouped_presets_for_model(grouped, groups, "H2C")
    names = {p["baseName"] for p in filtered}
    assert "Bambu PLA Basic" in names
    assert "SUNLU PLA PLUS GEN2" in names
    assert "Generic PLA" not in names
    grouped_p2s = group_presets_by_base_name(presets, model_token="P2S")
    filtered_p2s = filter_grouped_presets_for_model(grouped_p2s, groups, "P2S")
    p2s_names = {p["baseName"] for p in filtered_p2s}
    assert "SUNLU PLA PLUS GEN2" in p2s_names
    assert "Bambu PLA Basic" not in p2s_names


def test_filter_grouped_presets_does_not_leak_other_model_rows() -> None:
    """When both model rows share a base name, H2C filter must not return the P2S row."""
    presets = BBL_H2C_PRESETS + SAMPLE_PRESETS
    grouped_all = group_presets_by_base_name(presets, model_token=None)
    index = build_variant_index_from_presets(presets)
    groups = build_variant_groups_from_index(index)
    filtered_h2c = filter_grouped_presets_for_model(grouped_all, groups, "H2C")
    for row in filtered_h2c:
        assert row.get("model", "H2C").upper() == "H2C"
    assert not any(p["baseName"] == "SUNLU PLA PLUS GEN2" and p.get("model") == "P2S" for p in filtered_h2c)


def test_canonical_model_strips_nozzle_tail() -> None:
    assert canonical_printer_model_token("H2C 0.4") == "H2C"
    assert canonical_printer_model_token("Bambu Lab H2C 0.4 nozzle") == "H2C"


def test_infer_preset_models_from_compatible_printers() -> None:
    preset = {
        "code": "GFSA00_22",
        "name": "Bambu PLA Basic @BBL H2C",
        "compatible_printers": ["Bambu Lab H2C 0.4 nozzle"],
    }
    assert infer_preset_models(preset) == {"H2C"}
    assert preset_applies_to_model(preset, "H2C")
    assert not preset_applies_to_model(preset, "P2S")


def test_expected_cloud_preset_name_for_ui() -> None:
    expected = expected_cloud_preset_name("My Custom PLA", "H2C", 0.4)
    assert "My Custom PLA @BBL H2C" in expected
    assert "0.4" in expected


def test_parse_cloud_preset_name() -> None:
    base, model, nozzle = parse_cloud_preset_name(
        "SUNLU PLA PLUS GEN2 @Bambu Lab P2S 0.4 nozzle"
    )
    assert base == "SUNLU PLA PLUS GEN2"
    assert model == "P2S"
    assert nozzle == 0.4


def test_trailing_model_token_custom_preset_name() -> None:
    """Custom cloud presets may embed model without @Bambu Lab suffix."""
    preset = {
        "code": "PFUSb064508e73b480",
        "name": "Mads TPU95 P2S",
        "displayName": "Mads TPU95 P2S (Custom)",
        "isCustom": True,
    }
    base, model, nozzle = parse_cloud_preset_name(preset["name"])
    assert base == "Mads TPU95"
    assert model == "P2S"
    assert nozzle is None
    assert infer_preset_models(preset) == {"P2S"}

    grouped = group_presets_by_base_name([preset], model_token="P2S")
    assert len(grouped) == 1
    assert grouped[0]["baseName"] == "Mads TPU95"

    index = build_variant_index_from_presets([preset])
    assert index[("MADS TPU95", "P2S", None)] == "PFUSb064508e73b480"
    groups = build_variant_groups_from_index(index)
    filtered = filter_grouped_presets_for_model(grouped, groups, "P2S")
    assert len(filtered) == 1
    assert filtered[0]["baseName"] == "Mads TPU95"


def test_uniform_variant_code() -> None:
    assert uniform_variant_code({"P2S": "A", "H2C": "A"}) == "A"
    assert uniform_variant_code({"P2S": "A", "H2C": "B"}) is None
    assert uniform_variant_code({}) is None


def test_group_presets_by_base_name_filters_model() -> None:
    grouped = group_presets_by_base_name(SAMPLE_PRESETS, model_token="P2S")
    assert len(grouped) == 1
    assert grouped[0]["baseName"] == "SUNLU PLA PLUS GEN2"
    assert grouped[0]["displayName"] == "SUNLU PLA PLUS GEN2"


def test_is_override_profile_source() -> None:
    assert is_override_profile_source("override")
    assert is_override_profile_source("manual")
    assert not is_override_profile_source("linked")
    assert not is_override_profile_source(None)


def test_normalize_profiles_for_filament_copy() -> None:
    raw = {
        "P2S": {"base_name": "Generic PLA", "source": "manual"},
        "X1C": {"base_name": "Generic PLA", "source": "linked"},
    }
    out = normalize_profiles_for_filament_copy(raw)
    assert out["P2S"]["source"] == "override"
    assert out["X1C"]["source"] == "linked"


def test_extract_profile_overrides_any_connected_model() -> None:
    profiles = {
        "A1MINI": {"base_name": "Sunlu ABS", "source": "override"},
        "P1S": {"base_name": "Sunlu ABS", "source": "linked"},
    }
    assert extract_profile_overrides(profiles) == {"A1MINI": "Sunlu ABS"}


def test_compute_profile_backfill_diff_uses_connected_models() -> None:
    connected = ["P2S", "H2C", "X1C"]
    source_profiles = {
        "P2S": {"base_name": "Sunlu ABS", "source": "linked"},
        "H2C": {"base_name": "Sunlu ABS Pro", "source": "override"},
        "X1C": {"base_name": "Sunlu ABS", "source": "linked"},
    }
    target_profiles = {
        "P2S": {"base_name": "Generic PLA", "source": "linked"},
    }
    diff = compute_profile_backfill_diff(
        connected_models=connected,
        source_default="Sunlu ABS",
        source_profiles=source_profiles,
        target_default="Generic PLA",
        target_profiles=target_profiles,
    )
    assert diff["default_changes"] is True
    assert diff["filament_already_matches"] is False
    models = {c["model"] for c in diff["model_changes"]}
    assert models == {"P2S", "H2C", "X1C"}


def test_compute_profile_backfill_diff_already_matches() -> None:
    connected = ["P2S", "A1"]
    profiles = {
        "P2S": {"base_name": "Sunlu PETG", "source": "linked"},
        "A1": {"base_name": "Sunlu PETG Special", "source": "override"},
    }
    diff = compute_profile_backfill_diff(
        connected_models=connected,
        source_default="Sunlu PETG",
        source_profiles=profiles,
        target_default="Sunlu PETG",
        target_profiles=profiles,
    )
    assert diff["filament_already_matches"] is True
    assert diff["model_changes"] == []
