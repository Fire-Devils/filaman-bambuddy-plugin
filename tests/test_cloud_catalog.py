import importlib.util
from pathlib import Path


_MODULE_PATH = Path(__file__).resolve().parents[1] / "bambuddy" / "cloud_catalog.py"
_SPEC = importlib.util.spec_from_file_location("bambuddy_cloud_catalog", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

should_replace_catalog = _MODULE.should_replace_catalog
preset_name_from_catalog = _MODULE.preset_name_from_catalog
pick_vendor_tray_code = _MODULE.pick_vendor_tray_code
catalog_has_cloud_presets = _MODULE.catalog_has_cloud_presets
infer_tray_code_family = _MODULE.infer_tray_code_family
tray_code_matches_material = _MODULE.tray_code_matches_material
normalize_material_family = _MODULE.normalize_material_family
late_nfc_conflicts_with_recent_send = _MODULE.late_nfc_conflicts_with_recent_send
preset_material_fits = _MODULE.preset_material_fits
presets_from_saved_names = _MODULE.presets_from_saved_names

GENERIC = {"GFL99", "GFG99", "GFB99"}
BUILTINS = [
    {"code": "GFG99", "name": "Generic PETG"},
    {"code": "GFG00", "name": "Bambu PETG Basic"},
]
CLOUD = BUILTINS + [
    {"code": "PFUS7b88bfb7f44983", "name": "SUNLU PETG BASIC GEN2 @Bambu Lab H2D 0.4 nozzle"},
]


def test_degraded_fetch_keeps_last_good_catalog():
    # /cloud/filaments returned 500 → only builtins came back.
    assert should_replace_catalog(CLOUD, BUILTINS, cloud_ok=False) is False


def test_successful_fetch_replaces_catalog():
    assert should_replace_catalog(CLOUD, CLOUD, cloud_ok=True) is True


def test_first_load_accepts_builtins_only():
    # Nothing cached yet: a builtins-only catalog is better than none.
    assert should_replace_catalog([], BUILTINS, cloud_ok=False) is True
    assert should_replace_catalog(BUILTINS, BUILTINS, cloud_ok=False) is True


def test_empty_fetch_never_replaces():
    assert should_replace_catalog(CLOUD, [], cloud_ok=True) is False
    assert should_replace_catalog([], [], cloud_ok=False) is False


def test_catalog_has_cloud_presets():
    assert catalog_has_cloud_presets(CLOUD) is True
    assert catalog_has_cloud_presets(BUILTINS) is False


def test_preset_name_from_catalog():
    by_code = {p["code"]: p for p in CLOUD}
    assert (
        preset_name_from_catalog(by_code, "PFUS7b88bfb7f44983")
        == "SUNLU PETG BASIC GEN2 @Bambu Lab H2D 0.4 nozzle"
    )
    assert preset_name_from_catalog(by_code, "PFUSunknown") == ""
    assert preset_name_from_catalog(by_code, None) == ""


def test_pick_vendor_tray_code_prefers_stored_vendor_code():
    assert pick_vendor_tray_code(("SUN22001", None), GENERIC) == "SUN22001"


def test_pick_vendor_tray_code_skips_generic_pfus_and_empty():
    # Generic and PFUS candidates never count; fall through to the last-sent code.
    assert (
        pick_vendor_tray_code(("GFG99", "PFUS7b88bfb7f44983", "", "SUN22001"), GENERIC)
        == "SUN22001"
    )
    assert pick_vendor_tray_code(("GFG99", None, ""), GENERIC) is None


NAMES = {
    "GFL99": "Generic PLA",
    "GFG99": "Generic PETG",
    "GFA15": "Bambu PLA Galaxy",
    "GFL06": "Fiberon PETG-ESD",
    "GFB00": "Bambu ABS",
    "GFB01": "Bambu ASA",
}
EXACT = {"GFL99": "PLA", "GFG99": "PETG", "GFA00": "PLA", "GFG00": "PETG", "GFB99": "ABS", "GFB98": "ASA"}


def test_sun_vendor_prefixes_are_family_specific():
    assert infer_tray_code_family("SUN20012") == "PLA"
    assert infer_tray_code_family("SUN22001") == "PETG"
    assert tray_code_matches_material("SUN20012", "PETG") is False
    assert tray_code_matches_material("SUN22001", "PETG") is True
    assert tray_code_matches_material("SUN22001", "PLA") is False
    assert tray_code_matches_material("SUN20012", "PLA+") is True


def test_gf_letter_and_name_overrides():
    assert infer_tray_code_family("GFA15", names=NAMES) == "PLA"
    assert infer_tray_code_family("GFG99", names=NAMES, exact=EXACT) == "PETG"
    # Fiberon uses GFL for PETG — name wins over the L=PLA letter.
    assert infer_tray_code_family("GFL06", names=NAMES) == "PETG"
    assert tray_code_matches_material("GFL06", "PETG", names=NAMES) is True
    assert tray_code_matches_material("GFL99", "PETG", names=NAMES, exact=EXACT) is False


def test_abs_asa_need_name_or_exact_map():
    assert infer_tray_code_family("GFB00", names=NAMES) == "ABS"
    assert infer_tray_code_family("GFB01", names=NAMES) == "ASA"
    assert infer_tray_code_family("GFB77") is None
    assert tray_code_matches_material("GFB77", "ABS") is True


def test_unknown_and_custom_codes_are_allowed():
    assert tray_code_matches_material("Pccd0d10", "PETG") is True
    assert tray_code_matches_material("", "PETG") is True
    assert tray_code_matches_material(None, "PLA") is True


def test_normalize_material_family():
    assert normalize_material_family("PLA+") == "PLA"
    assert normalize_material_family("PETG-CF") == "PETG"
    assert normalize_material_family("NYLON") == "PA"


def test_late_nfc_skips_previous_occupant_color_and_sku():
    # Spool #110: we sent grey SUN20010; the tray still reported the old gold SUN20013.
    sent = {"code": "SUN20010", "color": "757575FF", "ts": 1000.0}
    assert (
        late_nfc_conflicts_with_recent_send(
            sent,
            "SUN20013",
            "FDCC1AFF",
            now=1001.0,
            window=90.0,
            generic_codes=GENERIC,
        )
        is True
    )


def test_late_nfc_allows_generic_to_specific_when_color_matches():
    sent = {"code": "GFL99", "color": "757575FF", "ts": 1000.0}
    assert (
        late_nfc_conflicts_with_recent_send(
            sent,
            "SUN20010",
            "757575",
            now=1002.0,
            window=90.0,
            generic_codes=GENERIC,
        )
        is False
    )


def test_late_nfc_conflict_expires_after_window():
    sent = {"code": "SUN20010", "color": "757575FF", "ts": 1000.0}
    assert (
        late_nfc_conflicts_with_recent_send(
            sent,
            "SUN20013",
            "FDCC1AFF",
            now=1090.0,
            window=90.0,
            generic_codes=GENERIC,
        )
        is False
    )


def test_preset_material_fits():
    petg = "SUNLU PETG HS MATTE GEN2 @Bambu Lab H2C 0.4 nozzle"
    pla = "SUNLU PLA HS MATTE GEN2 @Bambu Lab H2C 0.4 nozzle"
    assert preset_material_fits(petg, "PLA") is False
    assert preset_material_fits(pla, "PLA+") is True
    assert preset_material_fits("Bambu ASA @BBL H2C", "ASA") is True
    assert preset_material_fits(pla, "") is None
    assert preset_material_fits("Generic PC-ABS @BBL H2C", "PC-ABS") is True
    assert preset_material_fits("Generic PPA-CF @BBL H2C", "PPA") is True


def test_presets_from_saved_names_keeps_cloud_ids_only():
    entries = presets_from_saved_names(
        {
            "PFUS4007809f1aa7a": "SUNLU PLA MATTE GEN2 @Bambu Lab H2C 0.4 nozzle",
            "GFSB01_23": "Bambu ASA @BBL H2C",
            "": "missing",
        }
    )
    assert [e["code"] for e in entries] == ["PFUS4007809f1aa7a"]
    assert entries[0]["name"].endswith("H2C 0.4 nozzle")
