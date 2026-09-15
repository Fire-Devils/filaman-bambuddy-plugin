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
