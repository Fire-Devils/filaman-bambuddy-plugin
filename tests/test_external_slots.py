import importlib.util
from pathlib import Path

import pytest


_MODULE_PATH = Path(__file__).resolve().parents[1] / "bambuddy" / "external_slots.py"
_SPEC = importlib.util.spec_from_file_location("bambuddy_external_slots", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

canonical_external_tray_id = _MODULE.canonical_external_tray_id
external_slot_index = _MODULE.external_slot_index


def test_physical_external_trays_map_to_bambuddy_assignment_slots():
    assert canonical_external_tray_id(254) == 0
    assert canonical_external_tray_id(255) == 1
    assert external_slot_index(254) == "255-0"
    assert external_slot_index(255) == "255-1"


def test_already_canonical_external_slots_are_idempotent():
    assert canonical_external_tray_id(0) == 0
    assert canonical_external_tray_id(1) == 1


def test_unknown_external_tray_is_rejected():
    with pytest.raises(ValueError):
        canonical_external_tray_id(253)
