"""Configure must use the spool's material/colour when a caller or status frame lacks them.

Without material_type, _send_assignment assumes PLA; a PETG tray code (SUN22001)
then fails the material check and the slot is configured Generic (GFL99/GFG99).
"""

import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

_PKG_DIR = Path(__file__).resolve().parents[1] / "bambuddy"
if "bambuddy" not in sys.modules:
    _pkg = types.ModuleType("bambuddy")
    _pkg.__path__ = [str(_PKG_DIR)]
    _pkg.__package__ = "bambuddy"
    sys.modules["bambuddy"] = _pkg

from bambuddy.driver import Driver  # noqa: E402

SPOOL = {
    "id": 134,
    "material_type": "PETG",
    "material_subgroup": "matte",
    "color": "F2752CFF",
    "bambu_idx": "SUN22001",
    "bambu_slicer_setting_id": "PFUSbccf9a9fdb9fd7",
    "bambuddy_spool_id": "142",
    "filament_id": 2365,
}


def _driver() -> Driver:
    driver = Driver.__new__(Driver)
    driver._filament_data_for_spool = AsyncMock(return_value=dict(SPOOL))
    return driver


@pytest.mark.asyncio
async def test_fill_spool_basics_adds_missing_material_and_colour():
    driver = _driver()
    sparse = {"id": 134, "bambu_idx": "SUN22001"}

    filled = await driver._fill_spool_basics(sparse)

    assert filled["material_type"] == "PETG"
    assert filled["color"] == "F2752CFF"
    assert filled["material_subgroup"] == "matte"
    assert filled["bambu_idx"] == "SUN22001"
    assert "material_type" not in sparse  # caller's dict untouched


@pytest.mark.asyncio
async def test_fill_spool_basics_keeps_caller_values():
    driver = _driver()
    full = {"id": 134, "material_type": "PLA", "color": "000000FF"}

    assert await driver._fill_spool_basics(full) is full
    driver._filament_data_for_spool.assert_not_awaited()


@pytest.mark.asyncio
async def test_fill_spool_basics_without_spool_id_is_noop():
    driver = _driver()
    data = {"bambu_idx": "SUN22001"}

    assert await driver._fill_spool_basics(data) is data
    driver._filament_data_for_spool.assert_not_awaited()


def _late_nfc_driver() -> Driver:
    driver = _driver()
    driver._slot_configure_inflight = {}
    driver._slot_params_cache = {}
    driver._slot_to_filaman_spool = {"0-0": 134}
    driver._resolve_setting_id_for_assign = AsyncMock(
        return_value="PFUSbccf9a9fdb9fd7"
    )
    driver._send_assignment = AsyncMock()
    return driver


@pytest.mark.asyncio
async def test_late_nfc_partial_tray_uses_spool_material_and_colour():
    driver = _late_nfc_driver()

    # Partial status frame: tray_info_idx only, no tray_type/tray_color.
    await driver._reconfigure_slot_with_profile(0, 0, "SUN22001", {})

    sent = driver._send_assignment.await_args.args[2]
    assert sent["material_type"] == "PETG"
    assert sent["color"] == "F2752CFF"
    assert sent["bambu_idx"] == "SUN22001"


@pytest.mark.asyncio
async def test_late_nfc_live_tray_values_win():
    driver = _late_nfc_driver()

    await driver._reconfigure_slot_with_profile(
        0, 0, "SUN22001", {"tray_type": "PETG", "tray_color": "112233FF"}
    )

    sent = driver._send_assignment.await_args.args[2]
    assert sent["material_type"] == "PETG"
    assert sent["color"] == "112233FF"
