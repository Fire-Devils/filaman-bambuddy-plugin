"""The slot card must show the preset configure sent, not the shared spool name."""

import sys
import types
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

# Load the plugin package without bambuddy/__init__.py, which imports the
# copy installed under app.plugins. The driver still needs the FilaMan app.
_PKG_DIR = Path(__file__).resolve().parents[1] / "bambuddy"
if "bambuddy" not in sys.modules:
    _pkg = types.ModuleType("bambuddy")
    _pkg.__path__ = [str(_PKG_DIR)]
    _pkg.__package__ = "bambuddy"
    sys.modules["bambuddy"] = _pkg

from bambuddy.driver import Driver  # noqa: E402

H2D = "SUNLU PETG BASIC GEN2 @Bambu Lab H2D 0.4 nozzle"
H2D_ID = "PFUS7b88bfb7f44983"
H2C = "SUNLU PETG BASIC GEN2 @Bambu Lab H2C 0.4 nozzle"


class _Response:
    def raise_for_status(self) -> None:
        return None


def _driver(gen: int = 4) -> Driver:
    driver = Driver.__new__(Driver)
    driver._bambuddy_url = "http://bambuddy"
    driver._bambuddy_printer_id = 3
    driver._slot_configure_gen = {"128-0": gen}
    driver._debug_enabled = False
    driver._client = MagicMock()
    driver._client.put = AsyncMock(return_value=_Response())
    return driver


@pytest.mark.asyncio
async def test_successful_configure_labels_slot_with_sent_h2d_preset():
    driver = _driver()

    async def resolve(code: str) -> str:
        assert code == H2D_ID
        return H2D

    driver.resolve_preset_name = resolve  # type: ignore[method-assign]
    await driver._record_sent_slot_preset(128, 0, H2D_ID, expected_gen=4)

    driver._client.put.assert_awaited_once()
    url = driver._client.put.await_args.args[0]
    params = driver._client.put.await_args.kwargs["params"]
    assert url == "http://bambuddy/api/v1/printers/3/slot-presets/128/0"
    assert params == {
        "preset_id": H2D_ID,
        "preset_name": H2D,
        "preset_source": "cloud",
    }


@pytest.mark.asyncio
async def test_superseded_configure_does_not_relabel_the_slot():
    driver = _driver(gen=5)

    async def resolve(code: str) -> str:
        return H2D

    driver.resolve_preset_name = resolve  # type: ignore[method-assign]
    await driver._record_sent_slot_preset(128, 0, H2D_ID, expected_gen=4)
    driver._client.put.assert_not_called()


@pytest.mark.asyncio
async def test_tray_code_does_not_replace_the_slot_label():
    """SUN22001 plus the shared H2C name is what assignment stores today."""
    driver = _driver()

    async def resolve(code: str) -> str:
        return H2C

    driver.resolve_preset_name = resolve  # type: ignore[method-assign]
    await driver._record_sent_slot_preset(128, 0, "SUN22001", expected_gen=4)
    driver._client.put.assert_not_called()


@pytest.mark.asyncio
async def test_unknown_preset_name_leaves_the_existing_label():
    driver = _driver()

    async def resolve(code: str) -> str:
        return ""

    driver.resolve_preset_name = resolve  # type: ignore[method-assign]
    await driver._record_sent_slot_preset(128, 0, H2D_ID, expected_gen=4)
    driver._client.put.assert_not_called()
