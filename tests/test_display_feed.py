import importlib.util
from datetime import datetime, timezone
from pathlib import Path

_MODULE_PATH = Path(__file__).resolve().parents[1] / "bambuddy" / "display_feed.py"
_SPEC = importlib.util.spec_from_file_location("bambuddy_display_feed", _MODULE_PATH)
assert _SPEC is not None and _SPEC.loader is not None
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)

queue_display_fields = _MODULE.queue_display_fields

NOW = datetime(2026, 9, 27, 15, 0, tzinfo=timezone.utc)


def test_queue_depth_is_pending_jobs_for_this_printer():
    items = [
        {
            "printer_id": 3,
            "status": "printing",
            "position": 0,
            "archive_name": "benchy",
            "started_at": "2026-09-27T14:30:00+00:00",
            "filament_used_grams": 42.5,
            "print_time_seconds": 3600,
        },
        {
            "printer_id": 3,
            "status": "pending",
            "position": 1,
            "library_file_name": "lid",
            "filament_used_grams": 12,
            "print_time_seconds": 900,
        },
        {"printer_id": 3, "status": "pending", "position": 2, "archive_name": "later"},
        {"printer_id": None, "status": "pending", "archive_name": "any H2D"},
        {"printer_id": 2, "status": "pending", "archive_name": "other printer"},
        {"printer_id": 3, "status": "cancelled", "archive_name": "old"},
    ]
    out = queue_display_fields(items, printer_id=3, now=NOW)
    assert out["elapsed_seconds"] == 30 * 60
    assert out["filament_grams"] == 42.5
    assert out["queue"]["depth"] == 2
    assert out["queue"]["next"]["name"] == "lid"
    assert out["queue"]["next"]["filament_grams"] == 12
    assert out["queue"]["next"]["print_time_seconds"] == 900


def test_empty_queue_has_no_current_job_fields():
    out = queue_display_fields([], printer_id=3, now=NOW)
    assert out["elapsed_seconds"] is None
    assert out["filament_grams"] is None
    assert out["queue"] == {"depth": 0, "next": None}
