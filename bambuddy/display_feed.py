"""Fields the Display API cannot see on a Bambuddy printer-status frame.

The status payload has nozzle temperatures and remaining minutes. Elapsed
time, the job's filament weight, and the print queue live on the queue API.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _as_float(value: Any) -> float | None:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any) -> int | None:
    number = _as_float(value)
    return int(round(number)) if number is not None else None


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        started = value
    elif isinstance(value, (int, float)) or (isinstance(value, str) and value.strip().lstrip("-").replace(".", "", 1).isdigit()):
        stamp = float(value)
        if stamp > 10_000_000_000:
            stamp /= 1000
        started = datetime.fromtimestamp(stamp, timezone.utc)
    else:
        text = str(value).strip().replace("Z", "+00:00")
        try:
            started = datetime.fromisoformat(text)
        except ValueError:
            return None
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    return started.astimezone(timezone.utc)


def _job_name(item: dict[str, Any]) -> str:
    return str(item.get("archive_name") or item.get("library_file_name") or "").strip()


def queue_display_fields(
    items: list[Any] | None,
    printer_id: int,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Pending depth and the current queue item's elapsed time and filament.

    Only rows assigned to this printer count. A model-wide "any H2D" item
    still has a null printer id, so it is not this printer's queue.
    ``depth`` is jobs waiting, not the one already printing.
    """
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    mine = [
        item
        for item in items or []
        if isinstance(item, dict) and item.get("printer_id") == printer_id
    ]
    printing = next((item for item in mine if item.get("status") == "printing"), None)
    pending = [item for item in mine if item.get("status") == "pending"]

    elapsed_seconds = None
    filament_grams = None
    if printing is not None:
        filament_grams = _as_float(printing.get("filament_used_grams"))
        started = _parse_time(printing.get("started_at"))
        if started is not None:
            elapsed_seconds = max(0, int((now - started).total_seconds()))

    nxt = None
    if pending:
        item = pending[0]
        nxt = {
            "name": _job_name(item),
            "filament_grams": _as_float(item.get("filament_used_grams")),
            "print_time_seconds": _as_int(item.get("print_time_seconds")),
        }
    return {
        "elapsed_seconds": elapsed_seconds,
        "filament_grams": filament_grams,
        "queue": {"depth": len(pending), "next": nxt},
    }
