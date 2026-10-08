"""Deterministic capture-unit planning with no I/O."""

from __future__ import annotations

from datetime import date, timedelta

from video_media_catalog.temporal.models import CaptureUnit, PipelineMode
from video_media_catalog.temporal.names import (
    BOOTSTRAP_EXPORT_DATE,
    BOOTSTRAP_WINDOW_END,
    BOOTSTRAP_WINDOW_START,
)


def _iso(value: date) -> str:
    return value.isoformat()


def _parse(value: str, *, label: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{label} must be an ISO date") from exc


def plan_change_days(window_start: date, window_end: date) -> tuple[date, ...]:
    if window_end < window_start:
        raise ValueError("window_end must be on or after window_start")
    if window_end - window_start > timedelta(days=13):
        raise ValueError("TMDB change window must be between 1 and 14 inclusive days")
    days: list[date] = []
    current = window_start
    while current <= window_end:
        days.append(current)
        current += timedelta(days=1)
    return tuple(days)


def plan_capture_units(
    *,
    mode: PipelineMode,
    export_date: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> tuple[CaptureUnit, ...]:
    units: list[CaptureUnit] = []
    if mode == "bootstrap":
        export = _parse(export_date or BOOTSTRAP_EXPORT_DATE, label="export_date")
        start = _parse(window_start or BOOTSTRAP_WINDOW_START, label="window_start")
        end = _parse(window_end or BOOTSTRAP_WINDOW_END, label="window_end")
        units.append(CaptureUnit(kind="inventory", export_date=_iso(export)))
        for day in plan_change_days(start, end):
            units.append(
                CaptureUnit(
                    kind="changes",
                    window_start=_iso(day),
                    window_end=_iso(day),
                )
            )
        return tuple(units)

    if mode == "inventory-only":
        if not export_date:
            raise ValueError("export_date is required for inventory-only")
        export = _parse(export_date, label="export_date")
        return (CaptureUnit(kind="inventory", export_date=_iso(export)),)

    if mode == "daily":
        if not window_start or not window_end:
            raise ValueError("window_start and window_end are required for daily")
        start = _parse(window_start, label="window_start")
        end = _parse(window_end, label="window_end")
        for day in plan_change_days(start, end):
            units.append(
                CaptureUnit(
                    kind="changes",
                    window_start=_iso(day),
                    window_end=_iso(day),
                )
            )
        return tuple(units)

    raise ValueError(f"unsupported pipeline mode: {mode}")


def workflow_id_for(
    *,
    mode: PipelineMode,
    export_date: str | None = None,
    window_start: str | None = None,
    window_end: str | None = None,
) -> str:
    if mode == "bootstrap":
        export = export_date or BOOTSTRAP_EXPORT_DATE
        return f"tmdb-bootstrap-{export}"
    if mode == "inventory-only":
        if not export_date:
            raise ValueError("export_date is required for inventory-only workflow id")
        return f"tmdb-inventory-{export_date}"
    if mode == "daily":
        if not window_start or not window_end:
            raise ValueError("daily workflow id requires window bounds")
        if window_start == window_end:
            return f"tmdb-daily-{window_start}"
        return f"tmdb-daily-{window_start}_{window_end}"
    raise ValueError(f"unsupported pipeline mode: {mode}")
