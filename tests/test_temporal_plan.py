from __future__ import annotations

from datetime import date

import pytest

from video_media_catalog.temporal.models import CaptureUnit
from video_media_catalog.temporal.names import (
    BOOTSTRAP_EXPORT_DATE,
    BOOTSTRAP_WINDOW_END,
    BOOTSTRAP_WINDOW_START,
)
from video_media_catalog.temporal.plan import (
    plan_capture_units,
    plan_change_days,
    workflow_id_for,
)


def test_bootstrap_plans_inventory_and_fourteen_natural_days() -> None:
    units = plan_capture_units(mode="bootstrap")
    assert units[0].kind == "inventory"
    assert units[0].export_date == BOOTSTRAP_EXPORT_DATE
    change_units = [unit for unit in units if unit.kind == "changes"]
    assert len(change_units) == 14
    assert change_units[0].window_start == BOOTSTRAP_WINDOW_START
    assert change_units[-1].window_end == BOOTSTRAP_WINDOW_END
    assert all(unit.window_start == unit.window_end for unit in change_units)


def test_daily_empty_window_rejected() -> None:
    with pytest.raises(ValueError, match="window_end"):
        plan_change_days(date(2026, 10, 8), date(2026, 10, 7))


def test_daily_single_day_and_workflow_id() -> None:
    units = plan_capture_units(
        mode="daily",
        window_start="2026-10-07",
        window_end="2026-10-07",
    )
    assert len(units) == 1
    assert (
        workflow_id_for(
            mode="daily",
            window_start="2026-10-07",
            window_end="2026-10-07",
        )
        == "tmdb-daily-2026-10-07"
    )


def test_inventory_only_unit() -> None:
    units = plan_capture_units(mode="inventory-only", export_date="2026-10-07")
    assert units == (CaptureUnit(kind="inventory", export_date="2026-10-07"),)
