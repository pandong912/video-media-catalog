"""Stable Temporal names for the TMDB research capture pipeline."""

TEMPORAL_NAMESPACE = "vw-media-catalog-research"
WORKFLOW_TASK_QUEUE = "vw-media-catalog-tmdb-v1"
SILVER_TASK_QUEUE = "vw-media-catalog-tmdb-silver-v1"

WORKFLOW_TYPE_TMDB_PIPELINE = "TmdbCaptureSilverWorkflow"

SCHEDULE_DAILY_CHANGES = "tmdb-daily-changes-v1"
SCHEDULE_MONTHLY_INVENTORY = "tmdb-monthly-inventory-v1"
BOOTSTRAP_WORKFLOW_ID = "tmdb-bootstrap-2026-10-07"

BOOTSTRAP_EXPORT_DATE = "2026-10-07"
BOOTSTRAP_WINDOW_START = "2026-09-24"
BOOTSTRAP_WINDOW_END = "2026-10-07"
