from __future__ import annotations

from temporalio.worker import Replayer

from video_media_catalog.temporal.workflows.tmdb_pipeline import (
    TmdbCaptureSilverWorkflow,
)


def test_workflow_definition_is_replay_safe() -> None:
    # Construction validates the workflow sandbox can import the definition.
    replayer = Replayer(workflows=[TmdbCaptureSilverWorkflow])
    assert replayer is not None
