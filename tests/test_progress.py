from unittest.mock import patch

from piddiplatsch.monitoring.progress import (
    BoundedProgress,
    NoOpProgress,
    Progress,
    get_progress,
)


def test_progress_factory_selects_streaming_or_bounded_progress():
    stream = get_progress(title="consume", use_tqdm=False, stream=True)
    bounded = get_progress(
        title="publish handles",
        use_tqdm=False,
        stream=False,
        unit="handle",
        start=10,
    )

    assert isinstance(stream, NoOpProgress)
    assert isinstance(bounded, BoundedProgress)
    assert bounded.unit == "handle"
    assert bounded.start == 10


@patch("piddiplatsch.monitoring.progress.tqdm")
def test_bounded_progress_tracks_and_renders_items(tqdm_cls):
    progress = BoundedProgress(
        title="publish handles", unit="handle", enabled=True, start=10
    )

    with progress:
        progress.update(total=2, position=11, ok=True)
        progress.update(total=2, position=12, ok=False)

    tqdm_cls.assert_called_once_with(
        total=2,
        desc="publish handles 11-12",
        unit="handle",
        dynamic_ncols=True,
    )
    assert progress.position == 12
    assert progress.succeeded == 1
    assert progress.failed == 1
    assert tqdm_cls.return_value.update.call_count == 2
    tqdm_cls.return_value.close.assert_called_once_with()


@patch("piddiplatsch.monitoring.progress.tqdm")
def test_disabled_bounded_progress_tracks_without_rendering(tqdm_cls):
    progress = BoundedProgress(title="retry files", unit="file", enabled=False)

    with progress:
        progress.update(total=1, position=1, ok=True)

    assert progress.position == 1
    assert progress.succeeded == 1
    tqdm_cls.assert_not_called()


@patch("piddiplatsch.monitoring.progress.tqdm")
def test_stream_progress_close_is_idempotent(tqdm_cls):
    progress = Progress(title="consume")

    progress.close()
    progress.close()

    tqdm_cls.return_value.close.assert_called_once_with()


@patch("piddiplatsch.monitoring.progress.tqdm")
def test_publication_progress_supports_unknown_total(tqdm_cls):
    with BoundedProgress(
        title="publish handles", unit="handle", enabled=True, start=10
    ) as progress:
        progress.update(total=None, position=12, ok=True)
        progress.update(total=None, position=11, ok=False)
    tqdm_cls.assert_called_once_with(
        total=None, desc="publish handles", unit="handle", dynamic_ncols=True
    )
    assert progress.position == 12
    assert progress.succeeded == 1
    assert progress.failed == 1
    assert tqdm_cls.return_value.update.call_count == 2


@patch("piddiplatsch.monitoring.progress.tqdm")
def test_harvest_progress_reports_catalog_work_and_not_handles(tqdm_cls):
    from piddiplatsch.monitoring.stats import stats
    from piddiplatsch.result import ProcessingResult

    stats.record_result(ProcessingResult("post", success=True, catalog_written=True))
    stats.record_result(
        ProcessingResult(
            "remote", success=True, catalog_written=True, catalog_refreshed=True
        )
    )
    stats.record_result(ProcessingResult("replayed", success=True, filtered=True))
    stats.patch()
    progress = Progress(title="harvest")
    description = progress._format_desc()
    assert "events:3" in description
    assert "writes:2" in description
    assert "duplicates:1" in description
    assert "patches:1" in description
    assert "refreshes:1" in description
    assert "handles" not in description
    assert "replica" not in description
    assert "retracted" not in description
    progress.close()


@patch("piddiplatsch.monitoring.progress.tqdm")
def test_raw_harvest_has_no_catalog_counters(tqdm_cls):
    progress = Progress(title="harvest", mode="harvest_raw")
    description = progress._format_desc()
    assert "events:" in description
    assert "writes:" not in description
    assert "handles:" not in description
    progress.close()


@patch("piddiplatsch.monitoring.progress.tqdm")
def test_map_progress_counts_failed_items_and_prepared_handles(tqdm_cls):
    from piddiplatsch.monitoring.stats import stats
    from piddiplatsch.result import ProcessingResult

    stats.record_result(ProcessingResult("mapped", success=True, num_handles=10))
    stats.record_result(ProcessingResult("bad", error="invalid item"))
    progress = Progress(title="map")
    description = progress._format_desc()
    assert "items:2" in description
    assert "handles:10" in description
    assert "errors:1" in description
    assert "last_error:" in description
    assert "patches:" not in description
    progress.close()


@patch("piddiplatsch.monitoring.progress.tqdm")
def test_sync_progress_can_switch_phases_after_publication(tqdm_cls):
    progress = Progress(title="sync")
    assert "checking pending items and ready batches" in progress._format_desc()
    progress.set_phase("sync cmip6 map")
    assert "items:" in progress._format_desc()
    progress.close()
    progress.set_phase("sync cmip7 map")
    assert not progress.closed
    assert tqdm_cls.call_count == 2
    progress.close()


@patch("piddiplatsch.monitoring.progress.tqdm")
def test_progress_refresh_does_not_add_a_trailing_colon(tqdm_cls, monkeypatch):
    progress = Progress(title="harvest", update_interval=1)
    monkeypatch.setattr(
        "piddiplatsch.monitoring.progress.time.monotonic",
        lambda: progress.last_update + 2,
    )
    progress.refresh()
    assert tqdm_cls.return_value.set_description_str.called
    tqdm_cls.return_value.set_description.assert_not_called()
    progress.close()
