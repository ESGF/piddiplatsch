from unittest.mock import patch

from piddiplatsch.monitoring.progress import BoundedProgress


@patch("piddiplatsch.monitoring.progress.tqdm")
def test_bounded_progress_tracks_and_renders_items(tqdm_cls):
    progress = BoundedProgress(title="publish handles", unit="handle", enabled=True, start=10)

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
