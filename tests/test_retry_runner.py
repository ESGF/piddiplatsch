import logging
from pathlib import Path

from piddiplatsch.persist.retry import RetryRunner
from piddiplatsch.result import FeedResult, RetryResult


def test_run_batch_progress_callback(monkeypatch, tmp_path: Path, caplog):
    # Prepare dummy files to be returned by find_retry_files
    file1 = tmp_path / "a.jsonl"
    file2 = tmp_path / "b.jsonl"
    file1.write_text("{}\n")
    file2.write_text("{}\n")

    # Patch find_retry_files to return our deterministic list
    from piddiplatsch.persist import retry as retry_mod

    monkeypatch.setattr(retry_mod, "find_retry_files", lambda paths: [file1, file2])

    # Instantiate runner with a temp failures dir
    failures_dir = tmp_path / "failures"
    failures_dir.mkdir(parents=True, exist_ok=True)

    runner = RetryRunner(
        projects=["cmip6"],
        failure_dir=failures_dir,
        delete_after=False,
        dry_run=True,
    )

    # Prepare different results for each file
    r1 = RetryResult(total=1, succeeded=1, failed=0, failure_files=set())
    new_failure = failures_dir / "r1" / "failed_items_2026-01-16.jsonl"
    new_failure.parent.mkdir(parents=True, exist_ok=True)
    new_failure.write_text("{}\n")
    r2 = RetryResult(total=2, succeeded=1, failed=1, failure_files={new_failure})

    results = [r1, r2]

    def fake_run_file(path: Path) -> RetryResult:
        return results.pop(0)

    # Patch the instance method to return our fake results
    monkeypatch.setattr(runner, "run_file", fake_run_file)

    # Capture progress callbacks
    progress_calls = []

    def progress(file: Path, idx: int, total: int, res: RetryResult) -> None:
        progress_calls.append((file, idx, total, res))

    # Enable logging capture for verbose messages
    caplog.set_level(logging.INFO)

    overall = runner.run_batch((tmp_path,), verbose=True, progress_callback=progress)

    # Verify progress callback was invoked for both files
    assert len(progress_calls) == 2
    assert progress_calls[0][0] == file1
    assert progress_calls[0][1] == 1
    assert progress_calls[0][2] == 2
    assert progress_calls[1][0] == file2
    assert progress_calls[1][1] == 2
    assert progress_calls[1][2] == 2

    # Verify aggregation
    assert overall.total == 3
    assert overall.succeeded == 2
    assert overall.failed == 1
    assert new_failure in overall.failure_files


def test_run_file_reports_appended_daily_failure(monkeypatch, tmp_path: Path):
    from piddiplatsch import consumer
    from piddiplatsch.persist import retry as retry_mod

    failure_dir = tmp_path / "failures"
    existing_failure = failure_dir / "r1" / "failed_items_2026-08-25.jsonl"
    existing_failure.parent.mkdir(parents=True)
    existing_failure.write_text("{}\n", encoding="utf-8")
    source = tmp_path / "source.jsonl"
    source.write_text("{}\n", encoding="utf-8")

    monkeypatch.setattr(retry_mod, "load_failed_messages", lambda _path: [("key", {})])

    def fail_and_append(*args, **kwargs):
        with existing_failure.open("a", encoding="utf-8") as stream:
            stream.write("{}\n")
        return FeedResult(total=1, failed=1)

    monkeypatch.setattr(consumer, "feed_messages_direct", fail_and_append)
    runner = RetryRunner(projects=["cmip6"], failure_dir=failure_dir)

    result = runner.run_file(source)

    assert result.failed == 1
    assert result.failure_files == {existing_failure}


def test_filtered_retry_is_not_deleted_as_success(monkeypatch, tmp_path: Path):
    from piddiplatsch import consumer
    from piddiplatsch.persist import retry as retry_mod

    source = tmp_path / "source.jsonl"
    source.write_text("{}\n", encoding="utf-8")
    failure_dir = tmp_path / "failures"
    failure_dir.mkdir()

    monkeypatch.setattr(retry_mod, "load_failed_messages", lambda _path: [("key", {})])
    monkeypatch.setattr(
        consumer,
        "feed_messages_direct",
        lambda *args, **kwargs: FeedResult(total=1, filtered=1),
    )
    runner = RetryRunner(
        projects=["cmip6"],
        failure_dir=failure_dir,
        delete_after=True,
    )

    result = runner.run_file(source)

    assert result.filtered == 1
    assert result.failed == 1
    assert source.exists()


def test_retry_uses_one_run_scoped_handle_filename(monkeypatch, tmp_path: Path):
    from piddiplatsch import consumer
    from piddiplatsch.persist import retry as retry_mod

    source = tmp_path / "source.jsonl"
    source.write_text("{}\n", encoding="utf-8")
    failure_dir = tmp_path / "failures"
    failure_dir.mkdir()
    output_file = tmp_path / "cmip6" / "handles" / "retry-batch.jsonl"
    output_file.parent.mkdir(parents=True)

    monkeypatch.setattr(retry_mod, "load_failed_messages", lambda _path: [("key", {})])

    def feed(*args, **kwargs):
        assert kwargs["handle_output_filename"] == "retry-batch.jsonl"
        output_file.write_text("{}\n", encoding="utf-8")
        return FeedResult(total=1, succeeded=1)

    monkeypatch.setattr(consumer, "feed_messages_direct", feed)
    runner = RetryRunner(
        projects=["cmip6"],
        failure_dir=failure_dir,
        handle_output_filename="retry-batch.jsonl",
    )
    runner.output_dir = tmp_path

    result = runner.run_file(source)

    assert result.handle_files == {output_file}
