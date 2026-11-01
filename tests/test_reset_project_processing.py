"""The operator reset preserves source records and isolates project output."""

import json
import sqlite3
from contextlib import closing

import pytest
from click.testing import CliRunner

from piddiplatsch.catalog.models import CatalogEntry, KafkaCheckpoint
from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.config.config import Config
from piddiplatsch.monitoring.stats import SQLiteReporter
from piddiplatsch.runners import reset
from piddiplatsch.runners.sync import _worker_lock


@pytest.fixture
def setup(tmp_path):
    settings = Config()
    settings._set("consumer", "output_dir", str(tmp_path))
    settings._set("stats", "db_path", str(tmp_path / "stats.sqlite"))
    for project in ("cmip6plus", "cmip7"):
        with SQLiteCatalog(
            project, tmp_path / "catalog" / f"{project}.sqlite"
        ) as catalog:
            with catalog.transaction() as transaction:
                for number in range(2):
                    transaction.put_item(
                        CatalogEntry(
                            project,
                            {"id": f"{project}.item.v{number}"},
                            f"{project}.item",
                            number,
                        )
                    )
                transaction.set_checkpoint(KafkaCheckpoint("topic", 0, 123))
            if project == "cmip6plus":
                state = catalog.pending_changes()
                catalog.acknowledge_mapped(state.generation, state.sequence)
        for folder in (tmp_path / "batches" / project, tmp_path / project):
            folder.mkdir(parents=True)
            (folder / "example.jsonl").write_text(f"old {project}\n")
    reporter = SQLiteReporter(str(tmp_path / "stats.sqlite"))
    reporter.close()
    with closing(sqlite3.connect(tmp_path / "stats.sqlite")) as connection:
        for run_id, selected in (
            ("shared", ["cmip6plus", "cmip7"]),
            ("single", ["cmip6plus"]),
        ):
            connection.execute(
                "INSERT INTO monitor_runs (run_id, command, pid, hostname, started_at, heartbeat_at, status, selected_projects) "
                "VALUES (?, 'sync', 1, 'test', 'now', 'now', 'completed', ?)",
                (run_id, json.dumps(selected)),
            )
            for project in selected:
                connection.execute(
                    "INSERT INTO monitor_project_stats (run_id, project, handles, updated_at) VALUES (?, ?, 99, 'now')",
                    (run_id, project),
                )
                connection.execute(
                    "INSERT INTO monitor_samples "
                    "(run_id, project, sampled_at, consumed, routed, filtered, succeeded, skipped, failed, handles) "
                    "VALUES (?, ?, 'now', 1, 1, 0, 1, 0, 0, 99)",
                    (run_id, project),
                )
        connection.commit()
    return settings, tmp_path


def rows(path, table):
    with closing(sqlite3.connect(path)) as connection:
        return connection.execute(
            f"SELECT * FROM {table}"  # noqa: S608 - fixed test table names
        ).fetchall()


def test_reset_preserves_sources_and_checkpoints_and_only_clears_selected_project(
    setup,
):
    settings, root = setup
    selected = root / "catalog/cmip6plus.sqlite"
    other = root / "catalog/cmip7.sqlite"
    items, checkpoints = rows(selected, "items"), rows(selected, "checkpoints")
    other_pending = rows(other, "pending_items")
    generation = rows(selected, "mapping_state")[0][1]
    count, backup = reset.reset_processing(settings, "CMIP6Plus")
    assert count == 2
    assert rows(selected, "items") == items
    assert rows(selected, "checkpoints") == checkpoints
    assert len(rows(selected, "pending_items")) == 2
    assert rows(selected, "mapping_state")[0][1] != generation
    assert rows(other, "pending_items") == other_pending
    assert len(rows(other, "items")) == 2
    assert not (root / "batches/cmip6plus").exists()
    assert not (root / "cmip6plus").exists()
    assert (root / "batches/cmip7/example.jsonl").read_text() == "old cmip7\n"
    assert (root / "cmip7/example.jsonl").read_text() == "old cmip7\n"
    assert (backup / "batches/example.jsonl").read_text() == "old cmip6plus\n"
    assert (backup / "project-output/example.jsonl").read_text() == "old cmip6plus\n"
    assert rows(backup / "catalog.sqlite", "items") == items
    assert rows(backup / "catalog.sqlite", "pending_items") == []
    assert len(rows(backup / "monitor.sqlite", "monitor_project_stats")) == 3
    statistics = rows(root / "stats.sqlite", "monitor_project_stats")
    assert len(statistics) == 1 and statistics[0][1] == "cmip7"
    samples = rows(root / "stats.sqlite", "monitor_samples")
    assert len(samples) == 1 and samples[0][1] == "cmip7"
    assert [row[0] for row in rows(root / "stats.sqlite", "monitor_runs")] == ["shared"]


def test_reset_refuses_running_sync(setup):
    settings, root = setup
    with _worker_lock(root / "batches") as acquired:
        assert acquired
        with pytest.raises(RuntimeError, match="Sync is running"):
            reset.reset_processing(settings, "cmip6plus")
    assert (root / "batches/cmip6plus/example.jsonl").is_file()
    assert not (root / "reset-backups").exists()


def test_reset_rolls_back_and_restores_output_after_failure(setup, monkeypatch):
    settings, root = setup
    before = rows(root / "catalog/cmip6plus.sqlite", "mapping_state")

    def fail(*args):
        raise OSError("injected failure")

    monkeypatch.setitem(reset.__dict__, "_clear_project_statistics", fail)
    with pytest.raises(OSError, match="injected failure"):
        reset.reset_processing(settings, "cmip6plus")
    assert rows(root / "catalog/cmip6plus.sqlite", "mapping_state") == before
    assert rows(root / "catalog/cmip6plus.sqlite", "pending_items") == []
    assert len(rows(root / "stats.sqlite", "monitor_project_stats")) == 3
    assert (root / "batches/cmip6plus/example.jsonl").is_file()
    assert (root / "cmip6plus/example.jsonl").is_file()


def test_preview_does_not_modify_files(setup):
    _settings, root = setup
    config_file = root / "reset.toml"
    config_file.write_text(
        f'[consumer]\noutput_dir = "{root}"\n[stats]\ndb_path = "{root / "stats.sqlite"}"\n'
    )
    original = {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}
    result = CliRunner().invoke(
        cli, ["--config", str(config_file), "reset", "--project", "cmip6plus"]
    )
    assert result.exit_code == 0, result.output
    assert "Preview only" in result.output
    assert original == {
        path: path.read_bytes() for path in root.rglob("*") if path.is_file()
    }


def test_reset_refuses_archive_layout_containing_catalog(setup):
    settings, root = setup
    settings._set("catalog", "directory", str(root / "cmip6plus/catalog"))
    with pytest.raises(ValueError, match="protected path"):
        reset.reset_processing(settings, "cmip6plus")
    assert not (root / "reset-backups").exists()


def test_cli_apply_uses_configured_paths_without_publishing(setup, monkeypatch):
    _settings, root = setup
    config_file = root / "reset.toml"
    config_file.write_text(
        f'[consumer]\noutput_dir = "{root}"\n[stats]\ndb_path = "{root / "stats.sqlite"}"\n'
        '[logging]\nfile = "/nonexistent/reset-test/log"\n'
    )

    def no_publication(*args, **kwargs):
        raise AssertionError("Reset must not publish Handles")

    monkeypatch.setattr("piddiplatsch.runners.sync.HandlePublisher.run", no_publication)
    result = CliRunner().invoke(
        cli,
        ["--config", str(config_file), "reset", "--project", "cmip6plus", "--apply"],
    )
    assert result.exit_code == 0, result.output
    assert "Queued 2 existing cmip6plus items" in result.output
    assert len(rows(root / "catalog/cmip6plus.sqlite", "pending_items")) == 2
    assert rows(root / "catalog/cmip6plus.sqlite", "checkpoints")[0][2] == 123
    assert len(list((root / "reset-backups").iterdir())) == 1


@pytest.mark.parametrize(
    "options, message, exit_code",
    [
        ([], "Missing option '--project'", 2),
        (["--project", "not-a-project"], "Error:", 1),
        (["--project", "cmip6plus", "--apply"], "Catalog not found", 1),
    ],
)
def test_cli_reset_reports_input_errors(options, message, exit_code):
    result = CliRunner().invoke(cli, ["reset", *options])
    assert result.exit_code == exit_code, result.output
    assert message in result.output
    assert "Traceback" not in result.output
