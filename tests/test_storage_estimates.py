import sqlite3

from click.testing import CliRunner

from piddiplatsch.catalog.sqlite import SQLiteCatalog
from piddiplatsch.cli import cli
from piddiplatsch.monitoring.estimates import estimate_table_sizes
from piddiplatsch.monitoring.health import database_health
from piddiplatsch.monitoring.status import read_status


def test_estimates_are_bounded_weighted_samples_and_never_read_dbstat(tmp_path):
    db = tmp_path / "piddi.db"
    with SQLiteCatalog("small", db):
        pass
    with sqlite3.connect(db) as connection:
        for project, size in (("small", 1000), ("large", 10000)):
            connection.executemany(
                "INSERT INTO catalog_items VALUES (?, 'collection', ?, 'dataset', 1, zeroblob(?), 'now')",
                [(project, str(index), size) for index in range(100)],
            )
        connection.commit()
        statements = []
        connection.set_trace_callback(statements.append)

        def authorize(action, table, *args):
            if action == sqlite3.SQLITE_READ and table == "dbstat":
                return sqlite3.SQLITE_DENY
            return sqlite3.SQLITE_OK

        connection.set_authorizer(authorize)
        health = database_health(connection, fast=True)
        estimates = estimate_table_sizes(connection, health)
        assert estimates["estimated"]
        assert (
            estimates["allocated_bytes"]
            == connection.execute("PRAGMA page_count").fetchone()[0]
            * connection.execute("PRAGMA page_size").fetchone()[0]
        )
        catalog = next(
            row for row in estimates["tables"] if row["name"] == "catalog_items"
        )
        assert catalog["row_count"] == 200
        assert catalog["sample_rows"] == 32
        assert catalog["table_bytes"] > 100 * 1000 + 100 * 10000
        assert catalog["index_bytes"] > 0
        assert catalog["total_bytes"] == catalog["table_bytes"] + catalog["index_bytes"]
        samples = [sql for sql in statements if "typeof" in sql]
        assert len(samples) == 2
        assert all(sql.endswith("LIMIT 16") for sql in samples)
        assert not any("dbstat" in sql for sql in statements)
    selected = read_status(db, project="small")
    assert [row["project"] for row in selected["health"]["projects"]] == ["small"]
    assert selected["table_sizes"] == estimates  # Sizes still cover every project.


def test_top_uses_estimates_for_once_and_json(tmp_path):
    db = tmp_path / "piddi.db"
    with SQLiteCatalog("small", db):
        pass

    for option in ("--once", "--json"):
        result = CliRunner().invoke(cli, ["top", "--db", str(db), option])
        assert result.exit_code == 0, result.output
        assert (
            "~estimated" if option == "--once" else '"estimated": true'
        ) in result.output
