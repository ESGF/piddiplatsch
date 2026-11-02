"""Rough storage estimates from bounded row samples, without walking dbstat."""

import math


def _quote(name):
    return '"' + name.replace('"', '""') + '"'


def _value_bytes(name):
    column = _quote(name)
    return (
        f"CASE typeof({column}) WHEN 'null' THEN 0 "
        f"WHEN 'blob' THEN length({column}) WHEN 'text' THEN length(CAST({column} AS BLOB)) "
        "ELSE 8 END + 2"
    )


def estimate_table_sizes(connection, health):
    """Estimate records + indexes at 75% page occupancy using 16 rows/project.

    These are rough live-data footprints, not measured allocations. Sampling
    early rows can be biased when record sizes change over time.
    """
    page_size = connection.execute("PRAGMA page_size").fetchone()[0]
    allocated = page_size * connection.execute("PRAGMA page_count").fetchone()[0]
    free = page_size * connection.execute("PRAGMA freelist_count").fetchone()[0]
    tables = []
    for table in (
        "catalog_items",
        "work_items",
        "kafka_checkpoints",
        "stats",
        "outbox_batches",
        "handle_outbox",
    ):
        columns = connection.execute(f"PRAGMA table_info({_quote(table)})").fetchall()
        if not columns:
            continue  # Read-only monitoring also supports pre-upgrade databases.
        expressions = [_value_bytes(column[1]) for column in columns]
        table_expression = "8 + " + " + ".join(f"({expr})" for expr in expressions)
        index_expressions = []
        for index in connection.execute(
            f"PRAGMA index_list({_quote(table)})"
        ).fetchall():
            keys = connection.execute(
                f"PRAGMA index_info({_quote(index[1])})"
            ).fetchall()
            index_expressions.append(
                "16 + "
                + " + ".join(
                    f"({_value_bytes(key[2])})" for key in keys if key[2] is not None
                )
            )
        index_expression = " + ".join(f"({expr})" for expr in index_expressions) or "0"
        if table in {"catalog_items", "work_items"}:
            key = "total_items" if table == "catalog_items" else "work_items"
            groups = [(row["project"], row[key]) for row in health["projects"]]
        else:
            count = connection.execute(
                f"SELECT COUNT(*) FROM {_quote(table)}"  # noqa: S608 - fixed table allowlist
            ).fetchone()[0]
            groups = [(None, count)]
        table_payload = index_payload = sample_rows = row_count = 0
        for project, count in groups:
            if not count:
                continue
            query = f"SELECT {table_expression}, {index_expression} FROM {_quote(table)}"  # noqa: S608 - identifiers are quoted
            params = ()
            if project is not None:
                query += " WHERE project=?"
                params = (project,)
            samples = connection.execute(query + " LIMIT 16", params).fetchall()
            if samples:
                table_payload += count * sum(row[0] for row in samples) / len(samples)
                index_payload += count * sum(row[1] for row in samples) / len(samples)
            sample_rows += len(samples)
            row_count += count
        table_bytes = max(
            page_size, math.ceil(table_payload / 0.75 / page_size) * page_size
        )
        index_bytes = max(
            len(index_expressions) * page_size,
            math.ceil(index_payload / 0.75 / page_size) * page_size,
        )
        tables.append(
            dict(
                name=table,
                table_bytes=table_bytes,
                index_bytes=index_bytes,
                total_bytes=table_bytes + index_bytes,
                row_count=row_count,
                sample_rows=sample_rows,
            )
        )
    tables.sort(key=lambda row: (-row["total_bytes"], row["name"]))
    return dict(
        available=True,
        estimated=True,
        method="sampled row sizes; assumed 75% page occupancy",
        allocated_bytes=allocated,
        free_bytes=free,
        tables=tables,
    )
