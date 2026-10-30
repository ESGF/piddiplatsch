"""Read-only storage and backlog checks; no catalog migrations or archive scans."""

import json
import sqlite3
from contextlib import closing
from pathlib import Path

from piddiplatsch.config import config
from piddiplatsch.monitoring.disk import disk_status


def read_health(project: str | None = None) -> dict:
    output = Path(config.get("consumer", "output_dir", "outputs"))
    catalogs = Path(config.get("catalog", "directory") or output / "catalog")
    batches = Path(config.get("batch", "directory") or output / "batches")
    selected = config.get("consumer", "projects", [])
    names = set(selected) if isinstance(selected, list) else set()
    names.update(path.stem for path in catalogs.glob("*.sqlite"))
    if batches.is_dir():
        names.update(path.name for path in batches.iterdir() if path.is_dir())
    if project:
        names = {project.casefold()}
    projects = []
    for name in sorted(names):
        if Path(name).name != name or name in {".", ".."}:
            continue
        row = {
            "project": name,
            "pending_items": None,
            "ready_batches": 0,
            "failed_batches": 0,
            "last_publication": None,
            "errors": [],
        }
        catalog = catalogs / f"{name}.sqlite"
        if catalog.is_file():
            try:
                with closing(
                    sqlite3.connect(
                        catalog.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.2
                    )
                ) as connection:
                    row["pending_items"] = connection.execute(
                        "SELECT COUNT(*) FROM pending_items"
                    ).fetchone()[0]
            except sqlite3.Error as exc:
                row["errors"].append(f"Catalog status unavailable: {exc}")
        _batch_health(batches / name, row)
        projects.append(row)
    return {"storage": disk_status(), "projects": projects}


def _batch_health(root: Path, row: dict):
    try:
        for path in (root / "ready").glob("*"):
            if not path.is_dir():
                continue
            row["ready_batches"] += 1
            try:
                if json.loads((path / "manifest.json").read_text()).get(
                    "publication_error"
                ):
                    row["failed_batches"] += 1
            except (OSError, ValueError, AttributeError) as exc:
                row["errors"].append(f"Batch status unavailable: {exc}")
        for path in root.glob(".building-*/manifest.json"):
            try:
                if json.loads(path.read_text()).get("status") == "failed":
                    row["failed_batches"] += 1
            except (OSError, ValueError, AttributeError) as exc:
                row["errors"].append(f"Batch status unavailable: {exc}")
        marker = root / "last-publication.json"
        if marker.is_file():
            value = json.loads(marker.read_text())
            if not isinstance(value, dict):
                raise ValueError(f"Invalid publication status: {marker}")
            row["last_publication"] = value
    except (OSError, ValueError) as exc:
        row["errors"].append(str(exc))
