"""Read-only selection of a complete, ordered project replay suffix."""

import json
from dataclasses import dataclass
from datetime import UTC
from pathlib import Path


@dataclass
class ResetSelection:
    count: int
    first_id: int | None
    requested_from: str | None
    effective_from: str | None
    batch_ids: tuple[str, ...]
    moves: tuple[tuple[Path, Path], ...]
    copies: tuple[tuple[Path, Path], ...] = ()


def _manifests(root, project):
    result = []
    for path in sorted(root.rglob("manifest.json")):
        try:
            if path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
                raise ValueError("batch path leaves the project directory")
            manifest = json.loads(path.read_text())
            if manifest["project"] != project or not isinstance(
                manifest["batch_id"], str
            ):
                raise ValueError("batch identity/project mismatch")
            ids = tuple(row["id"] for row in manifest["outcomes"])
            if any(
                type(identity) is not int or identity < 1 for identity in ids
            ) or len(set(ids)) != len(ids):
                raise ValueError("invalid source work IDs")
            if not ids and manifest["status"] != "building":
                raise ValueError("sealed batch has no source work IDs")
            result.append((path.parent, manifest, ids))
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise ValueError(
                f"Cannot inspect batch for dated reset: {path}: {exc}"
            ) from exc
    return result


def _receipt(manifest, output, *, selected):
    value = manifest.get("receipt")
    if not value:
        return None
    if not isinstance(value, str):
        raise ValueError("Invalid publication receipt reference in batch manifest")
    path = Path(value)
    if not path.exists() and path.with_suffix(path.suffix + ".gz").exists():
        path = path.with_suffix(path.suffix + ".gz")
    if not path.exists():
        return None
    if selected and (path.is_symlink() or not path.resolve().is_relative_to(output)):
        raise ValueError(
            f"Cannot archive publication receipt outside output directory: {path}"
        )
    return path.resolve()


def _other_project_receipts(root, project, output, candidates):
    """Protect receipt logs referenced by another project's batch."""
    shared = set()
    if not candidates:
        return shared
    for other in root.iterdir():
        if other.name == project or not other.is_dir():
            continue
        if other.is_symlink():
            return set(candidates)
        for path in other.rglob("manifest.json"):
            try:
                receipt = _receipt(json.loads(path.read_text()), output, selected=False)
            except (OSError, ValueError, TypeError, AttributeError):
                # An unreadable unrelated batch must not lose its receipt log.
                return set(candidates)
            if receipt in candidates:
                shared.add(receipt)
    return shared


def _output_selection(manifests, selected, root, output):
    moves = [
        (path, Path("batches") / path.relative_to(root))
        for path, manifest, _ in manifests
        if manifest["batch_id"] in selected
    ]
    receipts = {}
    for _, manifest, _ in manifests:
        receipt = _receipt(manifest, output, selected=manifest["batch_id"] in selected)
        if receipt is not None:
            receipts.setdefault(receipt, set()).add(manifest["batch_id"])
    copies = []
    shared = _other_project_receipts(
        root.parent,
        root.name,
        output,
        {receipt for receipt, references in receipts.items() if references & selected},
    )
    for receipt, references in receipts.items():
        if not references & selected or any(
            receipt.is_relative_to(path) for path, _ in moves
        ):
            continue
        destination = Path("receipts") / receipt.relative_to(output)
        # Shared receipt logs remain available to unaffected batches.
        (copies if references - selected or receipt in shared else moves).append(
            (receipt, destination)
        )
    return tuple(moves), tuple(copies)


def _require_complete_batches(connection, project, selected, ranges, manifests):
    for _, manifest, ids in manifests:
        identity = manifest["batch_id"]
        if identity not in selected:
            continue
        if identity in ranges:
            complete = ranges[identity][2] == len(ids)
        else:
            complete = all(
                connection.execute(
                    "SELECT 1 FROM work_items WHERE project=? AND id=?",
                    (project, work_id),
                ).fetchone()
                for work_id in ids
            )
        if not complete:
            raise ValueError(
                f"Cannot reset complete batch {identity}: source work was removed by retention; restore the missing history first"
            )
    for identity in selected:
        summary = connection.execute(
            "SELECT items+invalid_items FROM stats WHERE kind='batch' AND project=? AND id=?",
            (project, identity),
        ).fetchone()
        if summary and identity in ranges and summary[0] != ranges[identity][2]:
            raise ValueError(
                f"Cannot reset complete batch {identity}: retained source work is incomplete"
            )


def select_reset(connection, project, output, batches, from_date=None):
    root = batches / project
    if from_date is None:
        count = connection.execute(
            "SELECT COUNT(*) FROM work_items WHERE project=?", (project,)
        ).fetchone()[0]
        moves = tuple(
            (path, Path(f"output-{index}"))
            for index, path in enumerate((root, output / project))
            if path.exists()
        )
        return ResetSelection(count, None, None, None, (), moves)
    if from_date.tzinfo is None:
        from_date = from_date.replace(tzinfo=UTC)
    cutoff = from_date.astimezone(UTC).isoformat()
    first = connection.execute(
        "SELECT MIN(id) FROM work_items WHERE project=? AND created_at>=?",
        (project, cutoff),
    ).fetchone()[0]
    if first is None:
        return ResetSelection(0, None, cutoff, cutoff, (), ())
    ranges = {
        identity: (low, high, count)
        for identity, low, high, count in connection.execute(
            "SELECT batch_id,MIN(id),MAX(id),COUNT(*) FROM work_items WHERE project=? AND batch_id IS NOT NULL GROUP BY batch_id",
            (project,),
        )
    }
    manifests = _manifests(root, project)
    bounds = [(identity, low, high) for identity, (low, high, _) in ranges.items()]
    bounds.extend(
        (manifest["batch_id"], min(ids), max(ids))
        for _, manifest, ids in manifests
        if ids
    )
    while True:
        expanded = min([first, *(low for _, low, high in bounds if high >= first)])
        if expanded == first:
            break
        first = expanded
    selected = {identity for identity, _, high in bounds if high >= first}
    _require_complete_batches(connection, project, selected, ranges, manifests)
    count, earliest = connection.execute(
        "SELECT COUNT(*),MIN(created_at) FROM work_items WHERE project=? AND id>=?",
        (project, first),
    ).fetchone()
    effective = min(cutoff, earliest)
    moves, copies = _output_selection(manifests, selected, root, output)
    return ResetSelection(
        count, first, cutoff, effective, tuple(sorted(selected)), moves, copies
    )
