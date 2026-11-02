"""Rolling processing speeds from existing cumulative monitoring samples."""

from datetime import datetime, timedelta
from itertools import pairwise

WINDOWS = {"10m": 600, "30m": 1800, "1h": 3600}


def processing_rates(payloads, now: datetime, project: str | None = None) -> list[dict]:
    """Weight sample deltas by active processing time, across batch boundaries.

    Intervals crossing a window edge contribute proportionally. Gaps between
    runs are excluded; stalled intervals within a run still count. Never
    extrapolate beyond the last heartbeat or assume missing samples are zero.
    """
    totals = {}
    cutoff = now - timedelta(seconds=max(WINDOWS.values()))
    for payload in payloads:
        run = payload["run"]
        command = run["command"]
        if command not in {"map", "sync", "publish"}:
            continue
        heartbeat = datetime.fromisoformat(run["heartbeat_at"])
        if heartbeat <= cutoff:
            continue
        phase, counter = (
            ("publish", "handles") if command == "publish" else ("map", "succeeded")
        )
        grouped = {}
        # The current counters fill the gap since the last periodic sample.
        rows = payload["samples"] + [
            dict(row, sampled_at=run["heartbeat_at"]) for row in payload["projects"]
        ]
        for row in rows:
            name = row["project"]
            if project is None or name.casefold() == project.casefold():
                timestamp = datetime.fromisoformat(row["sampled_at"])
                grouped.setdefault(name, {})[timestamp] = row.get(counter, 0)
        for name, samples in grouped.items():
            for (start, previous), (end, current) in pairwise(sorted(samples.items())):
                duration = (end - start).total_seconds()
                if duration <= 0 or current < previous:
                    continue
                for label, seconds in WINDOWS.items():
                    overlap = (
                        min(end, now) - max(start, now - timedelta(seconds=seconds))
                    ).total_seconds()
                    if overlap <= 0:
                        continue
                    phases = totals.setdefault(name, {})
                    windows = phases.setdefault(phase, {})
                    values = windows.setdefault(label, [0.0, 0.0])
                    values[0] += (current - previous) * overlap / duration
                    values[1] += overlap
    result = []
    for name, phases in sorted(totals.items()):
        row = dict(project=name)
        for phase in ("map", "publish"):
            row[phase] = {}
            for label in WINDOWS:
                count, elapsed = phases.get(phase, {}).get(label, (0, 0))
                row[phase][label] = dict(
                    per_second=count / elapsed if elapsed else None,
                    active_seconds=elapsed,
                )
        result.append(row)
    return result
