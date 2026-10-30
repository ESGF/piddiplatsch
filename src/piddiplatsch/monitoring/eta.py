"""Publication estimates from recent successful writes and known local work."""

from datetime import datetime, timedelta


def publication_progress(run: dict, samples, now: datetime) -> dict:
    published = sum(row["handles"] for row in run["projects"])
    failed = sum(row["failed"] for row in run["projects"])
    total = run.get("expected_handles")
    remaining = max(0, total - published) if total is not None else None
    result = {
        "published_handles": published,
        "remaining_handles": remaining,
        "handles_per_second": None,
        "eta_seconds": None,
        "estimated_end": None,
        "reason": "waiting for speed samples",
    }
    if run["state"] != "healthy" or failed:
        result["reason"] = (
            "publication errors require retry"
            if failed
            else "publisher is not active or heartbeat is stale"
        )
        return result
    heartbeat = datetime.fromisoformat(run["heartbeat_at"])
    cutoff = heartbeat - timedelta(seconds=60)
    grouped = {}
    for sample in samples:
        timestamp = sample["sampled_at"]
        if datetime.fromisoformat(timestamp) >= cutoff:
            grouped[timestamp] = grouped.get(timestamp, 0) + sample["handles"]
    if not grouped:
        return result
    first = min(grouped)
    elapsed = (heartbeat - datetime.fromisoformat(first)).total_seconds()
    if elapsed < 5:
        return result
    rate = max(0, published - grouped[first]) / elapsed
    result["handles_per_second"] = rate
    if not rate:
        result["reason"] = "no recent publication progress"
    elif remaining is None:
        result["reason"] = "batch Handle total is unknown"
    else:
        result.update(
            eta_seconds=remaining / rate,
            estimated_end=(now + timedelta(seconds=remaining / rate)).isoformat(),
            reason=None,
        )
    return result


def _handles_per_item(runs, project):
    handles = items = count = 0
    for run in runs:
        if run["command"] not in {"map", "sync"}:
            continue
        for row in run["projects"]:
            if row["project"] == project and row["succeeded"]:
                handles += row["handles"]
                items += row["succeeded"]
                count += 1
        if count >= 5:
            break
    return handles / items if items and handles else None


def _remaining_work(status, usable):
    remaining = 0
    covered = set()
    for project in status["health"]["projects"]:
        pending = project["pending_items"] or 0
        ready = project.get("ready_handles")
        ratio = _handles_per_item(status["runs"], project["project"])
        if project["errors"] or ready is None or (pending and ratio is None):
            return None
        batch_ids = project.get("ready_batch_ids", [])
        covered.update(batch_ids)
        for run in usable:
            if run.get("batch_id") in batch_ids:
                ready = max(0, ready - run["publication"]["published_handles"])
        remaining += ready + pending * (ratio or 0)
    for run in usable:
        if run.get("batch_id") in covered:
            continue
        work = run["publication"]["remaining_handles"]
        if work is None:
            return None
        remaining += work
    return remaining


def backlog_estimate(status: dict) -> dict:
    active = [
        run
        for run in status["runs"]
        if run["command"] == "publish" and run["status"] == "running"
    ]
    result = {
        "remaining_handles": None,
        "handles_per_second": None,
        "eta_seconds": None,
        "estimated_end": None,
        "approximate": True,
        "reason": "waiting for active publication",
    }
    if any(
        sum(row["failed"] for row in run["projects"])
        for run in active
        if run["state"] == "healthy"
    ):
        result["reason"] = "publication errors require retry"
        return result
    usable = [
        run
        for run in active
        if run["state"] == "healthy"
        and run.get("publication", {}).get("handles_per_second")
    ]
    if not usable:
        return result
    remaining = _remaining_work(status, usable)
    if remaining is None:
        result["reason"] = "waiting for complete backlog counts and mapping ratio"
        return result
    rate = sum(run["publication"]["handles_per_second"] for run in usable)
    seconds = remaining / rate
    result.update(
        remaining_handles=round(remaining),
        handles_per_second=rate,
        eta_seconds=seconds,
        estimated_end=(
            datetime.fromisoformat(status["generated_at"]) + timedelta(seconds=seconds)
        ).isoformat(),
        reason=None,
    )
    return result
