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


def _handles_per_item(runs, project, totals):
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
    if items and handles:
        return handles / items
    for row in totals:
        if (
            row["project"] == project
            and row.get("mapped_items")
            and row.get("prepared")
        ):
            return row["prepared"] / row["mapped_items"]
    return None


def _remaining_work(status, usable):
    remaining = 0
    covered = set()
    estimated = set()
    unestimated = {}
    for project in status["health"]["projects"]:
        name = project["project"]
        pending = project.get("pending_items")
        ready = project.get("ready_handles")
        if project.get("errors") or ready is None:
            unestimated[name] = dict(
                project=name,
                pending_events=pending,
                reason="backlog counts unavailable",
            )
            continue
        batch_ids = project.get("ready_batch_ids", [])
        covered.update(batch_ids)
        for run in usable:
            if run.get("batch_id") in batch_ids:
                ready = max(0, ready - run["publication"]["published_handles"])
        work = ready
        ratio = _handles_per_item(status["runs"], name, status.get("handle_totals", []))
        if pending is None:
            unestimated[name] = dict(
                project=name,
                pending_events=None,
                reason="pending event count unavailable",
            )
        elif pending and ratio is None:
            unestimated[name] = dict(
                project=name, pending_events=pending, reason="no mapping history yet"
            )
        else:
            work += (pending or 0) * (ratio or 0)
        remaining += work
        if work:
            estimated.add(name)
    for run in usable:
        if run.get("batch_id") in covered:
            continue
        work = run["publication"]["remaining_handles"]
        names = [row["project"] for row in run["projects"]]
        if work is None:
            for name in names:
                unestimated[name] = dict(
                    project=name,
                    pending_events=None,
                    reason="active batch size unavailable",
                )
        else:
            remaining += work
            if work:
                estimated.update(names)
    return (
        remaining,
        sorted(estimated),
        [unestimated[name] for name in sorted(unestimated)],
    )


def _recent_batch_rate(runs, projects, now):
    """Use the latest successful batch for the same projects, at most 5m old."""
    candidates = []
    for run in runs:
        if run["command"] != "publish" or run.get("status") != "completed":
            continue
        finished = run.get("finished_at")
        if not finished or {row["project"] for row in run["projects"]} != projects:
            continue
        if any(row.get("failed", 0) for row in run["projects"]):
            continue
        timestamp = datetime.fromisoformat(finished)
        if not 0 <= (now - timestamp).total_seconds() <= 300:
            continue
        rate = sum(
            row["handle_rate"]
            for row in run.get("history", [])
            if row["elapsed_seconds"] >= 5
        )
        if rate > 0:
            candidates.append((timestamp, rate))
    return max(candidates)[1] if candidates else None


def _publisher_rate(run, status):
    if run["state"] != "healthy":
        return None, None
    measured = run.get("publication", {}).get("handles_per_second")
    if measured:
        return measured, "live"
    # Do not extend an old rate over a stalled or stale publisher. A brief
    # warm-up allowance bridges new batches while their first samples arrive.
    started = run.get("started_at")
    now = datetime.fromisoformat(status["generated_at"])
    if (
        not started
        or not 0 <= (now - datetime.fromisoformat(started)).total_seconds() <= 30
    ):
        return None, None
    projects = {row["project"] for row in run["projects"]}
    rate = _recent_batch_rate(status["runs"], projects, now)
    return (rate, "recent_batch") if rate else (None, None)


def _usable_publishers(active, status):
    rated = [(run, _publisher_rate(run, status)) for run in active]
    usable = [run for run, (rate, _) in rated if rate]
    rate = sum(value for _, (value, _) in rated if value)
    sources = {source for _, (value, source) in rated if value}
    source = next(iter(sources)) if len(sources) == 1 else "mixed"
    return usable, rate, source


def backlog_estimate(status: dict) -> dict:
    active = [
        run
        for run in status["runs"]
        if run["command"] == "publish" and run["status"] == "running"
    ]
    result = {
        "remaining_handles": None,
        "handles_per_second": None,
        "rate_source": None,
        "eta_seconds": None,
        "estimated_end": None,
        "approximate": True,
        "partial": False,
        "estimated_projects": [],
        "unestimated_projects": [],
        "reason": "waiting for active publication",
    }
    if any(
        sum(row["failed"] for row in run["projects"])
        for run in active
        if run["state"] == "healthy"
    ):
        result["reason"] = "publication errors require retry"
        return result
    usable, rate, source = _usable_publishers(active, status)
    if not usable:
        if active:
            result["reason"] = "waiting for a healthy publisher with speed samples"
        return result
    remaining, estimated, unestimated = _remaining_work(status, usable)
    result.update(
        partial=bool(unestimated),
        estimated_projects=estimated,
        unestimated_projects=unestimated,
    )
    if not remaining and unestimated:
        result["reason"] = "waiting for estimable backlog counts or mapping history"
        return result
    seconds = remaining / rate
    result.update(
        remaining_handles=round(remaining),
        handles_per_second=rate,
        rate_source=source,
        eta_seconds=seconds,
        estimated_end=(
            datetime.fromisoformat(status["generated_at"]) + timedelta(seconds=seconds)
        ).isoformat(),
        reason=None,
    )
    return result
