# Changelog

All notable changes to this project are documented here.

## [Unreleased]

### Added
- Added a small native Handle REST backend for single-record publication and retrieval; the pyhandle backend remains available as a legacy option.
- Added `piddi publish` for idempotent, deferred publication of prepared Handle JSONL files without modifying the source files.
- Added bounded concurrent Handle publication with per-PID update ordering and opt-in contract tests for real Handle services.
- Added configurable Handle PUT latency to the Docker mock service, defaulting to 50 ms.
- Added separate `harvest` and `map` commands for queue-to-raw-JSONL and
  raw-JSONL-to-Handle-JSONL processing.

### Changed
- Handle publication selects `rest` or legacy `pyhandle`; both always write the
  JSONL audit record first.
- `consume` now always dumps raw messages before plugin filtering and defers
  Handle publication by default; `consume --publish` runs all three stages.
- REST publication suppresses repeated urllib3 insecure-request warnings when
  `handle.verify_https = false` was explicitly configured.
- Deferred Handle publication supports bounded batches, offsets, and transient retries with exponential backoff.
- Successful deferred publications now log created/updated Handle REST URLs and
  project, dataset, and asset context.
- Direct REST and pyhandle publication now always writes the project-scoped JSONL audit record before contacting the Handle service.
- Tests now use temporary folders and leave local output files untouched.
- Cleaned up dependencies, development tools, CI, and packaging.

### Fixed
- Made retries safer and fixed retry counts and skipped records.
- Fixed configured output folders, statistics, HTTPS defaults, and Docker builds.
- Fixed a few small code issues.

### Documentation
- Added short configuration and operations guides.

## [2.2.1] - 2026-08-25
### Changed
- Transferred the canonical repository from `cehbrecht/piddiplatsch` to `ESGF/piddiplatsch` and updated package metadata, repository links, and contributor instructions.
- Renamed the notebook environment from `piddiplatsch2` to `piddiplatsch`.

## [2.2.0] - 2026-03-03
### Changed
- Plugins: Namespace CMIP6 configuration under `[plugins.cmip6]` and switch to a static plugin registry (single active plugin selected via `consumer.processor`).
- Progress bar now displays processor name (e.g., `cmip6`) instead of object representation.
### Documentation
- Streamlined README: concise Testing section, compact Recovery/Retry, removed duplicates.
- Added explicit smoke test note: run `make test-smoke` for local end-to-end.
- Added status bar documentation explaining metrics in verbose mode.
### Fixed
- Code style: Modernized type annotations (`X | None` instead of `Optional[X]`).
- CI workflow: Use consistent linting via `make lint` (removed duplicate pre-commit step).
- Black formatter version alignment: Updated to black 26.x across all environments.

## [2.1.0] - 2026-01-22
### Added
- Run retry via `RetryRunner` class with `run_file` and `run_batch`.
- Common result module at `piddiplatsch.result` consolidating dataclasses.
- Common helpers at `piddiplatsch.helpers`: `DailyJsonlWriter`, `read_jsonl`, `find_jsonl`, `utc_now`.
- JSONL handle backend now uses shared helpers (daily rotation, UTC timestamps).
- Retry CLI supports files/dirs/globs, skipped items, `--dry-run`, `--delete-after`, and `-v`.
- New `--force` option to continue despite transient external failures (records skipped items).

### Changed
- Unified persistence API: `RecorderBase.record()` handles infos and writes to JSONL files.
- Retry logic migrated to class-based `persist/retry`.
- Standardized timestamps to UTC via `utc_now()`.
- Simplified logging and persistence flows.


## [2.0.0] - 2026-01-13
- Initial project setup.
- Add bump-my-version configuration and Makefile targets for patch/minor/major bumps.
- Update README with concise tagline and brief versioning usage.
- Establish Kafka consumer, CLI entry point, and basic tooling.
