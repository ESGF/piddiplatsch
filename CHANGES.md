# Changelog

## Unreleased

### Added

- `piddi insights` shows daily event, Handle, and error counts per project for
  30 days by default, with `--days`, `--project`, `--summary`, and JSON output.
- Optional duplicate Kafka event cleanup across closed plain/gzip dumps with
  `piddi maintain --deduplicate-dumps`, dry-run previews, and the
  `maintenance.deduplicate_dumps` setting for scheduled maintenance.

### Fixed

- Cache fingerprints for unchanged dump archives, skip unnecessary rewrites, and
  report scan progress during deduplication maintenance.
- Show stopped harvest failures and their saved error summaries in `piddi top`,
  and retain harvest errors when displaying mapping/publication totals.
- Stop automatic harvest service restarts on invalid publications (exit status
  65), preventing repeated logs and dump records for a blocked Kafka event while
  preserving its checkpoint and retries for transient failures.

## [2.4.0] - 2026-11-03

### Added

- Shared SQLite catalog with Kafka checkpoints, PATCH recovery, and retained work
  history for mapping and replay.
- Automatic mapping and publication with `piddi sync`, configurable batches, and
  per-project priorities.
- Transactional Handle outbox with resumable publication and ordered updates.
- Recovery commands under `piddi admin` to reset, rebuild, clean, and recover work.
- Disk-space guards, dump rotation, and scheduled output maintenance.
- Dataset alias Handles for all projects when `force_dataset_pid` is enabled.

### Changed

- Version history links use the local catalog and are enabled by default.
  Disable with `catalog.version_history = false` or `map --no-version-history`.
- Improved `piddi top` with batch phases, error totals, processing rates, and
  publication estimates.
- Clearer progress logs, PATCH and version-lookup summaries, and a separate error log.
- Simpler deployment and admin guides, service controls, and updated dependencies.
- Removed legacy configuration, Elasticsearch lookup, pyhandle support, and retry
  commands. Use named Handle profiles, REST publication, and catalog processing.

### Fixed

- Faster batch scheduling and validation for large backlogs.
- Durable handling of rejected items without repeatedly blocking later work.
- More reliable publication progress, error reporting, and recovery after interruption.

## [2.3.0] - 2026-10-12

### Added

- CMIP7, CMIP6Plus, and CORDEX-CMIP6 plugins.
- Separate `harvest`, `map`, and `publish` commands, with date selection.
- Concurrent Handle REST publication, retries, and per-record receipts.
- `piddi top` for live progress and recent history.
- Shared Handle profiles, per-project prefixes, and optional dataset PID generation.
- Configuration inspection and validation, with credentials hidden by default.
- Ansible deployment, systemd services, log rotation, and an Apple Silicon test VM.
- MkDocs guides and overview slides.

### Changed

- Shared processing code across plugins and commands.
- Bounded JSONL streaming with ordered updates to each Handle.
- Manual runs and deployment share `custom.toml`.
- Kafka defaults to `SASL_SSL` with `PLAIN`; local brokers need `PLAINTEXT`.
- Failure files are grouped by project. Publication writes JSONL audit records first.
- Consume and retry defer publication unless `--publish` is set.
- Configuration commands avoid unnecessary client imports and file logging.

### Fixed

- Safer retries, failure handling, output paths, statistics, and Docker builds.
- Tests keep local runtime files untouched.

### Removed

- `retry --dry-run` and legacy list-based JSONL loaders.

## [2.2.1] - 2026-08-25

- Moved the repository to `ESGF/piddiplatsch` and updated links.
- Renamed the notebook environment to `piddiplatsch`.

## [2.2.0] - 2026-03-03

- Moved CMIP6 settings to `[plugins.cmip6]` and added a static plugin registry.
- Improved progress labels and simplified the documentation.
- Updated type annotations, CI checks, and Black configuration.

## [2.1.0] - 2026-01-22

- Added class-based retries with file selection, skipped records, and cleanup options.
- Added `--force` to continue after temporary external failures.
- Shared result models, JSONL helpers, and persistence code.
- Standardized timestamps to UTC.

## [2.0.0] - 2026-01-13

- Initial Kafka consumer, CLI, and project tooling.
- Added version management and release targets.
