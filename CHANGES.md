# Changelog

## [Unreleased]

### Added

- Publication batch ETA and approximate known-backlog completion estimates in
  `piddi top`, based on recent Handle throughput and observed mapping ratios.
- Configurable disk-space warning/stop guards, daily maintenance for closed raw
  dump compression, and storage/backlog/publication health in `piddi top`.
- Shared alias Handle schema and dataset redirects for all projects when
  `force_dataset_pid` is enabled, including CMIP7.
- Service controls: `make start`, `make stop`, and `make restart`.
- Sync options: `--project`, `--workers`, and `--retries`.
- Dependency checks in CI and `make audit`.

### Changed

- Published sync batches use UTC date directories; default sync batches now
  trigger at and contain up to 10,000 catalog items.
- Publication receipts also use UTC date directories under `published/`.
- Simpler INFO progress logs; per-record details moved to DEBUG.
- Statistics summaries every 30 seconds or 10,000 messages by default.
- Four publication workers by default.
- Updated dependencies, security minimums, and development tools.
- Removed obsolete docs, simplified the guides, and added deployment to MkDocs.

### Fixed

- Keep prepared/published Handle totals visible in `piddi top` alongside harvest,
  and persist live publication progress and heartbeats.
- Explain malformed asset checksums, including duplicated SHA-256 multihash
  prefixes; standalone map rejects invalid assets and processing defaults to
  stopping at the first error when no error limit is configured.
- Suppressed the expected Brotli GIL warning in CLI commands.

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
