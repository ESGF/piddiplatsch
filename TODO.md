# TODO

## Architecture cleanup and multi-project routing

The ESGF publication workflow now has one Kafka topic carrying STAC publication
events for several projects (currently observed: CMIP6, CORDEX-CMIP6, CMIP7,
and CMIP6Plus; expected later: CORDEX-CMIP7 and others). Piddi must consume that
shared stream and run only the project plugins selected for a particular
deployment.

### Observed real-queue snapshot (2026-08-25)

The local production-like dump can be inspected safely as streaming JSONL with
`rg`, `head`/`sed`, and per-line `jq`; it does not need to be loaded into memory
or copied in full. At the time of inspection:

- `outputs/dump/dump_messages_2026-08-25.jsonl`: 4.6 GB, 436,654 events.
- `outputs/handles/handles_2026-08-25.jsonl`: 2.8 GB, 3,983,218 generated
  Handle records.
- Event methods: 436,650 POST and 4 PATCH.
- Collections/projects on the shared topic:
  - 423,584 CMIP6;
  - 13,045 CORDEX-CMIP6;
  - 23 CMIP7;
  - 2 CMIP6Plus.

This means the immediate plugin inventory from the captured queue is CMIP6,
CORDEX-CMIP6, CMIP7, and CMIP6Plus. CORDEX-CMIP7 remains an expected future
project and should not be confused with the observed CORDEX-CMIP6 records.

Observed field shapes:

| Collection | Dataset PID | File PID |
| --- | --- | --- |
| CMIP6 | `cmip6:pid` | `cmip6:tracking_id` |
| CORDEX-CMIP6 | `cordex-cmip6:pid` | `cordex-cmip6:tracking_id` |
| CMIP7 | `cmip7:pid` | `cmip7:tracking_id` |
| CMIP6Plus | `cmip6plus:pid` | `cmip6plus:tracking_id` |

- `data.payload.collection_id` and STAC `item.collection` carry the routing
  identity. At least some CMIP6 items omit `item.properties.project`, so that
  property cannot be the sole routing key.
- PATCH events in this capture are CMIP7 events with `collection_id` and
  `item_id`, confirming they can be routed before retrieving the full STAC item.
- Every observed POST has a namespaced dataset PID. Asset PID counts also use
  the corresponding project namespace; fixtures should retain representative
  asset counts rather than assuming one file per dataset.
- A sampled CMIP6 asset had source PID
  `hdl:21.14100/1b37978f-caf6-4e4a-9893-3266a93077a2`, while its generated JSONL
  Handle used deterministic suffix `bedf32ab-ef69-3184-b02a-c1dc6cbe3cf5`.
  This confirmed the old mapper ignored the namespaced file PID; the shared PID
  resolver on `dev-plugins` now preserves it.
- The generated Handle JSONL contains `handle`, `URL`, `data`, and `timestamp`,
  but no project/plugin or source-event identity. The production outbox schema
  must add that provenance before multi-project publishing and retries.
- Extract small, sanitized fixtures for each observed collection, all four PATCH
  events, representative multi-asset datasets, and their generated Handle
  records. Record the extraction command and source-file checksum so samples can
  be refreshed without committing the multi-gigabyte dumps.

Do the architecture cleanup before adding further project plugins. Keep it
incremental and preserve the current CMIP6 behaviour with tests.

### Implemented on `dev-plugins` (2026-08-25)

- One consumer now routes to selected built-in plugins; config and CLI support
  one, several, or all registered projects. Legacy `consumer.processor` was
  removed.
- CMIP6 and CMIP7 plugins are registered. Their common publication-envelope,
  PATCH, STAC-to-Handle mapping, and Pydantic Handle output schemas live in
  shared core code; thin plugin classes declare PID fields and differences.
- Input does not require full STAC Pydantic validation. Shared adapters validate
  only the envelope and consumed object/field shapes, while Pydantic validates
  generated dataset/file Handle records.
- CMIP7 POST mapping is covered by a compact fixture derived from the real
  queue. The real direct-list RFC 6902 PATCH shape is supported; the older
  wrapped `patch.operations` form remains readable.
- CMIP6 resolves namespaced PID fields with explicit legacy fallbacks; CMIP7
  resolves `cmip7:pid` and `cmip7:tracking_id`. Dataset/file relationships use
  those resolved PIDs and conflicting populated aliases fail visibly.
- CORDEX-CMIP6 is registered as `cordex-cmip6`, resolves
  `cordex-cmip6:pid` / `cordex-cmip6:tracking_id`, and uses the shared mapper.
  Its real queue fixture covers filename-keyed assets and exclusion of the
  non-file `globus` asset.
- CMIP6Plus is registered as `cmip6plus`, resolves `cmip6plus:pid` /
  `cmip6plus:tracking_id`, and uses the shared mapper. Its fixture represents
  the duplicated 20-file dataset observed in the real queue.
- A no-service integration test feeds compact real-queue-derived POST records
  for all four projects through the direct/recovery-style pipeline and JSONL
  backend. It verifies exact source PID preservation, output validation, and
  dataset/file Handle relationships.

Remaining items below include project-scoped output/provenance, stronger PID
edge-case coverage and operational rollout checks. All projects observed in the
captured queue now have plugins; CORDEX-CMIP7 remains a future project pending a
representative publication record.

### Target design

- Keep one Kafka consumer and introduce a project router between message
  decoding and project processing. Do not let each plugin consume the topic
  independently.
- Replace the singular `consumer.processor` setting with a project selection:
  one project, several projects, or all registered projects.
- Add repeatable CLI selection (for example `--project cmip6 --project cmip7`)
  plus `--all-projects`; CLI selection overrides configuration.
- Use stable, normalized plugin names (`cmip6`, `cordex-cmip6`, `cmip7`,
  `cmip6plus`, and later `cordex-cmip7`) while accepting the exact project
  identifiers used by publication records.
- Route a record to exactly zero or one selected plugin:
  - an unmatched or disabled project is intentionally ignored and counted, not
    treated as a processing error or written to the retry queue;
  - more than one matching plugin is a configuration/programming error and must
    fail loudly rather than publishing duplicate Handles.
- Determine the project from the publication envelope without requiring a full
  project-specific model. Prefer `data.payload.collection_id`, and define and
  test fallbacks for full STAC items (for example `item.collection` and
  `item.properties.project`). PATCH events must be routable before fetching the
  referenced STAC item.
- Define one clear plugin interface covering identity/matching, construction,
  preflight, and processing. Run preflight only for selected plugins.
- Keep the common publication-envelope, PATCH retrieval, Handle writing, and
  result/error machinery in core/shared code. Keep project schema, PID record
  mapping, and genuine behavioural differences inside each project plugin.
  Extract shared plugin helpers when the second implementation demonstrates the
  common shape instead of coupling new plugins directly to CMIP6 classes.

### Configuration and deployment semantics

- Settle and document configuration syntax. Proposed shape:

  ```toml
  [consumer]
  projects = ["cmip6"] # or multiple names; use an explicit `all` mode

  [plugins.cmip6]
  enabled = true
  ```

- Decide whether `all` is represented only by `--all-projects`, by a dedicated
  config value, or both. It must mean all registered built-in project plugins
  and must reject an empty registry.
- Validate unknown, duplicate, disabled, and conflicting project selections at
  startup. List the effective selection in logs and in a CLI inspection command.
- Remove `consumer.processor`; require `consumer.projects` and reject the old
  setting during configuration validation so it cannot be silently ignored.
- Document Kafka consumer-group safety prominently:
  - one process may route several selected projects from one shared group;
  - separate project-specific processes must use distinct `group.id` values;
  - changing the project set associated with an existing group can skip or
    replay work and therefore requires an explicit deployment decision.
- Consider validating or deriving a project-aware group-id suffix, but do not
  silently change an explicitly configured production group id.

### Outputs, recovery, and observability

- Make processed outputs project-scoped. Preferred layout:

  ```text
  outputs/
    cmip6/{handles,failures,skipped}/
    cordex-cmip6/{handles,failures,skipped}/
    cmip7/{handles,failures,skipped}/
    cmip6plus/{handles,failures,skipped}/
    cordex-cmip7/{handles,failures,skipped}/
    _stream/{dump,unrouted}/
  ```

- Decide whether `_stream/unrouted` is persisted only in diagnostic/observe
  mode; normal filtering should not create an unbounded copy of unrelated
  traffic.
- Store the resolved project/plugin identity in failure and skipped records so
  `piddi retry` selects the original plugin deterministically. Make retry output
  follow the same project layout.
- Report totals for consumed, routed, filtered/unmatched, succeeded, skipped,
  and failed records, with per-project breakdowns for routed work.
- Review Kafka acknowledgement semantics as part of the routing change. A
  filtered record is complete for this consumer group; a routed record must not
  be acknowledged before its success/failure persistence policy is satisfied.
  Document how this interacts with `enable.auto.commit`, fail-fast transient
  errors, restarts, and at-least-once Handle updates.

### Preserve PIDs from namespaced STAC fields

The current CMIP6 mapper looks for the legacy fields `properties.pid` (dataset)
and `asset.tracking_id` (file). Current CMIP6 STAC examples instead use
`properties["cmip6:pid"]` for the dataset PID and
`asset["cmip6:tracking_id"]` for each file PID. These are STAC namespaced keys
with a colon, not `cmip6_pid` or `cmip6_tracking_id`.

- Treat PID field names as project-plugin metadata because the namespace and
  possibly the PID semantics differ between CMIP6, CORDEX-CMIP6, CMIP7,
  CMIP6Plus, and future projects such as CORDEX-CMIP7.
- Define canonical fields and an explicit, ordered compatibility list per
  project. For CMIP6, support the current namespaced fields and the known legacy
  `pid`/`tracking_id` fields during migration.
- Check captured publication messages before deciding whether the older
  dataset-level `cmip6:tracking_id` seen in existing test data is a valid alias
  or an obsolete/incorrect fixture.
- Normalize equivalent Handle representations (for example
  `hdl:21.14100/<uuid>`, `21.14100/<uuid>`, and the UUID portion expected by the
  backend) in one well-tested resolver.
- If canonical and legacy fields are both present:
  - accept them when they resolve to the same PID;
  - reject the record when they disagree, rather than choosing silently.
- Distinguish a missing PID from an invalid PID. Never generate a replacement
  merely because a present source PID is malformed or stored under an
  unrecognized field; fail visibly so the publication data can be corrected.
- Generate a deterministic fallback PID only when the project's contract
  explicitly permits a missing PID. Log and count fallback generation so it is
  operationally visible.
- Resolve dataset and file PIDs before building Handle relationships. Ensure
  dataset `HAS_PARTS` uses each asset's resolved source PID and file
  `IS_PART_OF` uses the dataset's resolved source PID; do not mix authoritative
  PIDs with independently generated relationship targets.
- Keep PID extraction separate from Handle record construction so every plugin
  can reuse validation and conflict handling without inheriting CMIP6 schema
  logic.
- Add regression tests for:
  - canonical namespaced dataset and asset PID fields;
  - legacy unnamespaced fields;
  - canonical and legacy fields with equal and conflicting values;
  - missing versus malformed values;
  - `HAS_PARTS`/`IS_PART_OF` consistency;
  - values with and without the `hdl:` scheme and Handle prefix;
  - full POST records and PATCH records after the STAC item is retrieved.
- Replace or supplement the old fixtures with sanitized records captured from
  the real publication stream, retaining fixtures for every supported legacy
  shape.

### Decouple queue processing from Handle publication

Reading, mapping, and writing JSONL is fast enough to process roughly one
million publication records in under an hour. Direct Handle publication is the
bottleneck at about 20 Handles/second, which reduces CMIP6 throughput to roughly
one or two datasets/second once file Handles are included.

Make durable JSONL Handle output the normal ingestion path. Keep direct Handle
publication available as an explicit deployment mode, but do not make Kafka
consumption wait on Handle-service throughput by default.

#### Ingestion/outbox responsibilities

- Treat generated Handle JSONL as a production outbox, not as `--dry-run`
  output. Rename configuration, CLI help, classes, logs, and documentation so
  production spooling and diagnostic dry-run behaviour are not conflated.
- Define explicit ingestion modes, for example `spool` (default) and `direct`.
  Direct mode should use the same resolved Handle records as spool mode.
- Acknowledge a routed Kafka record only after all of its Handle operations are
  durably written to the outbox (or successfully completed in direct mode).
- Define a versioned, target-neutral JSONL envelope containing at least:
  operation, full Handle/PID, Handle values, project/plugin, source event
  identity, ordering metadata, creation time, and schema version. Include enough
  provenance to diagnose and safely replay a line without the original STAC
  message.
- Decide the durability boundary explicitly: flush/fsync policy, behaviour on a
  partial final line, recovery after a crash, and minimum free-disk/backpressure
  policy. Fast generation must not turn broker retention risk into silent local
  disk exhaustion.
- Keep raw Kafka dumps, generated Handle outbox records, mapping failures, and
  Handle-publication failures as separate artefact types with separate
  retention rules.

#### Safe file handoff

- Do not let a publisher tail the same daily JSONL file that ingestion is still
  appending to. Introduce a sealing/rotation protocol and atomically move closed
  files from a writing area to a ready area.
- Prefer a recoverable spool state model such as:

  ```text
  outputs/<project>/handles/
    writing/
    ready/
    processing/
    published/<target>/
    failed/<target>/
  ```

- Settle rotation triggers (size, record count, and/or time) so publication can
  start promptly without producing excessively small files.
- Support atomic file claiming so several publisher workers cannot process the
  same file concurrently. Recover claims left behind by crashed workers without
  requiring manual file edits.
- Add a file manifest or equivalent integrity metadata (record count and
  checksum) when sealing a file, and verify it before publication.

#### Publisher process

- Add a separate long-running publisher command/process that reads sealed
  outbox files and publishes their Handle operations, for example
  `piddi publish [PATH ...]` plus a watch/spool mode.
- Make publisher concurrency, batch size, rate limit, request timeout, retry
  policy, exponential backoff, and shutdown draining configurable per Handle
  service. Measure real throughput before choosing defaults.
- Persist line-level delivery progress so one bad Handle does not require
  replaying a large file from the beginning. Keep the original immutable outbox
  data and record failure details separately.
- Define success as an idempotent upsert/overwrite. Expect at-least-once delivery
  across crashes and retries; never rely on exactly-once network publication.
- Preserve the source order of updates for the same PID. Parallel workers must
  not allow an older queued record to overwrite a newer one. Decide whether to
  partition/serialize by PID, attach and compare source offsets/versions, or
  safely compact superseded full-state updates.
- Decide whether dataset Handles must be visible before their file Handles, and
  preserve that ordering if any consumer or validation process requires it.
- Expose backlog age/size, ready and in-flight files, Handles/second, successes,
  retries, permanent failures, and per-target lag.
- Support graceful stop/restart without losing the current checkpoint or
  leaving an unrecoverable file claim.

#### Minimal Handle REST client

Keep the existing `pyhandle` backend as a compatibility option, but add a small
first-party client for the Handle HTTP JSON REST API. Implement only the
operations piddi needs for publication and verification; do not attempt to
replace the full Handle client library.

The standard Handle v9 REST API documents one Handle per
`PUT /api/handles/{handle}`. The request body's array contains multiple values
for that one Handle; it is not a batch of multiple Handles. Handle also has
separate batch command-line tooling, but the standard REST documentation does
not currently define a multi-Handle JSON batch endpoint. Verify whether the
deployed ESGF Handle service provides a version-specific or local batch
extension before designing around one.

- Define a narrow backend protocol used by the publisher, for example:
  - upsert one complete Handle record;
  - upsert several Handle records, returning an outcome for every input;
  - optionally resolve one Handle for diagnostics/verification;
  - health/capability discovery where the service supports it.
- Implement that protocol for both `pyhandle` and the new REST client so backend
  selection does not leak into queue mapping, outbox files, or publisher state.
- Convert piddi's named Handle fields into the REST API's Handle-value JSON
  representation in one place. Define stable indices, types, TTLs, permissions,
  encoding, and administrator values, and verify that the resulting records are
  compatible with records produced by `pyhandle`.
- Use idempotent full-record upserts with explicit `overwrite=true`. Correctly
  URL-encode Handle names while preserving their prefix/suffix semantics.
- Support only the authentication modes required by the ESGF services. Start by
  validating whether the current Handle username/password can use HTTP Basic or
  Handle session authentication; add certificate authentication only if a real
  target requires it.
- Keep authentication pluggable and deliberately small:
  - implement simple, standard authentication directly in the REST client when
    it is sufficient (for example HTTP Basic, a static bearer/header token, or
    client certificates required by an actual target);
  - if a service requires complicated Handle challenge-response/session
    authentication, prefer the proven `pyhandle` backend for complete requests
    instead of reimplementing the protocol immediately;
  - only split authentication into a `pyhandle`-provided adapter plus the new
    REST transport if `pyhandle` exposes a stable, reusable authenticated
    session or request-signing interface and an integration test proves that it
    works with arbitrary REST requests.
- Do not reach into `pyhandle` private attributes to copy cookies, nonces,
  session keys, or generated authorization headers. If no supported adapter is
  available, select `pyhandle` end-to-end for that publication target while
  other targets may still use the REST backend.
- Allow backend and authentication strategy to be selected per named Handle
  service. This permits a mixed deployment, for example a REST/batch-capable
  target with simple authentication alongside a legacy target published through
  `pyhandle`.
- Require HTTPS for authenticated writes. Preserve configurable certificate
  verification/custom CA support, and never log credentials, authorization
  headers, session secrets, or full sensitive responses.
- Interpret both HTTP status and the JSON `responseCode`. Classify
  authentication, authorization, malformed-record, conflict, rate-limit,
  transient server, timeout, and connection failures into retryable and
  permanent outcomes.
- Reuse pooled persistent connections and configure connect/read timeouts.
  When multi-Handle batching is unavailable, use bounded concurrent single
  `PUT` requests plus rate limiting rather than opening one connection per
  Handle.

##### Optional multi-Handle batching

- Test the actual deployed Handle service and its version for a supported
  multi-Handle REST endpoint or documented extension. Capture its request,
  response, authentication, size limits, ordering, and partial-failure contract
  in an integration fixture/spec before implementation.
- Expose an explicit capability mode such as `batch = "auto" | "required" |
  "disabled"`:
  - `auto`: use a verified batch capability, otherwise use single-record PUTs;
  - `required`: fail preflight when batching is unavailable;
  - `disabled`: always use single-record PUTs.
- Do not infer lack of batch support from an arbitrary runtime failure. Fall
  back only after an unsupported-endpoint/capability result; authentication,
  validation, and transient server failures must remain visible.
- Make batch size and maximum request bytes configurable within server-advertised
  limits. Benchmark several sizes rather than assuming larger batches are
  faster.
- Require an item-level result for every Handle in a batch. Checkpoint only
  confirmed successes and retry only failed/unknown items; never replay the
  entire batch blindly after a partial response.
- Preserve per-PID ordering across batches and the publisher's at-least-once,
  idempotent delivery guarantees.
- If the deployed service has no safe multi-Handle REST operation, keep
  `publish_many()` as client-side orchestration over pooled concurrent single
  PUTs. This gives the publisher one interface without pretending the server
  supports atomic batches.

##### REST client verification

- Add contract tests for exact REST URLs, JSON Handle values, authentication,
  TLS configuration, overwrite semantics, response-code parsing, and secret
  redaction.
- Test single success, existing-Handle update, malformed values, unauthorized
  access, timeout, rate limiting, 5xx responses, and connection loss after the
  server may already have committed the upsert.
- Test batch capability detection, supported batching, unsupported fallback,
  partial batch success, oversized batches, and `required` mode failure.
- Run parity tests that publish equivalent records through `pyhandle` and the
  REST backend and compare resolved Handle values.
- If a supported `pyhandle` authentication adapter exists, add contract tests
  for session establishment, reuse, expiry/refresh, concurrent requests, and
  fallback. Otherwise document that authentication determines the complete
  backend for that target.
- Benchmark sequential REST PUTs, pooled concurrent PUTs, `pyhandle`, and any
  verified server-side batch endpoint against a non-production service before
  selecting production defaults.

#### One or several Handle services

- Model Handle services as named publication targets with separate endpoints,
  credentials, rate limits, and delivery state. Kafka ingestion should not need
  Handle credentials in spool mode.
- Make routing semantics explicit per project: publish to one selected target,
  fan out to every configured target, or route different projects/prefixes to
  different targets. Do not infer fan-out from the presence of several service
  configurations.
- Track completion independently for each target. A file delivered to one
  service but pending or failed for another must not be globally archived as
  complete.
- Decide whether each target gets its own ready queue or whether one immutable
  outbox plus a per-target delivery ledger is simpler and safer. Avoid copying
  large JSONL files merely to represent state unless operational isolation
  requires it.
- Keep credentials and target selection in publisher configuration; ensure
  diagnostic commands redact secrets.

#### Compatibility and rollout

- Preserve opt-in direct publication for small runs, debugging, and sites that
  do not want a separate publisher deployment.
- Define how the current `handle.backend = "jsonl"`, `--dry-run`, and JSONL file
  format migrate to the production spool without accidentally publishing old
  diagnostic files.
- Provide an inspection/validation command that checks outbox schema and
  integrity without publishing.
- Test crash points around append, seal, claim, checkpoint, successful request,
  failed request, archive, and restart.
- Test duplicate delivery, conflicting updates for one PID, partial/corrupt
  files, poison records, disk-full behaviour, and concurrent workers.
- Test independent delivery and recovery with two Handle targets where one is
  slow or unavailable.
- Benchmark ingestion and publication separately with representative CMIP6
  asset counts, then document sizing guidance for disk capacity, rotation, and
  publisher worker count.

### Plugin/registry cleanup

- Keep registration simple and static while plugins are used to organize code
  shipped inside the piddiplatsch repository. `PluginSpec` is the single source
  of plugin identity, accepted project identifiers, and processor construction;
  do not keep a parallel processor-class registry.
- Preserve a clean future discovery boundary without implementing it yet. If
  plugins later live in independent Python distributions, prefer standard
  package entry points discovered with `importlib.metadata` (for example a
  dedicated `piddiplatsch.plugins` entry-point group) that return the same
  `PluginSpec` objects used by built-ins.
- Consider `pluggy`, the hook mechanism used by pytest, only if external plugins
  eventually require a richer versioned hook/lifecycle API. Do not add it merely
  for package discovery or the current factory/process interface.
- When external discovery becomes a real requirement, define API compatibility,
  plugin version metadata, deterministic load order, name/project-id collision
  handling, isolation of import failures, and an explicit way to disable
  third-party plugins before enabling it by default.
- Do not hide plugin import errors in the registry. Detect duplicate plugin
  names and project identifiers during startup.
- Reduce global state for configuration and statistics. Pass configuration,
  statistics, lookups, Handle backends, and project output paths explicitly
  where useful.
- Add type checking after the interfaces are settled.
- Add an architecture document showing decode -> route -> process -> persist ->
  acknowledge, including filtered and failure paths.

### Test and rollout checklist

- Add representative POST and PATCH fixtures from each supported project.
- Test config-only and CLI-override selection for one, several, and all projects.
- Test normalization of identifiers (case and punctuation), unmatched records,
  ambiguous matches, and malformed envelopes.
- Test that only selected plugins are constructed and preflighted.
- Test project-scoped handles, failures, skipped records, retry selection, logs,
  and statistics.
- Test restart/offset behaviour with one multi-project process and with separate
  project-specific consumer groups.
- Migrate CMIP6 onto the router first and run it against a captured sample of the
  real shared publication stream before implementing the other observed
  projects.
- Use the second project implementation to refine shared abstractions, then add
  the remaining observed projects according to operational priority. Add
  CORDEX-CMIP7 when representative records and its schema are available.

### Open questions to resolve from real queue samples

- What are the authoritative project identifiers and aliases in
  `collection_id`, STAC `collection`, and `properties.project`?
- Can a publication event legitimately belong to more than one project, or is
  exactly one project an invariant?
- Which non-STAC/control records occur on the topic, and should any of them be
  persisted for diagnostics?
- Is the raw stream dump global, per selected project after routing, or both?
- Which plugin settings and Handle schema fields actually differ between CMIP6,
  CORDEX-CMIP6, CMIP7, CMIP6Plus, and CORDEX-CMIP7?
- What are the canonical dataset-PID and file-PID fields for CMIP7 and
  CORDEX-CMIP7, and are Handle prefixes fixed per project? Confirm that the
  observed CORDEX-CMIP6 and CMIP6Plus field names and prefixes are authoritative.
- Are source PIDs mandatory for datasets and files, or is fallback generation
  valid for either aggregation level?
- Are publication events full current-state upserts, or can applying a newer
  Handle record before an older one lose information?
- Does the Handle service support useful bulk or concurrent request patterns,
  and are its observed 20 Handles/second limits global, per credential, or per
  connection?
- For multiple Handle services, is the expected deployment fan-out of the same
  Handles, project/prefix-based routing, failover, or a combination of these?
- Does the deployed ESGF Handle service expose a non-standard multi-Handle REST
  endpoint, and if so, which server versions and targets support it?
- Which authentication method and Handle-value index/permission conventions
  must the new REST client reproduce from the current `pyhandle` deployment?
