# Recovery & retry

Piddiplatsch persists problematic records for later inspection or retry.

Failure records are written to:

```
outputs/<project>/failures/r<N>/failed_items_<date>.jsonl
```

Skipped (transient) records are written to:

```
outputs/<project>/skipped/skipped_items_<date>.jsonl
```

Each record stores the canonical project in `__infos__.project`. Events whose
project cannot be resolved remain in the legacy global `failures/` or
`skipped/` directory.

Dumped messages are written to:

```
outputs/dump/dump_messages_<date>.jsonl
```

Retry previously persisted items:

```bash
piddi retry <path...> [--delete-after] [-v]
```

Retry remaps without contacting the Handle Service and writes a distinct batch
to `outputs/<project>/handles/retry_handles_<timestamp>.jsonl`. The command
prints every created path; publish only the recovered batch when it is ready:

```bash
piddi publish --project cmip6 outputs/cmip6/handles/retry_handles_<timestamp>.jsonl
```

Use `retry --publish` only when immediate publication is intentional.

Implementation details:
- Retry logic: [src/piddiplatsch/runners/retry.py](https://github.com/ESGF/piddiplatsch/blob/main/src/piddiplatsch/runners/retry.py)
- Recorders: `src/piddiplatsch/persist/`

