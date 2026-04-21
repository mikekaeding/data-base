# Architecture

## Why This Structure

`data-base` now has two layers:

- an application runtime that owns daily scheduling, day-folder watermark state, and report
  directory management
- the existing validator engine that owns parquet discovery, partition grouping, integrity checks,
  semantic checks, baselines, and report generation for one bounded validation run

This split keeps the long-lived Docker Compose service simple. The runtime layer decides what
needs to be reviewed next, while the validator continues to reason about correctness one partition
at a time.

## Module Boundaries

- `flash_dataset/cli.py`
  Public CLI surface for `validate-parquet` and `run-daily` commands.
- `flash_dataset/runtime.py`
  Day-folder discovery, committed watermark and baseline-snapshot state management, no-op report
  generation, and the daily UTC scheduler.
- `validator/common.py`
  Stable runtime types: config, findings, schema profiles, file index entries, and run result.
- `validator/rules.py`
  Stable rule catalog with severity and status defaults.
- `validator/profiles.py`
  Exact physical parquet schema profiles for each `feed-base` dataset.
- `validator/discovery.py`
  Storage-root validation, date-window-aware partition-root discovery, supported path parsing, and
  parquet footer loading.
- `validator/arrow_compute.py`
  Small wrappers that isolate the dynamic `pyarrow.compute` surface so strict type-checking stays
  scoped to the Arrow boundary instead of leaking through the whole project.
- `typings/pyarrow/*.pyi`
  Local stubs for the exercised Arrow surface so strict pyright checks remain meaningful in the
  validator modules even though upstream `pyarrow` typing is incomplete.
- `validator/integrity.py`
  Cross-table parent/child, uniqueness, set-equality, and ordinal checks.
- `validator/semantics.py`
  Field-level writer-contract checks for base rows, transaction kinds, receipt shapes, topics,
  access lists, authorizations, receive-time coverage, and optional transaction RLP self-hashes.
- `validator/baselines.py`
  Per-partition physical and logical summaries plus drift checks against healthy same-hour
  partitions from the current run and committed runtime snapshot history.
- `validator/pipeline.py`
  Partition grouping, stage orchestration, per-partition Arrow table caching, and baseline-pass
  coordination after partition-local validation.
- `validator/reporting.py`
  Validator-owned `findings.jsonl` emission plus compact summary generation for one validator run.

## Core Invariants

- The runtime watermark is one ISO day stored in `latest-reviewed.txt`.
- The runtime scheduler derives candidate work from `year=YYYY/month=MM/day=DD` folders.
- The runtime rejects any storage tree that is not strict top-level `year/month/day`, including
  validator-only `date=...`, dataset-first, unknown top-level roots, unsupported top-level files,
  and malformed runtime partition directories, instead of treating them as idle/no-work states.
- The runtime only considers day folders no more than `max_days_back` days older than the newest
  discovered day folder.
- The runtime validates only the next expected day inside the active review window.
- Scheduled mode drains later eligible days by repeating that single-day pass until it reaches
  `idle` or `blocked`.
- The runtime emits `blocked` whenever later day folders are still blocked by the next expected day
  being missing or empty, or when the selected day has any non-metric findings, even if another
  partition in that same day reviewed successfully.
- On a fresh working directory, the runtime only establishes the first watermark after the
  selected day reaches the oldest day inside the current review window.
- The runtime fails closed on unreadable storage directories, unreadable state files, or a stored
  watermark newer than the newest discovered day folder.
- The runtime advances the committed report and committed watermark through one atomic pointer
  change. Each run directory stores its own `summary.md`, `findings.jsonl`,
  `latest-reviewed.txt`, and `baseline-snapshots.json`, and `reports/latest/current` atomically
  switches to the newest committed run. The top-level `latest-reviewed.txt` and
  `baseline-snapshots.json` resolve through `reports/latest/*`, so a hard kill cannot leave the
  committed report ahead of the committed runtime state or vice versa.
  When no watermark has been persisted yet, the runtime leaves both state links absent instead of
  publishing dangling symlinks, and any later dangling state link is treated as corruption.
- Scheduled mode validates the same persisted watermark state before its first sleep, so corrupted
  runtime state fails fast at startup rather than waiting until the next run slot.
- After startup validation, the first scheduled pass anchors to today's configured UTC slot. If the
  process starts after that slot, it runs immediately once and then resumes normal daily sleeps.
- Physical parquet footer schema must match the `feed-base` writer exactly.
- `flashblocks` is the parent table keyed by `(payload_id, index)`.
- Every child dataset row must map to an existing parent flashblock.
- Parent count columns must equal actual child row counts exactly.
- Ordinal fields must be contiguous from zero within their logical scope.
- Base-only fields must appear only on `index == 0`.
- A `flashblocks` row is valid if any one of `received_time_a`, `received_time_b`, or
  `received_time_c` is present.
- Operational baselines remain runtime-local: the validator compares the current partition set
  against compact committed snapshots from prior reviewed days instead of depending on an external
  database.

## Data Flow

1. The runtime scans top-level `year/month/day` folders under the storage directory.
2. The runtime fails fast if the storage tree is unreadable or does not expose the required
   top-level `year/month/day` layout for scheduled incremental review.
3. The runtime loads `latest-reviewed.txt` and selects only later day folders that fall within
   `max_days_back` of the newest discovered day.
4. The runtime selects only the next expected day inside that review window, and it stops at the
   first missing or empty pending day.
5. The runtime loads committed baseline snapshots for earlier reviewed days that still fall inside
   the same window.
6. The runtime launches one validator run bounded to that single target day.
7. The validator narrows discovery to matching partition roots when the run is date-bounded, then
   collects parquet files under those roots.
8. The validator parses supported storage layouts and groups files by storage partition.
9. The validator checks partition completeness, single-file cardinality, and per-file schema.
10. The validator loads column-pruned Arrow tables for each partition.
11. The validator runs integrity rules, semantic rules, and baseline checks using the current day
    plus committed snapshot history for comparison peers.
12. The validator writes `findings.jsonl` and one validator-local summary for the run.
13. The runtime writes the final run `summary.md`, writes that run directory's committed runtime
    state files, and atomically flips `reports/latest/current` so `reports/latest/*`,
    `latest-reviewed.txt`, and `baseline-snapshots.json` resolve to the same committed run.
