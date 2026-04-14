# Daily Report Plan

## Goal

Turn `data-base` into a single containerized program that can run inside Docker Compose, wake up at
one UTC time each day, validate only newly arrived day partitions, and write a report for the
partitions it has not reviewed yet.

## Scope Changes

- Keep the existing validator engine and report format as the core.
- Add a lightweight scheduler mode inside the Python program instead of depending on cron.
- Replace separate report/state paths with one program working directory.
- Remove the `required_received_time_slots` CLI parameter and validator setting.
- Remove the `max_examples` CLI parameter and use one fixed internal cap.

## Proposed CLI

The program should expose one explicit daily-run command:

```bash
flash-dataset run-daily \
  --working-directory /data/working \
  --storage-directory /data/storage \
  --run-at 6:00AM \
  --max-days-back 2
```

### Parameters

- `--working-directory PATH`
  - Purpose: single writable program directory for reports and persisted state.
  - Default: `/data/working`
- `--storage-directory PATH`
  - Purpose: input parquet directory to scan.
  - Default: `/data/storage`
- `--run-at TIME`
  - Purpose: daily UTC schedule in 12-hour format such as `6:00AM`. Set `off` to run once and
    exit immediately.
  - Default: `6:00AM`
- `--max-days-back N`
  - Purpose: ignore day folders more than `N` days older than the newest discovered folder.
  - Default: `2`
- `--verify-transaction-hashes`
  - Purpose: keep the current optional expensive hash check.
  - Default: `false`

## `working-directory` Layout

The program owns everything it writes beneath `--working-directory`:

- `latest-reviewed.txt`
  - Stores the latest successful review watermark as one ISO day such as `2026-04-13`.
- `reports/YYYY-MM-DDTHH-MM-SSZ/summary.md`
- `reports/YYYY-MM-DDTHH-MM-SSZ/findings.jsonl`
- `reports/latest/summary.md`
- `reports/latest/findings.jsonl`

`reports/latest/` is only a convenience copy of the most recent run.

## Scheduling Behavior

- If `--run-at` is set to `off`, run one scan immediately and exit.
- Otherwise, keep the process alive and run once per day at that UTC time.
- Startup behavior in scheduled mode:
  - Wait until the next matching UTC wall-clock time.
  - Run exactly once for that day.
  - Sleep until the next day’s run time.
- No timezone parameter is needed because all scheduling is UTC.

## Incremental Filter

Use one persisted day watermark derived from the storage layout.

### Day-Watermark Rules

- Assume the base storage layout is partitioned by `year=YYYY/month=MM/day=DD`.
- On a successful run, compute the maximum partition day reviewed in that run.
- Write that day to `latest-reviewed.txt` in ISO form such as `2026-04-13`.
- On the next run, scan the storage directory, discover all day partitions from the
  `year=YYYY/month=MM/day=DD` folder names, and select only partitions whose parsed day is later
  than the stored watermark.
- Ignore partitions more than `max-days-back` days older than the newest discovered day folder.
- Validate the unique set of selected day partitions.

### Notes

- This intentionally prefers partition-date ordering over filesystem timestamp tracking.
- The approach is simple and stable as long as new data arrives in new day partitions.
- If an already reviewed day receives late additional files later, this watermark will not recheck
  that day automatically. That tradeoff is accepted for this version.

## Validation Rule Change

The current code treats configured slots such as `a` as fully required. That no longer matches the
desired rule.

### New Rule

For each `flashblocks` row:

- Valid if at least one of `received_time_a`, `received_time_b`, or `received_time_c` is present.
- Warn if all three are null.

### Planned Code Changes

- Remove `required_received_time_slots` from:
  - `flash_dataset.cli`
  - `ValidatorConfig`
  - discovery config validation
  - pipeline wiring
  - tests and helpers that pass the setting
- Keep the existing per-slot coverage metric if it is still useful operationally.
- Replace `required_received_time_missing` with one rule such as
  `all_received_time_slots_missing`.

## Fixed Internal Defaults

- Example cap in `ValidationResult`: keep a fixed internal default of `25`.
- Report retention: do not delete old run directories automatically in the first version.
- Exit code:
  - `0` when the run completes with no fail findings.
  - `1` when the run completes with fail findings or encounters a runtime/configuration error.

## Docker Compose Shape

One long-running service is enough:

```yaml
services:
  data-base:
    build: .
    volumes:
      - ./feed-base:/data/storage:ro
      - ./data-base-runtime:/data/working
```

This keeps Compose responsible only for process lifecycle, not for time-based scheduling.

## Implementation Plan

1. Simplify CLI shape.
   Remove `required_received_time_slots` and `max_examples`, add `--working-directory` and
   `--max-days-back`, default to scheduled mode, and allow `--run-at=off` for one-shot runs.

2. Add day-watermark state handling.
   Read and write `latest-reviewed.txt`, parse one ISO day, and compute candidate partitions from
   `year=YYYY/month=MM/day=DD` directory names.

3. Add partition-targeted execution.
   Extend the discovery or pipeline layer so one run can validate only day partitions later than
   the stored watermark.

4. Update semantic received-time validation.
   Replace the required-slot logic with an “at least one slot present” rule.

5. Add scheduled runner.
   Parse `2:30AM` style input, compute the next UTC run time, sleep, and invoke the existing
   validation path.

6. Write reports under the managed working directory.
   Create timestamped run directories and refresh `reports/latest/`.

7. Add tests.
   Cover one-shot mode, scheduled time parsing, first run with no watermark, rerun with no newer
   day folders, rerun with newer day folders, and the new received-time rule.

8. Update top-level docs after code lands.
   Refresh `README.md`, `docs/architecture.md`, `docs/performance.md`, and
   `docs/lessons-learned.md` to match the shipped behavior.

## Risks

- Late data written into an already reviewed `day=DD` folder will be skipped by the day watermark.
- The plan assumes the production storage layout consistently exposes partition dates through
  `year=YYYY/month=MM/day=DD` path segments.
- The current validator is partition-batch oriented, so the cleanest change is to add partition
  targeting instead of trying to skip checks deep inside rule execution.
