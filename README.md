# data-base

`data-base` is a container-friendly validator for `feed-base` parquet output. It scans one
`year=YYYY/month=MM/day=DD` storage directory, validates recent unseen day partitions one day at a
time, and writes one report per run.

The validator checks:
- exact physical parquet schema per dataset
- path and partition consistency for supported storage layouts
- unexpected parquet datasets that fall outside the `feed-base` storage contract
- parent/child count agreement across `flashblocks`, transactions, receipts, logs, withdrawals,
  authorizations, access lists, and balance changes
- logical-key uniqueness and ordinal contiguity
- transaction-type, receipt-status, topic-count, base-field, and authorization field contracts
- per-partition physical parquet stats and logical-distribution baselines
- drift checks for payload lengths, tail-only payload volume, dataset physical stats, and
  transaction-type mix when enough same-hour comparison partitions are available inside the active
  review window
- optional `keccak(transaction_rlp) == transaction_hash` verification for every stored transaction row

## Commands

The CLI exposes two commands:

- `flash-dataset validate-parquet`
  One direct validator run over one optional date range.
- `flash-dataset run-daily`
  The long-lived UTC scheduler for unseen day folders. Set `--run-at off` to execute one
  single-day incremental pass and exit.

## `run-daily` Parameters

The `run-daily` command accepts five parameters:

- `--working-directory PATH`
  Writable directory for reports plus runtime review state such as `latest-reviewed.txt` and
  `reviewed-days.json`.
  Default: `/data/working`
- `--storage-directory PATH`
  The parquet storage directory to scan. A top-level `tmp/` subtree is ignored and reserved for
  staged writer output.
  Default: `/data/storage`
- `--run-at TIME`
  Daily UTC schedule in 12-hour format such as `6:00AM`. Set `off` to run once and exit.
  Default: `6:00AM`
- `--max-days-back N`
  Only inspect day folders no more than `N` days older than the newest discovered folder.
  Default: `2`
- `--verify-transaction-hashes`
  Enables offline `keccak(transaction_rlp) == transaction_hash` validation.
  Default: off.

## Quick Start

Run one incremental pass against the sample data copied into this repository:

```bash
uv run flash-dataset run-daily \
  --working-directory /Users/mike/zuy-ai/data-base/out/runtime \
  --storage-directory /Users/mike/zuy-ai/data-base/data \
  --run-at off
```

Run one direct validator pass without the runtime watermark:

```bash
uv run flash-dataset validate-parquet \
  --storage-directory /Users/mike/zuy-ai/data-base/data \
  --output-directory /Users/mike/zuy-ai/data-base/out/reports
```

Schedule one UTC run every day at `6:00AM`:

```bash
uv run flash-dataset run-daily \
  --working-directory /data/working \
  --storage-directory /data/storage \
  --run-at 6:00AM \
  --max-days-back 2
```

Each `run-daily` pass prints one compact JSON summary to stdout and writes:
- `reports/YYYY-MM-DDTHH-MM-SSZ[-NN]/summary.md`
- `reports/YYYY-MM-DDTHH-MM-SSZ[-NN]/findings.jsonl`
- `reports/YYYY-MM-DDTHH-MM-SSZ[-NN]/baseline-snapshots.json` after the runtime establishes the
  first persisted watermark
- `reports/latest/summary.md`
- `reports/latest/findings.jsonl`
- `reviewed-days.json` after the runtime establishes the first persisted review state
- `baseline-snapshots.json` after the runtime establishes the first persisted watermark
- `latest-reviewed.txt` after the runtime establishes the first persisted review state

under `--working-directory`. The `validate-parquet` command writes reports to
`--output-directory` without the runtime watermark.
For `run-daily`, `summary.md` is the runtime-owned run summary, including runtime status and
validator finding counts. `findings.jsonl` remains the raw validator finding stream for that run.

If no newer day folders exist, the program writes a no-op report and exits successfully. Scheduled
mode keeps running one-day passes until it reaches `idle` or `blocked`. The one-shot
`run-daily --run-at off` path reports only the next reviewable day: it exits non-zero when that day
is missing, empty, or the validator emits fail findings for the selected day.

## Watermark Rules

- The runtime layer discovers day partitions from `year=YYYY/month=MM/day=DD`.
- `reviewed-days.json` stores every day that has already completed a runtime pass.
- `latest-reviewed.txt` stores the newest reviewed day in ISO format such as `2026-04-13`.
  Those files do not exist until the runtime completes its first review.
- The runtime ignores day folders more than `--max-days-back` days older than the newest
  discovered folder.
- Each runtime pass validates the earliest unreviewed day inside the active review window that
  currently contains parquet files.
- Scheduled mode keeps invoking those single-day passes until there are no more unseen eligible
  days in the window or no remaining unreviewed day currently has parquet files.
- Missing day folders do not block later completed days in the same window.
- Empty discovered day folders do not block later completed days in the same window either, but if
  only empty unreviewed days remain then the runtime reports `blocked`.
- A one-shot runtime pass reports `blocked` only when unreviewed day folders exist in the active
  window but none of them currently contain parquet files. Validator warnings and failures are
  still reported for the selected day, but they do not stall runtime progress.
- The runtime refuses an unreadable `latest-reviewed.txt`, unreadable storage folders, or a stored
  watermark newer than the newest discovered day folder as configuration errors. Dangling
  `latest-reviewed.txt` or `reviewed-days.json` symlinks are treated as corrupted runtime state,
  not as a fresh bootstrap. Scheduled mode validates that runtime state before its first sleep, so
  bad state fails fast at startup.
- The runtime also persists compact committed baseline snapshots beside the watermark so one-day
  runs can still compare the current day against prior healthy same-hour peers inside the active
  review window.
- The runtime also refuses any storage tree that is not strict top-level
  `year=YYYY/month=MM/day=DD`, with one reserved exception for a top-level `tmp/` staging
  directory. Validator-only `date=...`, dataset-first, other unknown top-level roots, unsupported
  top-level files, and malformed runtime partition directories are still rejected instead of
  misreporting them as idle.
- After every selected day that completes a runtime pass, the runtime records that day in
  `reviewed-days.json` and refreshes `latest-reviewed.txt` to the newest reviewed day. Validator
  warnings and failures do not suppress that state update. The top-level state files still resolve
  through the stable `reports/latest/current -> reports/<run>/` pointer, so the committed latest
  report and committed runtime state move together.

This is intentionally simple. If more files arrive later inside an already reviewed `day=DD`
folder, that day is not revalidated automatically.

## Received Time Rule

For `flashblocks`, a row is valid as long as at least one of:
- `received_time_a`
- `received_time_b`
- `received_time_c`

is populated.

The validator still records coverage metrics for each slot, and it warns only when a row is
missing all three timestamps.

## Docker Compose

Build the image from this repository and run it as one long-lived service:

```yaml
services:
  data-base:
    build: .
    volumes:
      - ./feed-base:/data/storage:ro
      - ./data-base-runtime:/data/working
```

The image entrypoint is `flash-dataset` and the image default command is:

```text
run-daily --working-directory=/data/working --storage-directory=/data/storage --run-at=6:00AM --max-days-back=2
```

Override `command:` only if you need non-default paths or schedule. Use `run-daily --run-at=off`
when you want a one-shot incremental execution, or `validate-parquet` for a direct validator run.
The container runs as an unprivileged user, so the host path behind `/data/working` must be
writable by that UID.
When scheduled mode starts after the configured UTC slot, it immediately runs one catch-up pass
before sleeping until the next day instead of idling until tomorrow.

## Build And Upload

The current `Dockerfile` does not use `TARGET_CPU`-specific build arguments or a `netrc` secret,
so the publish commands are simpler than the `feed-base` image build.

### Login

```bash
aws ecr get-login-password --profile zuy --region us-east-1 | docker login --username AWS --password-stdin 572954863739.dkr.ecr.us-east-1.amazonaws.com
```

### Build And Push

```bash
docker buildx build --platform linux/amd64 -t 572954863739.dkr.ecr.us-east-1.amazonaws.com/data-base:amd64 --push .
```

If you want one local image without pushing it:

```bash
docker build -t data-base:local .
```

## Supported Layouts

`validate-parquet` still understands:
- production `feed-base` output: `date=YYYY-MM-DD/<dataset>_YYYY-MM-DD.parquet`
- current partitioned output:
  `year=YYYY/month=MM/day=DD/hour=HH/<dataset>_YYYY-MM-DD_HH.parquet`
- fixture-friendly copied layout:
  `<dataset>/year=YYYY/month=MM/day=DD/<dataset>_YYYY-MM-DD.parquet`
- a top-level `tmp/` subtree is ignored so external writers can stage files there safely

`run-daily` is narrower: it requires the current strict top-level
`year=YYYY/month=MM/day=DD` storage tree, but it ignores one reserved top-level `tmp/` staging
directory. It still fails fast on `date=...`, dataset-first, other unknown top-level roots, or
malformed runtime partition directories.

## Development Checks

Run the local verification stack with:

```bash
uv run pytest -q
uv run pyright
uv run ruff check .
uv run black --check .
```
