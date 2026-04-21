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
  Writable directory for reports and the `latest-reviewed.txt` watermark.
  Default: `/data/working`
- `--storage-directory PATH`
  The parquet storage directory to scan.
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
- `baseline-snapshots.json` after the runtime establishes the first persisted watermark
- `latest-reviewed.txt` after the runtime establishes the first contiguous day watermark

under `--working-directory`. The `validate-parquet` command writes reports to
`--output-directory` without the runtime watermark.
For `run-daily`, `summary.md` is the runtime-owned run summary, including runtime status and
validator finding counts. `findings.jsonl` remains the raw validator finding stream for that run.

If no newer day folders exist, the program writes a no-op report and exits successfully. Scheduled
mode keeps running one-day passes until it reaches `idle` or `blocked`. The one-shot
`run-daily --run-at off` path reports only the next expected day: it exits non-zero when that day
is missing, empty, or has non-metric findings.

## Watermark Rules

- The runtime layer discovers day partitions from `year=YYYY/month=MM/day=DD`.
- `latest-reviewed.txt` stores the newest reviewed day in ISO format such as `2026-04-13`.
  That file does not exist until the runtime establishes the first persisted watermark.
- The runtime ignores day folders more than `--max-days-back` days older than the newest
  discovered folder.
- Each runtime pass validates only the next expected day inside the active review window.
- Scheduled mode keeps invoking those single-day passes until there are no more unseen eligible
  days in the window or the next expected day is blocked.
- The watermark stops before the first missing or empty pending day so a later day cannot advance
  past an earlier gap.
- A one-shot runtime pass reports `blocked` when the next expected day is missing, empty, or has
  any non-metric findings, even if another partition in that same day reviewed successfully.
- On a fresh working directory, the runtime establishes the first watermark only after the
  selected day reaches the oldest day inside the current review window.
- The runtime refuses an unreadable `latest-reviewed.txt`, unreadable storage folders, or a stored
  watermark newer than the newest discovered day folder as configuration errors. A dangling
  `latest-reviewed.txt` symlink is treated as corrupted runtime state, not as a fresh bootstrap.
  Scheduled mode validates that runtime state before its first sleep, so a bad watermark fails
  fast at startup.
- The runtime also persists compact committed baseline snapshots beside the watermark so one-day
  runs can still compare the current day against prior healthy same-hour peers inside the active
  review window.
- The runtime also refuses any storage tree that is not strict top-level
  `year=YYYY/month=MM/day=DD`, including validator-only `date=...`, dataset-first, unknown
  top-level roots, unsupported top-level files, and malformed runtime partition directories,
  instead of misreporting them as idle.
- The watermark advances only when that single-day validator run finishes without fail findings, the
  day being stored produced at least one reviewed validator partition and no non-metric findings
  across the day, and the runtime successfully promotes that run through the stable
  `reports/latest/current -> reports/<run>/` pointer. The top-level `latest-reviewed.txt` resolves
  through that same pointer, so the committed latest report and committed watermark move together.

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

`run-daily` is narrower: it requires the current strict top-level
`year=YYYY/month=MM/day=DD` storage tree and fails fast on `date=...`, dataset-first, unknown
top-level roots, or malformed runtime partition directories.

## Development Checks

Run the local verification stack with:

```bash
uv run pytest -q
uv run pyright
uv run ruff check .
uv run black --check .
```
