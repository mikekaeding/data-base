# Lessons Learned

- Validate the physical parquet footer schema, not the dataset view returned by hive-style path
  reads. Path-derived partition columns can hide real schema drift if you validate the wrong layer.
- `flashblocks` is the only reliable parent table. High-confidence validation comes from proving
  every child table agrees with it on keys and counts.
- Base-only fields belong exclusively to `index == 0`. Treat that as a first-class invariant.
- A `flashblocks` row does not need every `received_time_*` column populated. The useful invariant
  is that at least one source timestamp exists.
- A simple day watermark is operationally cheap and easy to reason about, but it only works well
  when data arrives in new `year/month/day` folders instead of mutating older days.
- With only one stored day watermark, correctness depends on monotonic coverage: do not advance
  past an earlier pending day that is missing or still has no parquet, and do not advance after a
  failed run.
- A day directory being non-empty is not enough to advance a day watermark. Only advance through
  days that actually produced reviewed validator partitions.
- The first watermark is special. Without prior state, you can validate available days
  immediately, but you should not persist a latest-reviewed day until the reviewed run reaches the
  oldest day inside the active review window.
- Runtime filesystem errors must fail closed. An unreadable mount or unreadable state file should
  never look like an idle run or an empty day.
- An incremental scheduler must reject unsupported storage layouts up front. If the runtime only
  understands `year/month/day`, anything outside that strict root contract should be a
  configuration error, not a silent idle run. That includes stray top-level files, not just
  unexpected directories.
- Day-level runtime progress must be stricter than partition-level review. If any non-metric
  finding lands in a selected day, that day is still blocking runtime progress even when another
  partition in the same day reviewed successfully. Report it as blocked so one-shot automation does
  not treat a stalled watermark as success.
- Runtime reports must be owned by the runtime layer. If post-validation `summary.md` only reflects
  validator-local counts, operators lose the blocked/idle/validated state that explains whether the
  scheduler actually made progress.
- Runtime state and runtime reports must advance together. If `latest-reviewed.txt` is written
  separately from `reports/latest/summary.md`, a crash window will leave one ahead of the other.
  Keep the watermark snapshot inside each run directory and promote both through one stable latest
  pointer so hard kills cannot split committed state from committed reports. Do not publish state
  symlinks before a real watermark exists, and treat any dangling watermark symlink as corrupted
  runtime state instead of silently bootstrapping from scratch.
- Scheduled mode should validate persisted runtime state before its first sleep, not only before
  the first actual run, or operators will not see broken state until the next slot arrives.
- Scheduled mode should anchor its first pass to today's configured UTC slot instead of the next
  future slot. Otherwise a restart after the daily time can idle for nearly a day even when work is
  already available.
- Large storage scans do not need full-history review on every run. A small day window relative to
  the newest discovered folder bounds startup cost while keeping recent incremental review simple.
- Date-window pruning must only skip valid out-of-range directories. If you prune malformed layout
  roots too early, filtered runs hide `partition_parse_error` findings and misreport the run as
  `no_matching_parquet_files`.
- Baselines do not need a separate database to be useful. A single validation run can still detect
  meaningful drift by comparing each partition against other comparable partitions discovered under
  the same storage root.
- `pyarrow` remains a weakly typed dependency from a static-analysis perspective. Keep its dynamic
  compute calls behind narrow wrappers, and prefer small repo-local stubs for the Arrow surface you
  actually use over weakening strict type checks in the validator modules.
