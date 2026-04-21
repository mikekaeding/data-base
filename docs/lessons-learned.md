# Lessons Learned

- Validate the physical parquet footer schema, not the dataset view returned by hive-style path
  reads. Path-derived partition columns can hide real schema drift if you validate the wrong layer.
- `flashblocks` is the only reliable parent table. High-confidence validation comes from proving
  every child table agrees with it on keys and counts.
- Base-only fields belong exclusively to `index == 0`. Treat that as a first-class invariant.
- A `flashblocks` row does not need every `received_time_*` column populated. The useful invariant
  is that at least one source timestamp exists.
- A single day watermark is not enough once review order can skip gaps. Persist the set of already
  reviewed days separately so a later-arriving gap day can still be picked up without reopening
  every newer day.
- Missing day folders should not stall later completed days in the same review window. Treat
  missing days as absent work, not as a hard stop.
- Empty discovered day folders should only block when they are the only unreviewed in-window work
  left. If a later day is complete, review it now and come back to the empty day later.
- Persist initial review state immediately after the first completed runtime pass. Delaying the
  first state write makes later gap handling and restart behavior harder to reason about.
- Runtime filesystem errors must fail closed. An unreadable mount or unreadable state file should
  never look like an idle run or an empty day.
- An incremental scheduler must reject unsupported storage layouts up front. If the runtime only
  understands `year/month/day`, anything outside that strict root contract should be a
  configuration error, not a silent idle run. That includes stray top-level files, not just
  unexpected directories.
- Day-level runtime progress should track day availability, not validator cleanliness. Once the
  runtime selects a reviewable day and finishes that pass, later eligible days should stay
  reviewable even if the validator reported warnings or failures for the selected day. Keep those
  findings in the report and exit code instead of turning them into a stalled watermark.
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
- Baselines do not need a separate database to be useful. Compact committed snapshots alongside the
  runtime watermark are enough to preserve day-by-day drift checks without reopening older
  partitions for full validation.
- `pyarrow` remains a weakly typed dependency from a static-analysis perspective. Keep its dynamic
  compute calls behind narrow wrappers, and prefer small repo-local stubs for the Arrow surface you
  actually use over weakening strict type checks in the validator modules.
