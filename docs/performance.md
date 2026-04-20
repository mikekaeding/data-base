# Performance

## Throughput Strategy

- Discover candidate work from top-level `year/month/day` folders before touching parquet files.
- Fail scheduled runtime startup fast when the storage root does not expose that `year/month/day`
  layout, instead of sleeping first and surfacing the problem only at the next run slot.
- If the process restarts after the configured UTC slot, run the pending daily pass immediately
  instead of idling until tomorrow and stretching backlog latency by almost a full day.
- Skip days at or before the stored watermark without opening their parquet data.
- Bound the candidate scan to the newest discovered day and ignore folders older than
  `max_days_back` relative to that newest day.
- Stop candidate selection at the first missing or empty later day so the single-day watermark
  never advances past an earlier gap.
- Keep malformed in-range storage paths visible to discovery while pruning only valid out-of-range
  subtrees; otherwise filtered runs can hide parse warnings behind false `no_matching` results.
- For date-bounded runs, prune discovery to only the supported `date=...` or `year/month/day`
  partition roots that overlap the selected window before collecting file paths.
- Parse parquet paths first and open parquet footers only for files whose partition day falls
  inside the selected validation window.
- For selected days, discover parquet files once and validate partition-by-partition.
- Read only the columns needed for the current rule group.
- Reuse in-memory Arrow tables within one partition through the partition-table cache.
- Use Arrow group-by, join, and filter operations for large count and key checks.
- After partition-local validation, derive compact baseline snapshots and compare them across the
  current run instead of rereading full partitions from scratch.
- Fall back to Python iteration only for small or inherently row-oriented contracts such as
  access-list grouping, authorization ordinal checks, and transaction RLP hash validation.

## Hot Paths

- Recursive day-folder scans are cheap compared with parquet validation because they only inspect
  directory names and check whether a pending day contains any parquet files.
- Date-bounded validator runs avoid full-history parquet path walks; they enumerate candidate
  partition roots first and only recurse within the requested day window.
- `flashblock_balance_changes` row counting and grouping can dominate wide-partition scans because
  the table is often much larger than the others.
- `flashblock_receipt_logs` grouping is the next major cost because it combines high row counts
  with per-receipt join checks.
- Baseline collection is relatively cheap because it only reads a few additional count and type
  columns per partition plus parquet footer stats already available from discovery.
- `keccak(transaction_rlp)` validation is the most expensive Python-side semantic check because it
  hashes every stored transaction row, so it stays disabled unless explicitly requested.

## Constraints

- Incremental scheduling is optimized for append-by-day storage, not for detecting late writes into
  already reviewed day folders.
- Partition validation remains correctness-first. Some semantic checks intentionally scan entire
  partition tables because partial sampling would weaken confidence in stored parquet correctness.
- Drift warnings only fire when at least three healthy peer partitions for the same UTC hour are
  present in the current run, which keeps the baseline layer self-contained and avoids inventing
  unstable thresholds from one or two samples or from unlike hour buckets.
