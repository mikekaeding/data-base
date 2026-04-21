"""Incremental scheduled runner for feed-base parquet validation."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
import json
import os
from pathlib import Path
import time as time_module
from typing import Literal
from typing import cast

from .errors import ValidatorConfigurationError
from .validator import ValidatorConfig
from .validator import run_validation
from .validator.baselines import DatasetPhysicalSnapshot
from .validator.baselines import DistributionSummary
from .validator.baselines import PartitionSnapshot
from .validator.common import Finding
from .validator.common import PartitionIdentity
from .validator.discovery import validate_writable_directory_target

RUN_DIRECTORY_FORMAT = "%Y-%m-%dT%H-%M-%SZ"
STATE_FILE_NAME = "latest-reviewed.txt"
BASELINE_SNAPSHOTS_FILE_NAME = "baseline-snapshots.json"
SUMMARY_FILE_NAME = "summary.md"
FINDINGS_FILE_NAME = "findings.jsonl"
LATEST_POINTER_NAME = "current"
DEFAULT_WORKING_DIRECTORY = Path("/data/working")
DEFAULT_STORAGE_DIRECTORY = Path("/data/storage")
DEFAULT_RUN_AT = time(hour=6, minute=0)
DEFAULT_MAX_DAYS_BACK = 2


@dataclass(frozen=True)
class RuntimeConfig:
    """Application configuration for one-shot or scheduled validation.

    Args:
        working_directory: Writable directory that stores reports and incremental state.
        storage_directory: Root directory containing feed-base parquet output.
        run_at: Optional daily UTC wall-clock time for scheduled execution.
        max_days_back: Maximum day distance from the newest discovered folder to inspect.
        verify_transaction_hashes: Whether to run keccak(transaction_rlp) checks.
    """

    working_directory: Path = DEFAULT_WORKING_DIRECTORY
    storage_directory: Path = DEFAULT_STORAGE_DIRECTORY
    run_at: time | None = DEFAULT_RUN_AT
    max_days_back: int = DEFAULT_MAX_DAYS_BACK
    verify_transaction_hashes: bool = False


@dataclass(frozen=True)
class DayPartition:
    """One discovered `year=YYYY/month=MM/day=DD` partition directory.

    Args:
        partition_date: Calendar day encoded by the folder names.
        path: Filesystem path for the day directory.
    """

    partition_date: date
    path: Path


@dataclass(frozen=True)
class RunSummary:
    """Application-level summary for one scheduled or one-shot run.

    Args:
        status: Whether the run completed validation, is blocked by a missing or invalid selected day, or found no work.
        working_directory: Writable runtime directory.
        storage_directory: Input parquet storage directory.
        report_directory: Output directory for this run's reports.
        blocked_day: First calendar day currently blocking progress, if any.
        latest_reviewed_day: Latest persisted day watermark after the run, if any.
        discovered_day_count: Total day directories found under the storage directory.
        pending_day_count: Day directories later than the stored watermark and within the review window.
        reviewed_day_count: Distinct day directories that yielded at least one reviewed validator partition.
        reviewed_partition_count: Validator partitions reviewed in this run.
        fail_count: Number of fail findings emitted by the validator.
        warn_count: Number of warn findings emitted by the validator.
        metric_count: Number of metric findings emitted by the validator.
        has_failures: Whether the validator emitted any fail findings.
        rule_counts: Finding counts by rule id for validated runs.
    """

    status: Literal["idle", "blocked", "validated"]
    working_directory: str
    storage_directory: str
    report_directory: str
    blocked_day: str | None
    latest_reviewed_day: str | None
    discovered_day_count: int
    pending_day_count: int
    reviewed_day_count: int
    reviewed_partition_count: int
    fail_count: int
    warn_count: int
    metric_count: int
    has_failures: bool
    rule_counts: dict[str, int]

    def exit_code(self) -> int:
        """Return the process exit code for a completed one-shot run."""
        return 1 if self.has_failures or self.status == "blocked" else 0

    def to_payload(self) -> dict[str, object]:
        """Return one compact JSON-safe payload for logs and CLI output."""
        return {
            "status": self.status,
            "working_directory": self.working_directory,
            "storage_directory": self.storage_directory,
            "report_directory": self.report_directory,
            "blocked_day": self.blocked_day,
            "latest_reviewed_day": self.latest_reviewed_day,
            "discovered_day_count": self.discovered_day_count,
            "pending_day_count": self.pending_day_count,
            "reviewed_day_count": self.reviewed_day_count,
            "reviewed_partition_count": self.reviewed_partition_count,
            "fail_count": self.fail_count,
            "warn_count": self.warn_count,
            "metric_count": self.metric_count,
            "has_failures": self.has_failures,
            "rule_counts": dict(self.rule_counts),
        }


def parse_daily_utc_time(value: str) -> time:
    """Parse one daily UTC time string like `2:30AM` into a `time` object."""
    normalized = value.strip().upper().replace(" ", "")
    try:
        return datetime.strptime(normalized, "%I:%M%p").time()
    except ValueError as error:
        raise ValueError(f"invalid run time: {value}") from error


def next_scheduled_run(now: datetime, run_at: time) -> datetime:
    """Return the next UTC datetime at which the scheduled run should start."""
    now = _as_utc(now)
    candidate = datetime.combine(now.date(), run_at, tzinfo=UTC)
    if candidate < now:
        candidate += timedelta(days=1)
    return candidate


def discover_day_partitions(storage_directory: Path) -> list[DayPartition]:
    """Return sorted day directories discovered from `year/month/day` folders."""
    partitions: list[DayPartition] = []
    for year_directory in _matching_subdirectories(storage_directory, "year="):
        year = _parse_number_component(year_directory.name, "year")
        if year is None:
            continue
        for month_directory in _matching_subdirectories(year_directory, "month="):
            month = _parse_number_component(month_directory.name, "month")
            if month is None:
                continue
            for day_directory in _matching_subdirectories(month_directory, "day="):
                day_number = _parse_number_component(day_directory.name, "day")
                if day_number is None:
                    continue
                try:
                    partition_date = date(year, month, day_number)
                except ValueError:
                    continue
                partitions.append(
                    DayPartition(partition_date=partition_date, path=day_directory)
                )
    partitions.sort(key=lambda partition: partition.partition_date)
    return partitions


def run_once(config: RuntimeConfig, run_time: datetime | None = None) -> RunSummary:
    """Run one incremental validation pass and update persisted state on completion."""
    validate_runtime_config(config)
    run_time = _utc_now() if run_time is None else _as_utc(run_time)
    (
        state_path,
        baseline_snapshots_path,
        stored_watermark,
        committed_snapshots,
        discovered_days,
    ) = _load_runtime_state(config)
    oldest_allowed_day = _oldest_allowed_day(discovered_days, config.max_days_back)
    pending_days = _select_pending_days(
        discovered_days, stored_watermark, oldest_allowed_day
    )
    target_day, blocked_day = _select_next_reviewable_day(
        pending_days, stored_watermark, oldest_allowed_day
    )
    persisted_snapshots = _pruned_committed_snapshots(
        committed_snapshots,
        oldest_allowed_day,
        stored_watermark,
    )

    report_directory = _build_report_directory(config.working_directory, run_time)
    if target_day is None:
        status = "blocked" if blocked_day is not None else "idle"
        summary = RunSummary(
            status=status,
            working_directory=str(config.working_directory),
            storage_directory=str(config.storage_directory),
            report_directory=str(report_directory),
            blocked_day=None if blocked_day is None else blocked_day.isoformat(),
            latest_reviewed_day=(
                None if stored_watermark is None else stored_watermark.isoformat()
            ),
            discovered_day_count=len(discovered_days),
            pending_day_count=len(pending_days),
            reviewed_day_count=0,
            reviewed_partition_count=0,
            fail_count=0,
            warn_count=0,
            metric_count=0,
            has_failures=False,
            rule_counts={},
        )
        _finalize_run_outputs(
            summary,
            latest_directory=config.working_directory / "reports" / "latest",
            state_path=state_path,
            baseline_snapshots_path=baseline_snapshots_path,
            persisted_watermark=stored_watermark,
            persisted_snapshots=persisted_snapshots,
            status_text=_runtime_status_text(
                summary.status, blocked_by_pending_day=blocked_day is not None
            ),
            write_empty_findings=True,
        )
        return summary

    blocked_by_pending_day = blocked_day is not None
    result = run_validation(
        ValidatorConfig(
            storage_root=config.storage_directory,
            output_directory=report_directory,
            date_from=target_day.partition_date,
            date_to=target_day.partition_date,
            verify_transaction_hashes=config.verify_transaction_hashes,
        ),
        comparison_snapshots=_comparison_snapshots_for_target_day(
            persisted_snapshots,
            oldest_allowed_day,
            target_day.partition_date,
        ),
    )
    counts = result.finding_counts_by_status()
    reviewed_partition_dates = _reviewed_partition_dates(result.reviewed_partitions)
    issue_dates = _issue_dates(result.findings, config.storage_directory)
    reviewed_watermark_days = _contiguous_reviewed_days(
        [target_day],
        reviewed_partition_dates,
        issue_dates,
    )
    blocked_day = _blocked_day_after_validation(
        [target_day],
        reviewed_watermark_days,
        blocked_day,
    )
    persisted_watermark = stored_watermark
    if not result.has_failures() and _should_store_latest_reviewed_day(
        stored_watermark, reviewed_watermark_days, oldest_allowed_day
    ):
        persisted_watermark = reviewed_watermark_days[-1].partition_date
        persisted_snapshots = _next_committed_snapshots(
            persisted_snapshots,
            result.partition_snapshots,
            oldest_allowed_day,
            persisted_watermark,
        )
    summary = RunSummary(
        status="blocked" if blocked_day is not None else "validated",
        working_directory=str(config.working_directory),
        storage_directory=str(config.storage_directory),
        report_directory=str(report_directory),
        blocked_day=None if blocked_day is None else blocked_day.isoformat(),
        latest_reviewed_day=(
            None if persisted_watermark is None else persisted_watermark.isoformat()
        ),
        discovered_day_count=len(discovered_days),
        pending_day_count=len(pending_days),
        reviewed_day_count=len(reviewed_partition_dates),
        reviewed_partition_count=len(result.reviewed_partitions),
        fail_count=counts["fail"],
        warn_count=counts["warn"],
        metric_count=counts["metric"],
        has_failures=result.has_failures(),
        rule_counts=dict(result.rule_counts),
    )
    _finalize_run_outputs(
        summary,
        latest_directory=config.working_directory / "reports" / "latest",
        state_path=state_path,
        baseline_snapshots_path=baseline_snapshots_path,
        persisted_watermark=persisted_watermark,
        persisted_snapshots=persisted_snapshots,
        status_text=_runtime_status_text(
            summary.status, blocked_by_pending_day=blocked_by_pending_day
        ),
        write_empty_findings=False,
    )
    return summary


def run_scheduled(
    config: RuntimeConfig,
    emit_summary: Callable[[RunSummary], None],
    sleep: Callable[[float], None] = time_module.sleep,
    now_provider: Callable[[], datetime] | None = None,
) -> None:
    """Run incremental validation forever at the configured daily UTC time."""
    if config.run_at is None:
        raise ValidatorConfigurationError("run_at is required for scheduled execution")
    validate_runtime_config(config)
    _load_runtime_state(config)
    now_provider = _utc_now if now_provider is None else now_provider
    next_run = datetime.combine(
        _as_utc(now_provider()).date(), config.run_at, tzinfo=UTC
    )
    while True:
        now = _as_utc(now_provider())
        sleep_seconds = (next_run - now).total_seconds()
        if sleep_seconds > 0:
            sleep(sleep_seconds)
        while True:
            summary = run_once(config, run_time=now_provider())
            emit_summary(summary)
            if summary.status != "validated":
                break
        next_run = datetime.combine(
            next_run.date() + timedelta(days=1), config.run_at, tzinfo=UTC
        )


def _finalize_run_outputs(
    summary: RunSummary,
    latest_directory: Path,
    state_path: Path,
    baseline_snapshots_path: Path,
    persisted_watermark: date | None,
    persisted_snapshots: list[PartitionSnapshot],
    status_text: str,
    write_empty_findings: bool,
) -> None:
    _write_runtime_report(
        summary,
        status_text=status_text,
        write_empty_findings=write_empty_findings,
    )
    report_directory = Path(summary.report_directory)
    _write_report_state_files(
        report_directory,
        persisted_watermark,
        persisted_snapshots,
    )
    _promote_latest_report(
        report_directory,
        latest_directory,
        state_path,
        baseline_snapshots_path,
        has_persisted_watermark=persisted_watermark is not None,
    )


def _load_runtime_state(
    config: RuntimeConfig,
) -> tuple[Path, Path, date | None, list[PartitionSnapshot], list[DayPartition]]:
    try:
        config.working_directory.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ValidatorConfigurationError(
            f"working directory is not writable: {config.working_directory}"
        ) from error
    state_path = config.working_directory / STATE_FILE_NAME
    baseline_snapshots_path = config.working_directory / BASELINE_SNAPSHOTS_FILE_NAME
    stored_watermark = load_latest_reviewed_day(state_path)
    committed_snapshots = _load_committed_baseline_snapshots(baseline_snapshots_path)
    discovered_days = discover_day_partitions(config.storage_directory)
    _validate_stored_watermark(state_path, stored_watermark, discovered_days)
    _validate_committed_snapshots(
        baseline_snapshots_path,
        stored_watermark,
        committed_snapshots,
    )
    return (
        state_path,
        baseline_snapshots_path,
        stored_watermark,
        committed_snapshots,
        discovered_days,
    )


def load_latest_reviewed_day(state_path: Path) -> date | None:
    """Read the persisted ISO day watermark from disk when it exists."""
    try:
        raw_value = state_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        if state_path.is_symlink():
            raise ValidatorConfigurationError(
                f"latest reviewed day is a dangling symlink: {state_path}"
            ) from None
        return None
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to read latest reviewed day: {state_path}"
        ) from error
    if raw_value == "":
        return None
    try:
        return date.fromisoformat(raw_value)
    except ValueError as error:
        raise ValidatorConfigurationError(
            f"invalid latest reviewed day in {state_path}: {raw_value}"
        ) from error


def store_latest_reviewed_day(state_path: Path, latest_reviewed_day: date) -> None:
    """Persist one ISO day watermark to the provided state file path."""
    try:
        state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = state_path.with_name(f"{state_path.name}.tmp")
        temporary_path.write_text(
            f"{latest_reviewed_day.isoformat()}\n", encoding="utf-8"
        )
        temporary_path.replace(state_path)
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to write latest reviewed day: {state_path}"
        ) from error


def _load_committed_baseline_snapshots(
    baseline_snapshots_path: Path,
) -> list[PartitionSnapshot]:
    try:
        raw_value = baseline_snapshots_path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        if baseline_snapshots_path.is_symlink():
            raise ValidatorConfigurationError(
                "committed baseline snapshots are a dangling symlink: "
                f"{baseline_snapshots_path}"
            ) from None
        return []
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to read committed baseline snapshots: {baseline_snapshots_path}"
        ) from error
    if raw_value == "":
        return []
    try:
        payload = json.loads(raw_value)
    except json.JSONDecodeError as error:
        raise ValidatorConfigurationError(
            f"invalid committed baseline snapshots in {baseline_snapshots_path}"
        ) from error
    if not isinstance(payload, list):
        raise ValidatorConfigurationError(
            f"invalid committed baseline snapshots in {baseline_snapshots_path}"
        )
    payload_list = cast(list[object], payload)
    try:
        snapshots = [_partition_snapshot_from_payload(item) for item in payload_list]
    except (KeyError, TypeError, ValueError) as error:
        raise ValidatorConfigurationError(
            f"invalid committed baseline snapshots in {baseline_snapshots_path}"
        ) from error
    return sorted(snapshots, key=lambda snapshot: snapshot.partition)


def _store_committed_baseline_snapshots(
    baseline_snapshots_path: Path,
    committed_snapshots: list[PartitionSnapshot],
) -> None:
    try:
        baseline_snapshots_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = baseline_snapshots_path.with_name(
            f"{baseline_snapshots_path.name}.tmp"
        )
        payload = [
            _partition_snapshot_payload(snapshot)
            for snapshot in sorted(
                committed_snapshots,
                key=lambda snapshot: snapshot.partition,
            )
        ]
        temporary_path.write_text(
            f"{json.dumps(payload, sort_keys=True)}\n",
            encoding="utf-8",
        )
        temporary_path.replace(baseline_snapshots_path)
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to write committed baseline snapshots: {baseline_snapshots_path}"
        ) from error


def _partition_snapshot_payload(snapshot: PartitionSnapshot) -> dict[str, object]:
    return {
        "partition_date": snapshot.partition.partition_date.isoformat(),
        "partition_hour": snapshot.partition.partition_hour,
        "dataset_snapshots": [
            _dataset_physical_snapshot_payload(dataset_snapshot)
            for dataset_snapshot in snapshot.dataset_snapshots
        ],
        "empty_datasets": list(snapshot.empty_datasets),
        "payload_lengths": list(snapshot.payload_lengths),
        "payload_length_summary": _distribution_summary_payload(
            snapshot.payload_length_summary
        ),
        "tail_only_payload_count": snapshot.tail_only_payload_count,
        "transaction_count_summary": _distribution_summary_payload(
            snapshot.transaction_count_summary
        ),
        "receipt_log_count_summary": _distribution_summary_payload(
            snapshot.receipt_log_count_summary
        ),
        "balance_change_count_summary": _distribution_summary_payload(
            snapshot.balance_change_count_summary
        ),
        "withdrawal_count_summary": _distribution_summary_payload(
            snapshot.withdrawal_count_summary
        ),
        "transaction_type_counts": [
            {"label": label, "count": count}
            for label, count in snapshot.transaction_type_counts
        ],
        "total_transactions": snapshot.total_transactions,
        "peer_eligible": snapshot.peer_eligible,
    }


def _dataset_physical_snapshot_payload(
    dataset_snapshot: DatasetPhysicalSnapshot,
) -> dict[str, object]:
    return {
        "dataset": dataset_snapshot.dataset,
        "row_count": dataset_snapshot.row_count,
        "file_size_bytes": dataset_snapshot.file_size_bytes,
        "row_groups": dataset_snapshot.row_groups,
        "compressed_bytes": dataset_snapshot.compressed_bytes,
        "uncompressed_bytes": dataset_snapshot.uncompressed_bytes,
    }


def _distribution_summary_payload(summary: DistributionSummary) -> dict[str, object]:
    return {
        "count": summary.count,
        "minimum": summary.minimum,
        "median_value": summary.median_value,
        "maximum": summary.maximum,
    }


def _partition_snapshot_from_payload(payload: object) -> PartitionSnapshot:
    payload_map = _payload_mapping(payload)
    return PartitionSnapshot(
        partition=PartitionIdentity(
            partition_date=date.fromisoformat(
                _payload_string(payload_map, "partition_date")
            ),
            partition_hour=_payload_optional_int(payload_map, "partition_hour"),
        ),
        dataset_snapshots=tuple(
            _dataset_physical_snapshot_from_payload(item)
            for item in _payload_list(payload_map, "dataset_snapshots")
        ),
        empty_datasets=tuple(_payload_string_list(payload_map, "empty_datasets")),
        payload_lengths=tuple(_payload_int_list(payload_map, "payload_lengths")),
        payload_length_summary=_distribution_summary_from_payload(
            _payload_value(payload_map, "payload_length_summary")
        ),
        tail_only_payload_count=_payload_int(payload_map, "tail_only_payload_count"),
        transaction_count_summary=_distribution_summary_from_payload(
            _payload_value(payload_map, "transaction_count_summary")
        ),
        receipt_log_count_summary=_distribution_summary_from_payload(
            _payload_value(payload_map, "receipt_log_count_summary")
        ),
        balance_change_count_summary=_distribution_summary_from_payload(
            _payload_value(payload_map, "balance_change_count_summary")
        ),
        withdrawal_count_summary=_distribution_summary_from_payload(
            _payload_value(payload_map, "withdrawal_count_summary")
        ),
        transaction_type_counts=tuple(
            _transaction_type_count_from_payload(item)
            for item in _payload_list(payload_map, "transaction_type_counts")
        ),
        total_transactions=_payload_int(payload_map, "total_transactions"),
        peer_eligible=_payload_bool(payload_map, "peer_eligible"),
    )


def _dataset_physical_snapshot_from_payload(
    payload: object,
) -> DatasetPhysicalSnapshot:
    payload_map = _payload_mapping(payload)
    return DatasetPhysicalSnapshot(
        dataset=_payload_string(payload_map, "dataset"),
        row_count=_payload_int(payload_map, "row_count"),
        file_size_bytes=_payload_int(payload_map, "file_size_bytes"),
        row_groups=_payload_int(payload_map, "row_groups"),
        compressed_bytes=_payload_int(payload_map, "compressed_bytes"),
        uncompressed_bytes=_payload_int(payload_map, "uncompressed_bytes"),
    )


def _distribution_summary_from_payload(payload: object) -> DistributionSummary:
    payload_map = _payload_mapping(payload)
    return DistributionSummary(
        count=_payload_int(payload_map, "count"),
        minimum=_payload_optional_int(payload_map, "minimum"),
        median_value=_payload_optional_float(payload_map, "median_value"),
        maximum=_payload_optional_int(payload_map, "maximum"),
    )


def _transaction_type_count_from_payload(payload: object) -> tuple[str, int]:
    payload_map = _payload_mapping(payload)
    return (
        _payload_string(payload_map, "label"),
        _payload_int(payload_map, "count"),
    )


def _payload_mapping(payload: object) -> dict[str, object]:
    if not isinstance(payload, dict):
        raise TypeError("expected mapping payload")
    return cast(dict[str, object], payload)


def _payload_value(payload: dict[str, object], key: str) -> object:
    return payload[key]


def _payload_list(payload: dict[str, object], key: str) -> list[object]:
    value = _payload_value(payload, key)
    if not isinstance(value, list):
        raise TypeError(f"expected list for {key}")
    return cast(list[object], value)


def _payload_string(payload: dict[str, object], key: str) -> str:
    value = _payload_value(payload, key)
    if not isinstance(value, str):
        raise TypeError(f"expected string for {key}")
    return value


def _payload_string_list(payload: dict[str, object], key: str) -> list[str]:
    values = _payload_list(payload, key)
    return [_payload_string({"value": value}, "value") for value in values]


def _payload_int(payload: dict[str, object], key: str) -> int:
    value = _payload_value(payload, key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected int for {key}")
    return value


def _payload_int_list(payload: dict[str, object], key: str) -> list[int]:
    values = _payload_list(payload, key)
    return [_payload_int({"value": value}, "value") for value in values]


def _payload_optional_int(payload: dict[str, object], key: str) -> int | None:
    value = _payload_value(payload, key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"expected optional int for {key}")
    return value


def _payload_optional_float(payload: dict[str, object], key: str) -> float | None:
    value = _payload_value(payload, key)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"expected optional float for {key}")
    return float(value)


def _payload_bool(payload: dict[str, object], key: str) -> bool:
    value = _payload_value(payload, key)
    if not isinstance(value, bool):
        raise TypeError(f"expected bool for {key}")
    return value


def validate_runtime_config(config: RuntimeConfig) -> None:
    """Validate filesystem-backed application inputs before any run starts."""
    if not config.storage_directory.exists():
        raise ValidatorConfigurationError(
            f"storage directory does not exist: {config.storage_directory}"
        )
    if not config.storage_directory.is_dir():
        raise ValidatorConfigurationError(
            f"storage directory is not a directory: {config.storage_directory}"
        )
    validate_writable_directory_target(config.working_directory, "working directory")
    if config.max_days_back < 0:
        raise ValidatorConfigurationError("max_days_back must be zero or greater")
    _validate_runtime_storage_layout(config.storage_directory)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _build_report_directory(data_directory: Path, run_time: datetime) -> Path:
    report_root = data_directory / "reports"
    run_directory_name = run_time.strftime(RUN_DIRECTORY_FORMAT)
    try:
        report_root.mkdir(parents=True, exist_ok=True)
        candidate = report_root / run_directory_name
        suffix = 0
        while True:
            try:
                candidate.mkdir(exist_ok=False)
                return candidate
            except FileExistsError:
                suffix += 1
                candidate = report_root / f"{run_directory_name}-{suffix:02d}"
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to create report directory: {report_root}"
        ) from error


def _validate_runtime_storage_layout(storage_directory: Path) -> None:
    runtime_layout_message = (
        "run-daily requires top-level year=YYYY/month=MM/day=DD storage layout"
    )
    for year_directory in _directory_entries(storage_directory):
        if not year_directory.is_dir():
            raise ValidatorConfigurationError(
                f"{runtime_layout_message}: unsupported top-level entry {year_directory}"
            )
        if not year_directory.name.startswith("year="):
            raise ValidatorConfigurationError(
                f"{runtime_layout_message}: unsupported top-level directory {year_directory}"
            )
        year = _parse_number_component(year_directory.name, "year")
        if year is None:
            raise ValidatorConfigurationError(
                f"invalid runtime year directory: {year_directory}"
            )
        for month_directory in _directory_entries(year_directory):
            if not month_directory.is_dir():
                raise ValidatorConfigurationError(
                    f"invalid runtime month directory: {month_directory}"
                )
            if not month_directory.name.startswith("month="):
                raise ValidatorConfigurationError(
                    f"invalid runtime month directory: {month_directory}"
                )
            month = _parse_number_component(month_directory.name, "month")
            if month is None:
                raise ValidatorConfigurationError(
                    f"invalid runtime month directory: {month_directory}"
                )
            for day_directory in _directory_entries(month_directory):
                if not day_directory.is_dir():
                    raise ValidatorConfigurationError(
                        f"invalid runtime day directory: {day_directory}"
                    )
                if not day_directory.name.startswith("day="):
                    raise ValidatorConfigurationError(
                        f"invalid runtime day directory: {day_directory}"
                    )
                day_number = _parse_number_component(day_directory.name, "day")
                if day_number is None:
                    raise ValidatorConfigurationError(
                        f"invalid runtime day directory: {day_directory}"
                    )
                try:
                    date(year, month, day_number)
                except ValueError as error:
                    raise ValidatorConfigurationError(
                        f"invalid runtime day directory: {day_directory}"
                    ) from error


def _directory_entries(directory: Path) -> list[Path]:
    entries: list[Path] = []
    try:
        with os.scandir(directory) as scanned_entries:
            for entry in scanned_entries:
                entries.append(Path(entry.path))
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to scan directory: {directory}"
        ) from error
    entries.sort()
    return entries


def _subdirectories(directory: Path) -> list[Path]:
    child_directories: list[Path] = []
    try:
        with os.scandir(directory) as entries:
            for entry in entries:
                if entry.is_dir():
                    child_directories.append(Path(entry.path))
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to scan directory: {directory}"
        ) from error
    child_directories.sort()
    return child_directories


def _matching_subdirectories(directory: Path, prefix: str) -> list[Path]:
    return [
        child_directory
        for child_directory in _subdirectories(directory)
        if child_directory.name.startswith(prefix)
    ]


def _day_contains_parquet_files(partition: DayPartition) -> bool:
    def onerror(error: OSError) -> None:
        raise ValidatorConfigurationError(
            f"failed to scan day directory: {partition.path}"
        ) from error

    try:
        for _, _, file_names in os.walk(partition.path, onerror=onerror):
            if any(file_name.endswith(".parquet") for file_name in file_names):
                return True
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to scan day directory: {partition.path}"
        ) from error
    return False


def _select_pending_days(
    discovered_days: list[DayPartition],
    stored_watermark: date | None,
    oldest_allowed_day: date | None,
) -> list[DayPartition]:
    if oldest_allowed_day is None:
        return []
    return [
        partition
        for partition in discovered_days
        if partition.partition_date >= oldest_allowed_day
        and (stored_watermark is None or partition.partition_date > stored_watermark)
    ]


def _validate_stored_watermark(
    state_path: Path,
    stored_watermark: date | None,
    discovered_days: list[DayPartition],
) -> None:
    if stored_watermark is None or not discovered_days:
        return
    newest_discovered_day = discovered_days[-1].partition_date
    if stored_watermark > newest_discovered_day:
        raise ValidatorConfigurationError(
            "latest reviewed day is after newest discovered day: "
            f"{state_path} -> {stored_watermark.isoformat()} > {newest_discovered_day.isoformat()}"
        )


def _validate_committed_snapshots(
    baseline_snapshots_path: Path,
    stored_watermark: date | None,
    committed_snapshots: list[PartitionSnapshot],
) -> None:
    if stored_watermark is None:
        if committed_snapshots:
            raise ValidatorConfigurationError(
                "committed baseline snapshots exist without a latest reviewed day: "
                f"{baseline_snapshots_path}"
            )
        return
    for snapshot in committed_snapshots:
        if snapshot.partition.partition_date > stored_watermark:
            raise ValidatorConfigurationError(
                "committed baseline snapshot is after the latest reviewed day: "
                f"{baseline_snapshots_path} -> {snapshot.partition.label()} > "
                f"{stored_watermark.isoformat()}"
            )


def _select_next_reviewable_day(
    pending_days: list[DayPartition],
    stored_watermark: date | None,
    oldest_allowed_day: date | None,
) -> tuple[DayPartition | None, date | None]:
    """Return the next reviewable day and the first day currently blocking progress."""
    if not pending_days:
        return None, None
    expected_day = pending_days[0].partition_date
    if stored_watermark is not None:
        expected_day = stored_watermark + timedelta(days=1)
    if oldest_allowed_day is not None and expected_day < oldest_allowed_day:
        expected_day = oldest_allowed_day
    first_pending_day = pending_days[0]
    if first_pending_day.partition_date != expected_day:
        return None, expected_day
    if not _day_contains_parquet_files(first_pending_day):
        return None, expected_day
    return first_pending_day, None


def _comparison_snapshots_for_target_day(
    committed_snapshots: list[PartitionSnapshot],
    oldest_allowed_day: date | None,
    target_day: date,
) -> tuple[PartitionSnapshot, ...]:
    if oldest_allowed_day is None:
        return ()
    return tuple(
        snapshot
        for snapshot in committed_snapshots
        if oldest_allowed_day <= snapshot.partition.partition_date < target_day
    )


def _pruned_committed_snapshots(
    committed_snapshots: list[PartitionSnapshot],
    oldest_allowed_day: date | None,
    latest_reviewed_day: date | None,
) -> list[PartitionSnapshot]:
    if oldest_allowed_day is None or latest_reviewed_day is None:
        return []
    return sorted(
        (
            snapshot
            for snapshot in committed_snapshots
            if oldest_allowed_day
            <= snapshot.partition.partition_date
            <= latest_reviewed_day
        ),
        key=lambda snapshot: snapshot.partition,
    )


def _next_committed_snapshots(
    committed_snapshots: list[PartitionSnapshot],
    current_snapshots: list[PartitionSnapshot],
    oldest_allowed_day: date | None,
    latest_reviewed_day: date,
) -> list[PartitionSnapshot]:
    snapshot_by_label = {
        snapshot.partition.label(): snapshot
        for snapshot in _pruned_committed_snapshots(
            committed_snapshots,
            oldest_allowed_day,
            latest_reviewed_day,
        )
    }
    for snapshot in current_snapshots:
        snapshot_by_label[snapshot.partition.label()] = snapshot
    return sorted(snapshot_by_label.values(), key=lambda snapshot: snapshot.partition)


def _reviewed_partition_dates(reviewed_partitions: list[str]) -> set[date]:
    return {
        date.fromisoformat(partition_label[:10])
        for partition_label in reviewed_partitions
    }


def _issue_dates(findings: list[Finding], storage_directory: Path) -> set[date]:
    issue_dates: set[date] = set()
    for finding in findings:
        if finding.status == "metric":
            continue
        issue_date = _issue_date(finding, storage_directory)
        if issue_date is not None:
            issue_dates.add(issue_date)
    return issue_dates


def _issue_date(finding: Finding, storage_directory: Path) -> date | None:
    if finding.day is not None:
        return _parse_iso_day_prefix(finding.day)
    if finding.partition is not None:
        return _parse_iso_day_prefix(finding.partition)
    if finding.file is None:
        return None
    return _runtime_file_day(Path(finding.file), storage_directory)


def _parse_iso_day_prefix(value: str) -> date | None:
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _runtime_file_day(path: Path, storage_directory: Path) -> date | None:
    try:
        relative_parts = path.relative_to(storage_directory).parts
    except ValueError:
        return None
    for index in range(len(relative_parts) - 2):
        year = _parse_number_component(relative_parts[index], "year")
        month = _parse_number_component(relative_parts[index + 1], "month")
        day_number = _parse_number_component(relative_parts[index + 2], "day")
        if year is None or month is None or day_number is None:
            continue
        try:
            return date(year, month, day_number)
        except ValueError:
            return None
    return None


def _contiguous_reviewed_days(
    reviewable_days: list[DayPartition],
    reviewed_partition_dates: set[date],
    issue_dates: set[date],
) -> list[DayPartition]:
    reviewed_days: list[DayPartition] = []
    for partition in reviewable_days:
        if (
            partition.partition_date not in reviewed_partition_dates
            or partition.partition_date in issue_dates
        ):
            return reviewed_days
        reviewed_days.append(partition)
    return reviewed_days


def _blocked_day_after_validation(
    reviewable_days: list[DayPartition],
    reviewed_watermark_days: list[DayPartition],
    blocked_day: date | None,
) -> date | None:
    if blocked_day is not None:
        return blocked_day
    if len(reviewed_watermark_days) < len(reviewable_days):
        return reviewable_days[len(reviewed_watermark_days)].partition_date
    return None


def _oldest_allowed_day(
    discovered_days: list[DayPartition], max_days_back: int
) -> date | None:
    """Return the oldest day still inside the sliding review window."""
    if not discovered_days:
        return None
    newest_discovered_day = discovered_days[-1].partition_date
    return newest_discovered_day - timedelta(days=max_days_back)


def _should_store_latest_reviewed_day(
    stored_watermark: date | None,
    reviewed_days: list[DayPartition],
    oldest_allowed_day: date | None,
) -> bool:
    """Return whether this run can advance the persisted day watermark."""
    if not reviewed_days:
        return False
    if stored_watermark is not None:
        return True
    return (
        oldest_allowed_day is not None
        and reviewed_days[0].partition_date == oldest_allowed_day
    )


def _parse_number_component(name: str, prefix: str) -> int | None:
    if not name.startswith(f"{prefix}="):
        return None
    value = name[len(prefix) + 1 :]
    if not value.isdigit():
        return None
    return int(value)


def _write_report_state_files(
    report_directory: Path,
    persisted_watermark: date | None,
    persisted_snapshots: list[PartitionSnapshot],
) -> None:
    if persisted_watermark is None:
        return
    store_latest_reviewed_day(report_directory / STATE_FILE_NAME, persisted_watermark)
    _store_committed_baseline_snapshots(
        report_directory / BASELINE_SNAPSHOTS_FILE_NAME,
        persisted_snapshots,
    )


def _promote_latest_report(
    source_directory: Path,
    latest_directory: Path,
    state_path: Path,
    baseline_snapshots_path: Path,
    has_persisted_watermark: bool,
) -> None:
    try:
        latest_directory.mkdir(parents=True, exist_ok=True)
        _replace_symlink(
            latest_directory / SUMMARY_FILE_NAME,
            Path(LATEST_POINTER_NAME) / SUMMARY_FILE_NAME,
        )
        _replace_symlink(
            latest_directory / FINDINGS_FILE_NAME,
            Path(LATEST_POINTER_NAME) / FINDINGS_FILE_NAME,
        )
        if has_persisted_watermark:
            _replace_symlink(
                latest_directory / STATE_FILE_NAME,
                Path(LATEST_POINTER_NAME) / STATE_FILE_NAME,
            )
            _replace_symlink(
                latest_directory / BASELINE_SNAPSHOTS_FILE_NAME,
                Path(LATEST_POINTER_NAME) / BASELINE_SNAPSHOTS_FILE_NAME,
            )
            _replace_symlink(
                state_path,
                Path("reports") / latest_directory.name / STATE_FILE_NAME,
            )
            _replace_symlink(
                baseline_snapshots_path,
                Path("reports") / latest_directory.name / BASELINE_SNAPSHOTS_FILE_NAME,
            )
        else:
            _remove_path_if_present(latest_directory / STATE_FILE_NAME)
            _remove_path_if_present(latest_directory / BASELINE_SNAPSHOTS_FILE_NAME)
            _remove_path_if_present(state_path)
            _remove_path_if_present(baseline_snapshots_path)
        _replace_symlink(
            latest_directory / LATEST_POINTER_NAME,
            Path("..") / source_directory.name,
        )
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to refresh latest report directory: {latest_directory}"
        ) from error


def _replace_symlink(link_path: Path, target: Path) -> None:
    temporary_path = link_path.with_name(f".{link_path.name}.tmp")
    try:
        temporary_path.unlink()
    except FileNotFoundError:
        pass
    os.symlink(os.fspath(target), temporary_path)
    temporary_path.replace(link_path)


def _remove_path_if_present(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _runtime_status_text(
    status: Literal["idle", "blocked", "validated"], blocked_by_pending_day: bool
) -> str:
    if status == "idle":
        return "no new day partitions"
    if status == "validated":
        return "validated"
    if blocked_by_pending_day:
        return "blocked by an earlier pending day"
    return "blocked by a selected day that did not validate cleanly"


def _write_runtime_report(
    summary: RunSummary, status_text: str, write_empty_findings: bool
) -> None:
    report_directory = Path(summary.report_directory)
    payload = json.dumps(summary.to_payload(), sort_keys=True, indent=2)
    lines = [
        "# feed-base parquet validation",
        "",
        f"- status: `{status_text}`",
        f"- storage directory: `{summary.storage_directory}`",
        f"- working directory: `{summary.working_directory}`",
        f"- blocked day: `{summary.blocked_day or 'none'}`",
        f"- latest reviewed day: `{summary.latest_reviewed_day or 'none'}`",
        f"- discovered day directories: `{summary.discovered_day_count}`",
        f"- pending day directories: `{summary.pending_day_count}`",
        f"- reviewed day directories: `{summary.reviewed_day_count}`",
        f"- reviewed validator partitions: `{summary.reviewed_partition_count}`",
        f"- fail findings: `{summary.fail_count}`",
        f"- warn findings: `{summary.warn_count}`",
        f"- metric findings: `{summary.metric_count}`",
        "",
        "## Rule Counts",
    ]
    if not summary.rule_counts:
        lines.append("- no findings")
    else:
        for rule_id in sorted(summary.rule_counts):
            lines.append(f"- `{rule_id}`: `{summary.rule_counts[rule_id]}`")
    lines.extend(
        [
            "",
            "## Summary Payload",
            "",
            "```json",
            payload,
            "```",
            "",
        ]
    )
    summary_text = "\n".join(lines)
    try:
        (report_directory / SUMMARY_FILE_NAME).write_text(
            summary_text, encoding="utf-8"
        )
        if write_empty_findings:
            (report_directory / FINDINGS_FILE_NAME).write_text("", encoding="utf-8")
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to write runtime report: {report_directory}"
        ) from error
