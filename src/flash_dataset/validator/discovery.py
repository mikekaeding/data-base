"""Validator config, discovery, and path parsing helpers."""

from __future__ import annotations

from datetime import date
import os
from pathlib import Path
import re
import tempfile

import pyarrow.parquet as pq

from ..errors import ValidatorConfigurationError
from .common import BuildFileEntryOutcome
from .common import ColumnProfile
from .common import FileIndexEntry
from .common import Finding
from .common import ParsedStoragePath
from .common import PartitionIdentity
from .common import ValidationResult
from .common import ValidatorConfig
from .rules import make_finding

DATE_FILE_RE = re.compile(r"^(?P<dataset>.+)_(?P<day>\d{4}-\d{2}-\d{2})\.parquet$")
HOUR_FILE_RE = re.compile(
    r"^(?P<dataset>.+)_(?P<day>\d{4}-\d{2}-\d{2})_(?P<hour>\d{2})\.parquet$"
)
TEMPORARY_STORAGE_DIRECTORY_NAME = "tmp"


def validate_runtime_config(config: ValidatorConfig) -> None:
    """Validate filesystem-backed config inputs before discovery starts."""
    if not config.storage_root.exists():
        raise ValidatorConfigurationError(
            f"storage root does not exist: {config.storage_root}"
        )
    if not config.storage_root.is_dir():
        raise ValidatorConfigurationError(
            f"storage root is not a directory: {config.storage_root}"
        )
    validate_directory_target(config.output_directory, "output directory")
    if (
        config.date_from is not None
        and config.date_to is not None
        and config.date_from > config.date_to
    ):
        raise ValidatorConfigurationError("date_from must be on or before date_to")
    validate_writable_directory_target(config.output_directory, "output directory")


def discover_files(
    config: ValidatorConfig, result: ValidationResult
) -> list[FileIndexEntry]:
    """Discover readable parquet files under the configured storage root."""
    entries: list[FileIndexEntry] = []
    for path in scan_parquet_paths(
        config.storage_root,
        date_from=config.date_from,
        date_to=config.date_to,
    ):
        parsed = parse_storage_path(path, config.storage_root)
        if parsed is None:
            result.add(_partition_parse_error(path))
            continue
        if not _partition_matches_filters(config, parsed.partition):
            continue
        outcome = _build_file_entry_from_parsed(path, parsed)
        if outcome.warning is not None:
            result.add(outcome.warning)
        if outcome.entry is None:
            continue
        entries.append(outcome.entry)
        result.scanned_files += 1
        result.scanned_rows += outcome.entry.row_count
    return entries


def scan_parquet_paths(
    storage_root: Path,
    date_from: date | None = None,
    date_to: date | None = None,
) -> list[Path]:
    """Return sorted parquet paths under the storage root."""
    parquet_paths: list[Path] = []

    def onerror(error: OSError) -> None:
        raise ValidatorConfigurationError(
            f"failed to scan storage root: {storage_root}"
        ) from error

    for root_str, directory_names, file_names in os.walk(
        storage_root, topdown=True, onerror=onerror
    ):
        root_path = Path(root_str)
        _prune_temporary_storage_directory(
            storage_root,
            root_path,
            directory_names,
        )
        if date_from is not None or date_to is not None:
            _prune_directory_names(
                storage_root,
                root_path,
                directory_names,
                date_from,
                date_to,
            )
        for file_name in file_names:
            if file_name.endswith(".parquet"):
                parquet_paths.append(root_path / file_name)
    parquet_paths.sort()
    return parquet_paths


def _prune_temporary_storage_directory(
    storage_root: Path,
    root_path: Path,
    directory_names: list[str],
) -> None:
    if root_path != storage_root:
        return
    directory_names[:] = [
        name for name in directory_names if name != TEMPORARY_STORAGE_DIRECTORY_NAME
    ]


def _prune_directory_names(
    storage_root: Path,
    root_path: Path,
    directory_names: list[str],
    date_from: date | None,
    date_to: date | None,
) -> None:
    """Prune valid out-of-range subtrees while keeping malformed paths visible to discovery."""
    relative_parts = root_path.relative_to(storage_root).parts
    directory_names[:] = [
        name
        for name in directory_names
        if _should_descend_into_directory(relative_parts, name, date_from, date_to)
    ]


def _should_descend_into_directory(
    relative_parts: tuple[str, ...],
    child_name: str,
    date_from: date | None,
    date_to: date | None,
) -> bool:
    if _is_storage_root(relative_parts):
        if child_name.startswith("date="):
            partition_date = _parse_iso_date_component(child_name, "date")
            return partition_date is None or _date_is_in_range(
                partition_date, date_from, date_to
            )
        if child_name.startswith("year="):
            return _year_overlaps_range(
                _parse_number_component(child_name, "year"), date_from, date_to
            )
        return True
    if _is_dataset_root(relative_parts):
        if child_name.startswith("year="):
            return _year_overlaps_range(
                _parse_number_component(child_name, "year"), date_from, date_to
            )
        return True
    if _is_year_root(relative_parts):
        if not child_name.startswith("month="):
            return True
        year = _parse_number_component(relative_parts[-1], "year")
        return _month_overlaps_range(
            year,
            _parse_number_component(child_name, "month"),
            date_from,
            date_to,
        )
    if _is_month_root(relative_parts):
        if not child_name.startswith("day="):
            return True
        year = _parse_number_component(relative_parts[-2], "year")
        month = _parse_number_component(relative_parts[-1], "month")
        day = _parse_number_component(child_name, "day")
        if year is None or month is None or day is None:
            return True
        try:
            partition_date = date(year, month, day)
        except ValueError:
            return True
        return _date_is_in_range(partition_date, date_from, date_to)
    return True


def _is_storage_root(relative_parts: tuple[str, ...]) -> bool:
    return len(relative_parts) == 0


def _is_dataset_root(relative_parts: tuple[str, ...]) -> bool:
    return len(relative_parts) == 1 and not relative_parts[0].startswith(
        ("date=", "year=")
    )


def _is_year_root(relative_parts: tuple[str, ...]) -> bool:
    return len(relative_parts) >= 1 and relative_parts[-1].startswith("year=")


def _is_month_root(relative_parts: tuple[str, ...]) -> bool:
    return (
        len(relative_parts) >= 2
        and relative_parts[-2].startswith("year=")
        and relative_parts[-1].startswith("month=")
    )


def _date_is_in_range(
    partition_date: date, date_from: date | None, date_to: date | None
) -> bool:
    if date_from is not None and partition_date < date_from:
        return False
    if date_to is not None and partition_date > date_to:
        return False
    return True


def _year_overlaps_range(
    year: int | None, date_from: date | None, date_to: date | None
) -> bool:
    if year is None:
        return True
    try:
        year_start = date(year, 1, 1)
        year_end = date(year, 12, 31)
    except ValueError:
        return True
    return _date_range_overlaps(year_start, year_end, date_from, date_to)


def _month_overlaps_range(
    year: int | None,
    month: int | None,
    date_from: date | None,
    date_to: date | None,
) -> bool:
    if year is None or month is None:
        return True
    try:
        month_start = date(year, month, 1)
        if month == 12:
            month_end = date(year, 12, 31)
        else:
            month_end = date(year, month + 1, 1) - date.resolution
    except ValueError:
        return True
    return _date_range_overlaps(month_start, month_end, date_from, date_to)


def _date_range_overlaps(
    range_start: date,
    range_end: date,
    date_from: date | None,
    date_to: date | None,
) -> bool:
    if date_from is not None and range_end < date_from:
        return False
    if date_to is not None and range_start > date_to:
        return False
    return True


def build_file_entry(path: Path, storage_root: Path) -> BuildFileEntryOutcome:
    """Parse one parquet file path and load footer metadata when possible."""
    parsed = parse_storage_path(path, storage_root)
    if parsed is None:
        return BuildFileEntryOutcome(
            partition=None,
            entry=None,
            warning=_partition_parse_error(path),
        )
    return _build_file_entry_from_parsed(path, parsed)


def _build_file_entry_from_parsed(
    path: Path, parsed: ParsedStoragePath
) -> BuildFileEntryOutcome:
    """Load footer metadata for one parquet file whose path was already parsed."""
    try:
        parquet_file = pq.ParquetFile(path)
    except Exception:
        return BuildFileEntryOutcome(
            partition=parsed.partition,
            entry=None,
            warning=make_finding(
                "invalid_parquet_file",
                "failed to read parquet footer",
                day=parsed.partition,
                dataset=parsed.dataset,
                file=str(path),
            ),
        )
    schema = tuple(
        ColumnProfile.from_field(field) for field in parquet_file.schema_arrow
    )
    metadata = parquet_file.metadata
    file_size_bytes = int(path.stat().st_size)
    compressed_bytes = 0
    uncompressed_bytes = 0
    for row_group_index in range(metadata.num_row_groups):
        row_group = metadata.row_group(row_group_index)
        for column_index in range(row_group.num_columns):
            column = row_group.column(column_index)
            compressed_bytes += int(column.total_compressed_size)
            uncompressed_bytes += int(column.total_uncompressed_size)
    entry = FileIndexEntry(
        path=path,
        dataset=parsed.dataset,
        partition=parsed.partition,
        layout=parsed.layout,
        row_count=int(metadata.num_rows),
        file_size_bytes=file_size_bytes,
        row_groups=int(metadata.num_row_groups),
        compressed_bytes=compressed_bytes,
        uncompressed_bytes=uncompressed_bytes,
        schema=schema,
    )
    return BuildFileEntryOutcome(
        partition=parsed.partition,
        entry=entry,
        warning=parsed.warning,
    )


def _partition_parse_error(path: Path) -> Finding:
    """Build one parse warning for an unsupported parquet path."""
    return make_finding(
        "partition_parse_error",
        "parquet path does not match a supported feed-base storage layout",
        file=str(path),
    )


def _partition_matches_filters(
    config: ValidatorConfig, partition: PartitionIdentity
) -> bool:
    if config.date_from is not None and partition.partition_date < config.date_from:
        return False
    if config.date_to is not None and partition.partition_date > config.date_to:
        return False
    return True


def parse_storage_path(path: Path, storage_root: Path) -> ParsedStoragePath | None:
    """Extract logical dataset metadata from either supported storage layout."""
    try:
        relative_path = path.relative_to(storage_root)
    except ValueError:
        return None
    parts = relative_path.parts
    if len(parts) == 2 and parts[0].startswith("date="):
        return _parse_date_partition_layout(path, parts)
    if len(parts) == 5 and parts[0].startswith("year="):
        return _parse_hour_partition_layout(path, parts)
    if len(parts) == 5 and parts[1].startswith("year="):
        return _parse_table_partition_layout(path, parts)
    return None


def validate_directory_target(path: Path, label: str) -> None:
    """Validate that a directory target can exist beneath a directory parent chain."""
    if path.exists() and not path.is_dir():
        raise ValidatorConfigurationError(f"{label} is not a directory: {path}")
    validate_parent_chain(path, label)


def validate_writable_directory_target(path: Path, label: str) -> None:
    """Validate that one directory target can be created and written."""
    validate_directory_target(path, label)
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise ValidatorConfigurationError(f"{label} is not writable: {path}") from error
    try:
        with tempfile.NamedTemporaryFile(dir=path):
            pass
    except OSError as error:
        raise ValidatorConfigurationError(f"{label} is not writable: {path}") from error


def validate_parent_chain(path: Path, label: str) -> None:
    """Validate that the nearest existing ancestor is a directory."""
    for candidate in (path, *path.parents):
        if not candidate.exists():
            continue
        if not candidate.is_dir():
            raise ValidatorConfigurationError(
                f"{label} parent is not a directory: {candidate}"
            )
        return


def _parse_iso_date_component(name: str, prefix: str) -> date | None:
    if not name.startswith(f"{prefix}="):
        return None
    try:
        return date.fromisoformat(name[len(prefix) + 1 :])
    except ValueError:
        return None


def _parse_number_component(name: str, prefix: str) -> int | None:
    if not name.startswith(f"{prefix}="):
        return None
    value = name[len(prefix) + 1 :]
    if not value.isdigit():
        return None
    return int(value)


def _parse_date_partition_layout(
    path: Path, parts: tuple[str, ...]
) -> ParsedStoragePath | None:
    partition_text = parts[0][len("date=") :]
    try:
        partition_date = date.fromisoformat(partition_text)
    except ValueError:
        return None
    match = DATE_FILE_RE.match(parts[1])
    if match is None:
        return None
    try:
        filename_date = date.fromisoformat(match.group("day"))
    except ValueError:
        return None
    dataset = match.group("dataset")
    warning = None
    if filename_date != partition_date:
        warning = make_finding(
            "partition_date_mismatch",
            "filename day does not match partition day",
            day=PartitionIdentity(partition_date),
            dataset=dataset,
            file=str(path),
            example={"filename_day": filename_date.isoformat()},
        )
    return ParsedStoragePath(
        dataset=dataset,
        partition=PartitionIdentity(partition_date),
        layout="date_partition",
        warning=warning,
    )


def _parse_table_partition_layout(
    path: Path, parts: tuple[str, ...]
) -> ParsedStoragePath | None:
    dataset = parts[0]
    try:
        year = int(parts[1][len("year=") :])
        month = int(parts[2][len("month=") :])
        day_number = int(parts[3][len("day=") :])
        partition_date = date(year, month, day_number)
    except ValueError:
        return None
    match = DATE_FILE_RE.match(parts[-1])
    if match is None:
        return None
    if match.group("dataset") != dataset:
        return None
    try:
        filename_date = date.fromisoformat(match.group("day"))
    except ValueError:
        return None
    warning = None
    if filename_date != partition_date:
        warning = make_finding(
            "partition_date_mismatch",
            "filename day does not match partition day",
            day=PartitionIdentity(partition_date),
            dataset=dataset,
            file=str(path),
            example={"filename_day": filename_date.isoformat()},
        )
    return ParsedStoragePath(
        dataset=dataset,
        partition=PartitionIdentity(partition_date),
        layout="table_partition",
        warning=warning,
    )


def _parse_hour_partition_layout(
    path: Path, parts: tuple[str, ...]
) -> ParsedStoragePath | None:
    try:
        year = int(parts[0][len("year=") :])
        month = int(parts[1][len("month=") :])
        day_number = int(parts[2][len("day=") :])
        partition_hour = int(parts[3][len("hour=") :])
        partition_date = date(year, month, day_number)
    except ValueError:
        return None
    if partition_hour < 0 or partition_hour > 23:
        return None
    match = HOUR_FILE_RE.match(parts[4])
    if match is None:
        return None
    dataset = match.group("dataset")
    try:
        filename_date = date.fromisoformat(match.group("day"))
    except ValueError:
        return None
    filename_hour = int(match.group("hour"))
    warning = None
    if filename_date != partition_date or filename_hour != partition_hour:
        warning = make_finding(
            "partition_date_mismatch",
            "filename partition does not match directory partition",
            day=PartitionIdentity(partition_date, partition_hour),
            dataset=dataset,
            file=str(path),
            example={
                "filename_day": filename_date.isoformat(),
                "filename_hour": filename_hour,
                "partition_hour": partition_hour,
            },
        )
    return ParsedStoragePath(
        dataset=dataset,
        partition=PartitionIdentity(partition_date, partition_hour),
        layout="hour_partition",
        warning=warning,
    )
