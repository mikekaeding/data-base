"""Validation orchestration for feed-base parquet storage."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from collections.abc import Sequence
from datetime import date

import pyarrow as pa
import pyarrow.dataset as ds

from . import integrity
from . import semantics
from .baselines import collect_partition_snapshot
from .baselines import PartitionSnapshot
from .baselines import run_baseline_checks
from .common import FileIndexEntry
from .common import PartitionIdentity
from .common import ValidationResult
from .common import ValidatorConfig
from .discovery import discover_files
from .discovery import validate_runtime_config
from .profiles import KNOWN_DATASETS
from .profiles import REQUIRED_DATASETS
from .profiles import table_profile_for
from .reporting import write_reports
from .rules import make_finding


def run_validation(
    config: ValidatorConfig,
    progress_callback: Callable[[str], None] | None = None,
    comparison_snapshots: Sequence[PartitionSnapshot] = (),
) -> ValidationResult:
    """Execute one bounded parquet validation flow and return aggregated findings."""
    validate_runtime_config(config)
    result = ValidationResult()
    _progress("discovering parquet files", progress_callback)
    entries = discover_files(config, result)
    if not entries:
        if result.issue_count() == 0:
            result.add(
                make_finding(
                    "no_matching_parquet_files",
                    "no parquet files matched the configured storage root and date filters",
                )
            )
        _progress("writing reports", progress_callback)
        write_reports(config, result)
        return result
    partition_groups = _group_entries_by_partition(entries)
    sorted_partitions = sorted(partition_groups, key=_partition_sort_key)
    snapshots: list[PartitionSnapshot] = []
    for partition in sorted_partitions:
        _progress(f"validating {partition.label()}", progress_callback)
        snapshot = _validate_partition(
            config, partition, partition_groups[partition], result
        )
        if snapshot is not None:
            result.reviewed_partitions.append(partition.label())
            result.partition_snapshots.append(snapshot)
            snapshots.append(snapshot)
    run_baseline_checks(
        snapshots,
        result,
        comparison_snapshots=comparison_snapshots,
    )
    _progress("writing reports", progress_callback)
    write_reports(config, result)
    return result


def _validate_partition(
    config: ValidatorConfig,
    partition: PartitionIdentity,
    partition_entries: dict[str, list[FileIndexEntry]],
    result: ValidationResult,
) -> PartitionSnapshot | None:
    layouts = {
        entry.layout for entries in partition_entries.values() for entry in entries
    }
    if len(layouts) > 1:
        result.add(
            make_finding(
                "mixed_storage_layout_for_partition",
                "partition mixes multiple storage layouts",
                day=partition,
                count=len(layouts),
            )
        )

    missing_datasets = sorted(set(REQUIRED_DATASETS).difference(partition_entries))
    for dataset in missing_datasets:
        result.add(
            make_finding(
                "missing_required_dataset",
                "required dataset is missing for the partition",
                day=partition,
                dataset=dataset,
            )
        )

    unknown_datasets = sorted(set(partition_entries).difference(KNOWN_DATASETS))
    for dataset in unknown_datasets:
        result.add(
            make_finding(
                "unknown_dataset",
                "storage contains an unknown dataset for the partition",
                day=partition,
                dataset=dataset,
            )
        )

    file_cardinality_failures = 0
    schema_failures = 0
    for dataset, entries in sorted(partition_entries.items()):
        if len(entries) > 1:
            file_cardinality_failures += 1
            result.add(
                make_finding(
                    "multiple_readable_files_for_dataset_partition",
                    "partition contains multiple readable parquet files for one dataset",
                    day=partition,
                    dataset=dataset,
                    count=len(entries),
                )
            )
        profile = table_profile_for(dataset)
        if profile is None:
            continue
        for entry in entries:
            if entry.schema != profile.columns:
                schema_failures += 1
                result.add(
                    make_finding(
                        "schema_mismatch",
                        "parquet footer schema does not match the expected writer contract",
                        day=partition,
                        dataset=dataset,
                        file=str(entry.path),
                    )
                )
    if missing_datasets or file_cardinality_failures > 0 or schema_failures > 0:
        return None

    cache = _PartitionTableCache(partition_entries)
    integrity.run_integrity_checks(partition, cache.read, result)
    semantics.run_semantic_checks(
        partition,
        config.verify_transaction_hashes,
        cache.read,
        result,
    )
    return collect_partition_snapshot(
        partition,
        {
            dataset: entries[0]
            for dataset, entries in partition_entries.items()
            if dataset in KNOWN_DATASETS
        },
        cache.read,
        peer_eligible=_partition_issue_count(result, partition) == 0,
    )


def _group_entries_by_partition(
    entries: list[FileIndexEntry],
) -> dict[PartitionIdentity, dict[str, list[FileIndexEntry]]]:
    grouped: dict[PartitionIdentity, dict[str, list[FileIndexEntry]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for entry in entries:
        grouped[entry.partition][entry.dataset].append(entry)
    return {
        partition: {
            dataset: sorted(dataset_entries, key=lambda item: str(item.path))
            for dataset, dataset_entries in datasets.items()
        }
        for partition, datasets in grouped.items()
    }


def _progress(message: str, progress_callback: Callable[[str], None] | None) -> None:
    if progress_callback is not None:
        progress_callback(message)


def _partition_issue_count(
    result: ValidationResult, partition: PartitionIdentity
) -> int:
    partition_label = partition.label()
    return sum(
        1
        for finding in result.findings
        if finding.status != "metric" and finding.partition == partition_label
    )


def _partition_sort_key(partition: PartitionIdentity) -> tuple[date, int]:
    hour = -1 if partition.partition_hour is None else partition.partition_hour
    return (partition.partition_date, hour)


class _PartitionTableCache:
    """Caches column-pruned Arrow tables per dataset within one partition."""

    def __init__(self, entries_by_dataset: dict[str, list[FileIndexEntry]]) -> None:
        self._entries_by_dataset = entries_by_dataset
        self._tables: dict[tuple[str, tuple[str, ...]], pa.Table] = {}

    def read(self, dataset: str, columns: tuple[str, ...]) -> pa.Table:
        """Return one table for the requested dataset and selected columns."""
        cache_key = (dataset, columns)
        if cache_key not in self._tables:
            entry = self._entries_by_dataset[dataset][0]
            self._tables[cache_key] = ds.dataset(
                str(entry.path), format="parquet"
            ).to_table(columns=list(columns))
        return self._tables[cache_key]
