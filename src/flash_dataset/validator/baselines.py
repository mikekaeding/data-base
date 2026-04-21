"""Cross-partition baseline metrics and anomaly checks."""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from statistics import median
from typing import Any

from .common import FileIndexEntry
from .common import PartitionIdentity
from .common import ValidationResult
from .common import iter_table_rows
from .rules import make_finding

BASELINE_COMPARISON_PARTITIONS = 3
SHORT_PAYLOAD_MIN_PARTITION_SIZE = 4
SHORT_PAYLOAD_MIN_MEDIAN_LENGTH = 3
TAIL_ONLY_WARN_COUNT = 3
TAIL_ONLY_WARN_RATIO = 0.25
TRANSACTION_TYPE_DRIFT_THRESHOLD = 0.35
PHYSICAL_DRIFT_RATIO_THRESHOLDS: dict[str, float] = {
    "row_count": 1.5,
    "file_size_bytes": 1.5,
    "row_groups": 2.0,
    "compression_ratio": 1.25,
}
PHYSICAL_DRIFT_ABSOLUTE_THRESHOLDS: dict[str, float] = {
    "row_count": 1.0,
    "file_size_bytes": 1.0,
    "row_groups": 2.0,
    "compression_ratio": 0.1,
}
TRANSACTION_TYPE_LABELS: tuple[str, ...] = (
    "legacy",
    "eip2930",
    "eip1559",
    "eip7702",
    "deposit",
)


@dataclass(frozen=True)
class DistributionSummary:
    """Compact distribution summary for one integer series.

    Args:
        count: Number of sampled values.
        minimum: Smallest sampled value.
        median_value: Median sampled value.
        maximum: Largest sampled value.
    """

    count: int
    minimum: int | None
    median_value: float | None
    maximum: int | None


@dataclass(frozen=True)
class DatasetPhysicalSnapshot:
    """Physical parquet metadata captured for one dataset within one partition.

    Args:
        dataset: Logical feed-base dataset name.
        row_count: Physical parquet row count.
        file_size_bytes: On-disk parquet file size in bytes.
        row_groups: Physical parquet row-group count.
        compressed_bytes: Sum of parquet column-chunk compressed sizes.
        uncompressed_bytes: Sum of parquet column-chunk uncompressed sizes.
    """

    dataset: str
    row_count: int
    file_size_bytes: int
    row_groups: int
    compressed_bytes: int
    uncompressed_bytes: int

    def compression_ratio(self) -> float | None:
        """Return the physical parquet compression ratio when it is defined."""
        if self.compressed_bytes <= 0 or self.uncompressed_bytes <= 0:
            return None
        return self.uncompressed_bytes / self.compressed_bytes


@dataclass(frozen=True)
class PartitionSnapshot:
    """Logical and physical baseline snapshot for one validated partition.

    Args:
        partition: Partition represented by the snapshot.
        dataset_snapshots: Physical parquet metadata for the readable datasets in this partition.
        empty_datasets: Datasets with zero physical rows in this partition.
        payload_lengths: Contiguous payload interval lengths within the partition.
        payload_length_summary: Summary stats for payload interval lengths.
        tail_only_payload_count: Number of payloads whose minimum index exceeds zero.
        transaction_count_summary: Summary stats for flashblock transaction counts.
        receipt_log_count_summary: Summary stats for receipt log counts.
        balance_change_count_summary: Summary stats for flashblock balance-change counts.
        withdrawal_count_summary: Summary stats for flashblock withdrawal counts.
        transaction_type_counts: Sorted `(type_label, count)` pairs for transactions.
        total_transactions: Total transaction rows in the partition.
        peer_eligible: Whether the partition is clean enough to act as a drift peer.
    """

    partition: PartitionIdentity
    dataset_snapshots: tuple[DatasetPhysicalSnapshot, ...]
    empty_datasets: tuple[str, ...]
    payload_lengths: tuple[int, ...]
    payload_length_summary: DistributionSummary
    tail_only_payload_count: int
    transaction_count_summary: DistributionSummary
    receipt_log_count_summary: DistributionSummary
    balance_change_count_summary: DistributionSummary
    withdrawal_count_summary: DistributionSummary
    transaction_type_counts: tuple[tuple[str, int], ...]
    total_transactions: int
    peer_eligible: bool

    def dataset_snapshot_for(self, dataset: str) -> DatasetPhysicalSnapshot | None:
        """Return the physical parquet metadata for one dataset when present."""
        for dataset_snapshot in self.dataset_snapshots:
            if dataset_snapshot.dataset == dataset:
                return dataset_snapshot
        return None

    def transaction_type_share(self, transaction_type_label: str) -> float:
        """Return one transaction-type share within the partition."""
        if self.total_transactions == 0:
            return 0.0
        for label, count in self.transaction_type_counts:
            if label == transaction_type_label:
                return count / self.total_transactions
        return 0.0


def collect_partition_snapshot(
    partition: PartitionIdentity,
    entries_by_dataset: dict[str, FileIndexEntry],
    read_table: Callable[[str, tuple[str, ...]], Any],
    peer_eligible: bool,
) -> PartitionSnapshot:
    """Collect physical and logical baseline stats for one readable partition."""
    flashblocks = read_table(
        "flashblocks",
        (
            "payload_id",
            "index",
            "transaction_count",
            "balance_change_count",
            "withdrawal_count",
        ),
    )
    payload_ranges = flashblocks.group_by(["payload_id"]).aggregate(
        [("index", "min"), ("index", "max"), ("index", "count")]
    )
    payload_lengths = _payload_interval_lengths(payload_ranges)
    tail_only_payload_count = sum(
        1
        for minimum_index in _column_int_values(payload_ranges["index_min"])
        if minimum_index > 0
    )

    receipts = read_table("flashblock_receipts", ("log_count",))
    transactions = read_table("flashblock_transactions", ("transaction_type_label",))
    transaction_type_counter: Counter[str] = Counter()
    for row in iter_table_rows(transactions):
        label = row["transaction_type_label"]
        if label is not None:
            transaction_type_counter[str(label)] += 1

    return PartitionSnapshot(
        partition=partition,
        dataset_snapshots=tuple(
            sorted(
                (
                    DatasetPhysicalSnapshot(
                        dataset=entry.dataset,
                        row_count=entry.row_count,
                        file_size_bytes=entry.file_size_bytes,
                        row_groups=entry.row_groups,
                        compressed_bytes=entry.compressed_bytes,
                        uncompressed_bytes=entry.uncompressed_bytes,
                    )
                    for entry in entries_by_dataset.values()
                ),
                key=lambda dataset_snapshot: dataset_snapshot.dataset,
            )
        ),
        empty_datasets=tuple(
            sorted(
                dataset
                for dataset, entry in entries_by_dataset.items()
                if entry.row_count == 0
            )
        ),
        payload_lengths=payload_lengths,
        payload_length_summary=_summarize_values(payload_lengths),
        tail_only_payload_count=tail_only_payload_count,
        transaction_count_summary=_summarize_values(
            _column_int_values(flashblocks["transaction_count"])
        ),
        receipt_log_count_summary=_summarize_values(
            _column_int_values(receipts["log_count"])
        ),
        balance_change_count_summary=_summarize_values(
            _column_int_values(flashblocks["balance_change_count"])
        ),
        withdrawal_count_summary=_summarize_values(
            _column_int_values(flashblocks["withdrawal_count"])
        ),
        transaction_type_counts=tuple(sorted(transaction_type_counter.items())),
        total_transactions=sum(transaction_type_counter.values()),
        peer_eligible=peer_eligible,
    )


def run_baseline_checks(
    snapshots: Sequence[PartitionSnapshot],
    result: ValidationResult,
    comparison_snapshots: Sequence[PartitionSnapshot] = (),
) -> None:
    """Emit baseline metrics and anomaly warnings for the current validation run."""
    all_snapshots = (*comparison_snapshots, *snapshots)
    for snapshot in sorted(snapshots, key=lambda item: item.partition):
        _add_partition_metrics(snapshot, result)
        _add_short_payload_warning(snapshot, result)
        _add_excess_tail_only_warning(snapshot, result)
        if not _is_drift_peer(snapshot):
            continue
        _add_dataset_physical_drift_warnings(snapshot, all_snapshots, result)
        _add_transaction_type_mix_drift_warning(snapshot, all_snapshots, result)


def _add_partition_metrics(
    snapshot: PartitionSnapshot, result: ValidationResult
) -> None:
    for dataset_snapshot in snapshot.dataset_snapshots:
        result.add(
            make_finding(
                "dataset_physical_stats",
                "dataset physical parquet stats recorded for baseline tracking",
                day=snapshot.partition,
                dataset=dataset_snapshot.dataset,
                count=dataset_snapshot.row_count,
                example={
                    "row_count": dataset_snapshot.row_count,
                    "file_size_bytes": dataset_snapshot.file_size_bytes,
                    "row_groups": dataset_snapshot.row_groups,
                    "compressed_bytes": dataset_snapshot.compressed_bytes,
                    "uncompressed_bytes": dataset_snapshot.uncompressed_bytes,
                    "compression_ratio": _round_float(
                        dataset_snapshot.compression_ratio()
                    ),
                },
            )
        )

    result.add(
        make_finding(
            "payload_run_length_distribution",
            "payload interval length distribution recorded for the partition",
            day=snapshot.partition,
            dataset="flashblocks",
            count=snapshot.payload_length_summary.count,
            example=_distribution_example(
                "payload_length", snapshot.payload_length_summary
            ),
        )
    )
    result.add(
        make_finding(
            "flashblock_child_count_distribution",
            "flashblock child-count distributions recorded for the partition",
            day=snapshot.partition,
            dataset="flashblocks",
            count=snapshot.transaction_count_summary.count,
            example={
                **_distribution_example(
                    "transaction_count", snapshot.transaction_count_summary
                ),
                **_distribution_example(
                    "log_count", snapshot.receipt_log_count_summary
                ),
                **_distribution_example(
                    "balance_change_count", snapshot.balance_change_count_summary
                ),
                **_distribution_example(
                    "withdrawal_count", snapshot.withdrawal_count_summary
                ),
            },
        )
    )
    result.add(
        make_finding(
            "transaction_type_mix",
            "transaction-type mix recorded for the partition",
            day=snapshot.partition,
            dataset="flashblock_transactions",
            count=snapshot.total_transactions,
            example=_transaction_type_mix_example(snapshot),
        )
    )
    result.add(
        make_finding(
            "empty_dataset_count",
            f"empty dataset count is {len(snapshot.empty_datasets)}",
            day=snapshot.partition,
            count=len(snapshot.empty_datasets),
            example={
                "datasets": (
                    ",".join(snapshot.empty_datasets)
                    if snapshot.empty_datasets
                    else "none"
                )
            },
        )
    )


def _add_short_payload_warning(
    snapshot: PartitionSnapshot, result: ValidationResult
) -> None:
    warning_details = _short_payload_warning_details(snapshot)
    if warning_details is None:
        return
    short_payload_count, warning_example = warning_details
    result.add(
        make_finding(
            "short_payload_run_length",
            "partition contains payload intervals that are much shorter than the local norm",
            day=snapshot.partition,
            dataset="flashblocks",
            count=short_payload_count,
            example=warning_example,
        )
    )


def _add_excess_tail_only_warning(
    snapshot: PartitionSnapshot, result: ValidationResult
) -> None:
    warning_example = _excess_tail_only_warning_example(snapshot)
    if warning_example is None:
        return
    result.add(
        make_finding(
            "excess_tail_only_payloads",
            "partition contains an unusually large number of tail-only payloads",
            day=snapshot.partition,
            dataset="flashblocks",
            count=snapshot.tail_only_payload_count,
            example=warning_example,
        )
    )


def _add_dataset_physical_drift_warnings(
    snapshot: PartitionSnapshot,
    snapshots: Sequence[PartitionSnapshot],
    result: ValidationResult,
) -> None:
    peer_snapshots, comparison_scope = _comparison_snapshots(snapshot, snapshots)
    if not peer_snapshots:
        return
    for dataset_snapshot in snapshot.dataset_snapshots:
        drifted_metrics: list[tuple[str, float, float, float]] = []
        for metric_name, metric_value in _physical_metrics(dataset_snapshot).items():
            if metric_value is None:
                continue
            peer_values = [
                peer_metric
                for peer in peer_snapshots
                for peer_metric in [
                    _physical_metric_value(peer, dataset_snapshot.dataset, metric_name)
                ]
                if peer_metric is not None
            ]
            if len(peer_values) < BASELINE_COMPARISON_PARTITIONS:
                continue
            peer_median = float(median(peer_values))
            if not _is_physical_drift(metric_name, metric_value, peer_median):
                continue
            drift_ratio = _drift_ratio(metric_value, peer_median)
            drifted_metrics.append(
                (metric_name, float(metric_value), peer_median, drift_ratio)
            )
        if not drifted_metrics:
            continue
        dominant_metric = max(drifted_metrics, key=lambda item: item[3])
        result.add(
            make_finding(
                "dataset_physical_drift",
                "dataset physical stats drift from the comparison cohort",
                day=snapshot.partition,
                dataset=dataset_snapshot.dataset,
                count=len(drifted_metrics),
                example={
                    "metrics": ",".join(item[0] for item in drifted_metrics),
                    "metric": dominant_metric[0],
                    "value": _round_float(dominant_metric[1]),
                    "peer_median": _round_float(dominant_metric[2]),
                    "drift_ratio": _round_float(dominant_metric[3]),
                    "peer_count": len(peer_snapshots),
                    "comparison_scope": comparison_scope,
                },
            )
        )


def _add_transaction_type_mix_drift_warning(
    snapshot: PartitionSnapshot,
    snapshots: Sequence[PartitionSnapshot],
    result: ValidationResult,
) -> None:
    peer_snapshots, comparison_scope = _comparison_snapshots(snapshot, snapshots)
    if not peer_snapshots or snapshot.total_transactions == 0:
        return

    drifted_types: list[tuple[str, float, float]] = []
    labels = sorted(
        {
            *TRANSACTION_TYPE_LABELS,
            *(label for label, _ in snapshot.transaction_type_counts),
            *(
                label
                for peer in peer_snapshots
                for label, _ in peer.transaction_type_counts
            ),
        }
    )
    for label in labels:
        peer_shares = [
            peer.transaction_type_share(label)
            for peer in peer_snapshots
            if peer.total_transactions > 0
        ]
        if len(peer_shares) < BASELINE_COMPARISON_PARTITIONS:
            continue
        share = snapshot.transaction_type_share(label)
        peer_median = float(median(peer_shares))
        difference = abs(share - peer_median)
        if difference >= TRANSACTION_TYPE_DRIFT_THRESHOLD:
            drifted_types.append((label, share, peer_median))
    if not drifted_types:
        return

    dominant_type = max(drifted_types, key=lambda item: abs(item[1] - item[2]))
    result.add(
        make_finding(
            "transaction_type_mix_drift",
            "transaction-type mix drifts from comparable partitions",
            day=snapshot.partition,
            dataset="flashblock_transactions",
            count=len(drifted_types),
            example={
                "type_label": dominant_type[0],
                "share": _round_float(dominant_type[1]),
                "peer_median_share": _round_float(dominant_type[2]),
                "difference": _round_float(abs(dominant_type[1] - dominant_type[2])),
                "peer_count": len(peer_snapshots),
                "comparison_scope": comparison_scope,
            },
        )
    )


def _comparison_snapshots(
    snapshot: PartitionSnapshot, snapshots: Sequence[PartitionSnapshot]
) -> tuple[tuple[PartitionSnapshot, ...], str]:
    same_hour_snapshots = tuple(
        peer
        for peer in snapshots
        if peer.partition != snapshot.partition
        and _is_drift_peer(peer)
        and peer.partition.partition_hour == snapshot.partition.partition_hour
    )
    if len(same_hour_snapshots) >= BASELINE_COMPARISON_PARTITIONS:
        return (same_hour_snapshots, "same_hour")
    return ((), "insufficient_same_hour_peers")


def _column_int_values(column: Any) -> tuple[int, ...]:
    return tuple(int(value) for value in column.to_pylist() if value is not None)


def _is_drift_peer(snapshot: PartitionSnapshot) -> bool:
    return (
        snapshot.peer_eligible
        and _short_payload_warning_details(snapshot) is None
        and _excess_tail_only_warning_example(snapshot) is None
    )


def _short_payload_warning_details(
    snapshot: PartitionSnapshot,
) -> tuple[int, dict[str, str | int | float | bool | None]] | None:
    summary = snapshot.payload_length_summary
    if (
        summary.count < SHORT_PAYLOAD_MIN_PARTITION_SIZE
        or summary.median_value is None
        or summary.median_value < SHORT_PAYLOAD_MIN_MEDIAN_LENGTH
    ):
        return None
    threshold_length = max(1, int(summary.median_value // 2))
    short_payload_count = sum(
        1
        for payload_length in snapshot.payload_lengths
        if payload_length <= threshold_length
    )
    if short_payload_count == 0 or short_payload_count == summary.count:
        return None
    return (
        short_payload_count,
        {
            "short_payload_count": short_payload_count,
            "median_length": _round_float(summary.median_value),
            "threshold_length": threshold_length,
        },
    )


def _excess_tail_only_warning_example(
    snapshot: PartitionSnapshot,
) -> dict[str, str | int | float | bool | None] | None:
    payload_count = snapshot.payload_length_summary.count
    if payload_count == 0:
        return None
    tail_only_ratio = snapshot.tail_only_payload_count / payload_count
    if (
        snapshot.tail_only_payload_count < TAIL_ONLY_WARN_COUNT
        or tail_only_ratio < TAIL_ONLY_WARN_RATIO
    ):
        return None
    return {
        "payload_count": payload_count,
        "tail_only_ratio": _round_float(tail_only_ratio),
    }


def _payload_interval_lengths(payload_ranges: Any) -> tuple[int, ...]:
    minimum_indexes = _column_int_values(payload_ranges["index_min"])
    maximum_indexes = _column_int_values(payload_ranges["index_max"])
    return tuple(
        sorted(
            (maximum_index - minimum_index) + 1
            for minimum_index, maximum_index in zip(
                minimum_indexes, maximum_indexes, strict=True
            )
        )
    )


def _distribution_example(
    prefix: str, summary: DistributionSummary
) -> dict[str, str | int | float | bool | None]:
    return {
        f"{prefix}_count": summary.count,
        f"{prefix}_min": summary.minimum,
        f"{prefix}_median": _round_float(summary.median_value),
        f"{prefix}_max": summary.maximum,
    }


def _drift_ratio(value: float, peer_median: float) -> float:
    if value <= 0 or peer_median <= 0:
        return float("inf") if value != peer_median else 1.0
    return max(value, peer_median) / min(value, peer_median)


def _is_physical_drift(metric_name: str, value: float, peer_median: float) -> bool:
    if peer_median == 0:
        return value > 0
    drift_ratio = _drift_ratio(value, peer_median)
    absolute_difference = abs(value - peer_median)
    return (
        drift_ratio >= PHYSICAL_DRIFT_RATIO_THRESHOLDS[metric_name]
        and absolute_difference >= PHYSICAL_DRIFT_ABSOLUTE_THRESHOLDS[metric_name]
    )


def _physical_metric_value(
    snapshot: PartitionSnapshot, dataset: str, metric_name: str
) -> float | None:
    dataset_snapshot = snapshot.dataset_snapshot_for(dataset)
    if dataset_snapshot is None:
        return None
    return _physical_metrics(dataset_snapshot)[metric_name]


def _physical_metrics(
    dataset_snapshot: DatasetPhysicalSnapshot,
) -> dict[str, float | None]:
    return {
        "row_count": float(dataset_snapshot.row_count),
        "file_size_bytes": float(dataset_snapshot.file_size_bytes),
        "row_groups": float(dataset_snapshot.row_groups),
        "compression_ratio": dataset_snapshot.compression_ratio(),
    }


def _round_float(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 4)


def _summarize_values(values: Sequence[int]) -> DistributionSummary:
    if not values:
        return DistributionSummary(0, None, None, None)
    sorted_values = sorted(values)
    return DistributionSummary(
        count=len(sorted_values),
        minimum=sorted_values[0],
        median_value=float(median(sorted_values)),
        maximum=sorted_values[-1],
    )


def _transaction_type_mix_example(
    snapshot: PartitionSnapshot,
) -> dict[str, str | int | float | bool | None]:
    example: dict[str, str | int | float | bool | None] = {
        "total_transactions": snapshot.total_transactions
    }
    for label in TRANSACTION_TYPE_LABELS:
        count = 0
        for current_label, current_count in snapshot.transaction_type_counts:
            if current_label == label:
                count = current_count
                break
        example[f"{label}_count"] = count
        example[f"{label}_share"] = round(snapshot.transaction_type_share(label), 4)
    return example
