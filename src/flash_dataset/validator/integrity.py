"""Cross-table integrity checks for feed-base parquet partitions."""

from __future__ import annotations

from collections.abc import Callable
from itertools import groupby
import pyarrow as pa

from . import arrow_compute as pc
from .common import iter_table_rows
from .common import PartitionIdentity
from .common import ValidationResult
from .rules import make_finding

U64 = pa.uint64()
I64 = pa.int64()


def run_integrity_checks(
    partition: PartitionIdentity,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    result: ValidationResult,
) -> None:
    """Run cross-table count, key, and ordinal integrity checks for one partition."""
    flashblocks = read_table(
        "flashblocks",
        (
            "payload_id",
            "index",
            "content_hash",
            "metadata_block_number",
            "transaction_count",
            "withdrawal_count",
            "balance_change_count",
            "receipt_count",
        ),
    )
    _run_flashblock_key_checks(partition, flashblocks, result)
    _run_flashblock_count_checks(partition, read_table, flashblocks, result)
    _run_transaction_checks(partition, read_table, flashblocks, result)
    _run_receipt_checks(partition, read_table, flashblocks, result)
    _run_balance_change_checks(partition, read_table, flashblocks, result)
    _run_withdrawal_checks(partition, read_table, flashblocks, result)


def _run_flashblock_key_checks(
    partition: PartitionIdentity, flashblocks: pa.Table, result: ValidationResult
) -> None:
    duplicate_keys = _count_duplicates(flashblocks, ("payload_id", "index"))
    if duplicate_keys > 0:
        result.add(
            make_finding(
                "duplicate_flashblock_key",
                "duplicate (payload_id, index) rows found in flashblocks",
                day=partition,
                dataset="flashblocks",
                count=duplicate_keys,
            )
        )

    payload_multiple_blocks = _count_group_distinct_gt_one(
        flashblocks,
        group_keys=("payload_id",),
        distinct_column="metadata_block_number",
    )
    if payload_multiple_blocks > 0:
        result.add(
            make_finding(
                "payload_maps_to_multiple_blocks",
                "payload_id maps to multiple metadata_block_number values",
                day=partition,
                dataset="flashblocks",
                count=payload_multiple_blocks,
            )
        )

    block_multiple_payloads = _count_group_distinct_gt_one(
        flashblocks,
        group_keys=("metadata_block_number",),
        distinct_column="payload_id",
    )
    if block_multiple_payloads > 0:
        result.add(
            make_finding(
                "block_maps_to_multiple_payloads",
                "metadata_block_number maps to multiple payload_id values",
                day=partition,
                dataset="flashblocks",
                count=block_multiple_payloads,
            )
        )

    duplicate_content_hash = _count_duplicates(flashblocks, ("content_hash",))
    if duplicate_content_hash > 0:
        result.add(
            make_finding(
                "duplicate_content_hash",
                "content_hash is duplicated across flashblock rows",
                day=partition,
                dataset="flashblocks",
                count=duplicate_content_hash,
            )
        )

    payload_ranges = flashblocks.group_by(["payload_id"]).aggregate(
        [("index", "min"), ("index", "max"), ("index", "count")]
    )
    non_contiguous = payload_ranges.filter(
        pc.not_equal(
            pc.subtract(
                pc.add(payload_ranges["index_max"], pa.scalar(1, type=U64)),
                payload_ranges["index_min"],
            ),
            payload_ranges["index_count"],
        )
    ).num_rows
    if non_contiguous > 0:
        result.add(
            make_finding(
                "non_contiguous_payload_indexes",
                "payload index interval contains an internal gap",
                day=partition,
                dataset="flashblocks",
                count=non_contiguous,
            )
        )

    late_start = payload_ranges.filter(
        pc.greater(payload_ranges["index_min"], pa.scalar(0, type=U64))
    ).num_rows
    if late_start > 0:
        result.add(
            make_finding(
                "payload_starts_after_zero",
                "payload interval starts after index zero",
                day=partition,
                dataset="flashblocks",
                count=late_start,
            )
        )
    result.add(
        make_finding(
            "tail_only_payload_count",
            f"tail-only payload count is {late_start}",
            day=partition,
            dataset="flashblocks",
            count=late_start,
        )
    )


def _run_flashblock_count_checks(
    partition: PartitionIdentity,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    flashblocks: pa.Table,
    result: ValidationResult,
) -> None:
    if _count_value_mismatches(flashblocks, "transaction_count", "receipt_count") > 0:
        result.add(
            make_finding(
                "flashblock_transaction_receipt_count_mismatch",
                "transaction_count does not equal receipt_count",
                day=partition,
                dataset="flashblocks",
            )
        )

    _check_child_count(
        day=partition,
        dataset="flashblock_transactions",
        child_column="transaction_ordinal",
        parent_count_column="transaction_count",
        read_table=read_table,
        flashblocks=flashblocks,
        result=result,
    )
    _check_child_count(
        day=partition,
        dataset="flashblock_receipts",
        child_column="transaction_hash",
        parent_count_column="receipt_count",
        read_table=read_table,
        flashblocks=flashblocks,
        result=result,
    )
    _check_child_count(
        day=partition,
        dataset="flashblock_withdrawals",
        child_column="withdrawal_ordinal",
        parent_count_column="withdrawal_count",
        read_table=read_table,
        flashblocks=flashblocks,
        result=result,
    )
    _check_child_count(
        day=partition,
        dataset="flashblock_balance_changes",
        child_column="address",
        parent_count_column="balance_change_count",
        read_table=read_table,
        flashblocks=flashblocks,
        result=result,
    )


def _run_transaction_checks(
    partition: PartitionIdentity,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    flashblocks: pa.Table,
    result: ValidationResult,
) -> None:
    transactions = read_table(
        "flashblock_transactions",
        (
            "payload_id",
            "index",
            "transaction_ordinal",
            "block_number",
            "transaction_hash",
            "transaction_type_label",
        ),
    )
    parent_keys = flashblocks.select(["payload_id", "index", "metadata_block_number"])
    _check_orphan_keys(
        partition, "flashblock_transactions", transactions, parent_keys, result
    )
    _check_child_block_number(
        partition, "flashblock_transactions", transactions, parent_keys, result
    )

    duplicate_ordinals = _count_duplicates(
        transactions,
        ("payload_id", "index", "transaction_ordinal"),
    )
    if duplicate_ordinals > 0:
        result.add(
            make_finding(
                "duplicate_transaction_ordinal",
                "transaction_ordinal is duplicated within one flashblock",
                day=partition,
                dataset="flashblock_transactions",
                count=duplicate_ordinals,
            )
        )

    duplicate_hashes = _count_duplicates(
        transactions,
        ("payload_id", "index", "transaction_hash"),
    )
    if duplicate_hashes > 0:
        result.add(
            make_finding(
                "duplicate_transaction_hash",
                "transaction_hash is duplicated within one flashblock",
                day=partition,
                dataset="flashblock_transactions",
                count=duplicate_hashes,
            )
        )

    transaction_ranges = transactions.group_by(["payload_id", "index"]).aggregate(
        [
            ("transaction_ordinal", "min"),
            ("transaction_ordinal", "max"),
            ("transaction_ordinal", "count"),
        ]
    )
    joined = flashblocks.select(["payload_id", "index", "transaction_count"]).join(
        transaction_ranges,
        keys=["payload_id", "index"],
        join_type="left outer",
    )
    non_contiguous = joined.filter(
        pc.or_kleene(
            pc.and_kleene(
                pc.greater(joined["transaction_count"], pa.scalar(0, type=U64)),
                pc.not_equal(joined["transaction_ordinal_min"], pa.scalar(0, type=U64)),
            ),
            pc.and_kleene(
                pc.greater(joined["transaction_count"], pa.scalar(0, type=U64)),
                pc.not_equal(
                    joined["transaction_ordinal_max"],
                    pc.subtract(joined["transaction_count"], pa.scalar(1, type=U64)),
                ),
            ),
        )
    ).num_rows
    if non_contiguous > 0:
        result.add(
            make_finding(
                "non_contiguous_transaction_ordinals",
                "transaction ordinals do not cover 0..transaction_count-1",
                day=partition,
                dataset="flashblock_transactions",
                count=non_contiguous,
            )
        )


def _run_receipt_checks(
    partition: PartitionIdentity,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    flashblocks: pa.Table,
    result: ValidationResult,
) -> None:
    receipts = read_table(
        "flashblock_receipts",
        (
            "payload_id",
            "index",
            "block_number",
            "transaction_hash",
            "receipt_type_label",
            "cumulative_gas_used",
            "log_count",
        ),
    )
    parent_keys = flashblocks.select(["payload_id", "index", "metadata_block_number"])
    _check_orphan_keys(partition, "flashblock_receipts", receipts, parent_keys, result)
    _check_child_block_number(
        partition, "flashblock_receipts", receipts, parent_keys, result
    )

    duplicate_receipts = _count_duplicates(
        receipts,
        ("payload_id", "index", "transaction_hash"),
    )
    if duplicate_receipts > 0:
        result.add(
            make_finding(
                "duplicate_receipt_key",
                "receipt key is duplicated within one flashblock",
                day=partition,
                dataset="flashblock_receipts",
                count=duplicate_receipts,
            )
        )

    transactions = read_table(
        "flashblock_transactions",
        (
            "payload_id",
            "index",
            "transaction_ordinal",
            "transaction_hash",
            "transaction_type_label",
        ),
    )
    transaction_keys = transactions.group_by(
        ["payload_id", "index", "transaction_hash"]
    ).aggregate([])
    receipt_keys = receipts.group_by(
        ["payload_id", "index", "transaction_hash"]
    ).aggregate([])
    missing_receipts = transaction_keys.join(
        receipt_keys,
        keys=["payload_id", "index", "transaction_hash"],
        join_type="left anti",
    ).num_rows
    missing_transactions = receipt_keys.join(
        transaction_keys,
        keys=["payload_id", "index", "transaction_hash"],
        join_type="left anti",
    ).num_rows
    if missing_receipts > 0 or missing_transactions > 0:
        result.add(
            make_finding(
                "transaction_receipt_hash_mismatch",
                "transaction and receipt hash sets differ within at least one flashblock",
                day=partition,
                count=missing_receipts + missing_transactions,
            )
        )

    joined_types = transactions.select(
        [
            "payload_id",
            "index",
            "transaction_hash",
            "transaction_type_label",
            "transaction_ordinal",
        ]
    ).join(
        receipts.select(
            [
                "payload_id",
                "index",
                "transaction_hash",
                "receipt_type_label",
                "cumulative_gas_used",
            ]
        ),
        keys=["payload_id", "index", "transaction_hash"],
        join_type="inner",
    )
    type_mismatches = joined_types.filter(
        pc.not_equal(
            joined_types["transaction_type_label"], joined_types["receipt_type_label"]
        )
    ).num_rows
    if type_mismatches > 0:
        result.add(
            make_finding(
                "transaction_receipt_type_mismatch",
                "transaction_type_label and receipt_type_label disagree for the same transaction",
                day=partition,
                count=type_mismatches,
            )
        )

    logs = read_table(
        "flashblock_receipt_logs",
        (
            "payload_id",
            "index",
            "block_number",
            "transaction_hash",
            "log_ordinal",
        ),
    )
    _check_orphan_keys(partition, "flashblock_receipt_logs", logs, parent_keys, result)
    _check_child_block_number(
        partition, "flashblock_receipt_logs", logs, parent_keys, result
    )

    duplicate_logs = _count_duplicates(
        logs,
        ("payload_id", "index", "transaction_hash", "log_ordinal"),
    )
    if duplicate_logs > 0:
        result.add(
            make_finding(
                "duplicate_receipt_log_key",
                "receipt log key is duplicated",
                day=partition,
                dataset="flashblock_receipt_logs",
                count=duplicate_logs,
            )
        )

    log_ranges = logs.group_by(["payload_id", "index", "transaction_hash"]).aggregate(
        [("log_ordinal", "min"), ("log_ordinal", "max"), ("log_ordinal", "count")]
    )
    joined_logs = receipts.select(
        ["payload_id", "index", "transaction_hash", "log_count"]
    ).join(
        log_ranges,
        keys=["payload_id", "index", "transaction_hash"],
        join_type="left outer",
    )
    log_count_mismatches = joined_logs.filter(
        pc.not_equal(
            joined_logs["log_count"], pc.fill_null(joined_logs["log_ordinal_count"], 0)
        )
    ).num_rows
    if log_count_mismatches > 0:
        result.add(
            make_finding(
                "receipt_log_count_mismatch",
                "receipt log_count does not match the number of receipt log rows",
                day=partition,
                dataset="flashblock_receipt_logs",
                count=log_count_mismatches,
            )
        )

    non_contiguous_logs = joined_logs.filter(
        pc.or_kleene(
            pc.and_kleene(
                pc.greater(joined_logs["log_count"], pa.scalar(0, type=U64)),
                pc.not_equal(joined_logs["log_ordinal_min"], pa.scalar(0, type=U64)),
            ),
            pc.and_kleene(
                pc.greater(joined_logs["log_count"], pa.scalar(0, type=U64)),
                pc.not_equal(
                    joined_logs["log_ordinal_max"],
                    pc.subtract(joined_logs["log_count"], pa.scalar(1, type=U64)),
                ),
            ),
        )
    ).num_rows
    if non_contiguous_logs > 0:
        result.add(
            make_finding(
                "non_contiguous_log_ordinals",
                "receipt log ordinals do not cover 0..log_count-1",
                day=partition,
                dataset="flashblock_receipt_logs",
                count=non_contiguous_logs,
            )
        )

    receipt_rows = joined_types.sort_by(
        [
            ("payload_id", "ascending"),
            ("index", "ascending"),
            ("transaction_ordinal", "ascending"),
        ]
    ).select(["payload_id", "index", "cumulative_gas_used"])
    monotonic_failures = 0
    for _, rows in groupby(
        iter_table_rows(receipt_rows),
        key=lambda row: (row["payload_id"], row["index"]),
    ):
        previous = -1
        for row in rows:
            current = int(row["cumulative_gas_used"])
            if current < previous:
                monotonic_failures += 1
                break
            previous = current
    if monotonic_failures > 0:
        result.add(
            make_finding(
                "receipt_cumulative_gas_used_not_monotonic",
                "receipt cumulative_gas_used moves backward within one flashblock",
                day=partition,
                dataset="flashblock_receipts",
                count=monotonic_failures,
            )
        )


def _run_balance_change_checks(
    partition: PartitionIdentity,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    flashblocks: pa.Table,
    result: ValidationResult,
) -> None:
    balances = read_table(
        "flashblock_balance_changes",
        ("payload_id", "index", "block_number", "address"),
    )
    parent_keys = flashblocks.select(["payload_id", "index", "metadata_block_number"])
    _check_orphan_keys(
        partition, "flashblock_balance_changes", balances, parent_keys, result
    )
    _check_child_block_number(
        partition, "flashblock_balance_changes", balances, parent_keys, result
    )
    duplicate_addresses = _count_duplicates(
        balances, ("payload_id", "index", "address")
    )
    if duplicate_addresses > 0:
        result.add(
            make_finding(
                "duplicate_balance_change_key",
                "balance-change address is duplicated within one flashblock",
                day=partition,
                dataset="flashblock_balance_changes",
                count=duplicate_addresses,
            )
        )


def _run_withdrawal_checks(
    partition: PartitionIdentity,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    flashblocks: pa.Table,
    result: ValidationResult,
) -> None:
    withdrawals = read_table(
        "flashblock_withdrawals",
        ("payload_id", "index", "block_number", "withdrawal_ordinal"),
    )
    parent_keys = flashblocks.select(["payload_id", "index", "metadata_block_number"])
    _check_orphan_keys(
        partition, "flashblock_withdrawals", withdrawals, parent_keys, result
    )
    _check_child_block_number(
        partition, "flashblock_withdrawals", withdrawals, parent_keys, result
    )

    duplicate_withdrawals = _count_duplicates(
        withdrawals,
        ("payload_id", "index", "withdrawal_ordinal"),
    )
    if duplicate_withdrawals > 0:
        result.add(
            make_finding(
                "duplicate_withdrawal_ordinal",
                "withdrawal_ordinal is duplicated within one flashblock",
                day=partition,
                dataset="flashblock_withdrawals",
                count=duplicate_withdrawals,
            )
        )

    withdrawal_ranges = withdrawals.group_by(["payload_id", "index"]).aggregate(
        [
            ("withdrawal_ordinal", "min"),
            ("withdrawal_ordinal", "max"),
            ("withdrawal_ordinal", "count"),
        ]
    )
    joined = flashblocks.select(["payload_id", "index", "withdrawal_count"]).join(
        withdrawal_ranges,
        keys=["payload_id", "index"],
        join_type="left outer",
    )
    non_contiguous = joined.filter(
        pc.or_kleene(
            pc.and_kleene(
                pc.greater(joined["withdrawal_count"], pa.scalar(0, type=U64)),
                pc.not_equal(joined["withdrawal_ordinal_min"], pa.scalar(0, type=U64)),
            ),
            pc.and_kleene(
                pc.greater(joined["withdrawal_count"], pa.scalar(0, type=U64)),
                pc.not_equal(
                    joined["withdrawal_ordinal_max"],
                    pc.subtract(joined["withdrawal_count"], pa.scalar(1, type=U64)),
                ),
            ),
        )
    ).num_rows
    if non_contiguous > 0:
        result.add(
            make_finding(
                "non_contiguous_withdrawal_ordinals",
                "withdrawal ordinals do not cover 0..withdrawal_count-1",
                day=partition,
                dataset="flashblock_withdrawals",
                count=non_contiguous,
            )
        )


def _check_child_count(
    day: PartitionIdentity,
    dataset: str,
    child_column: str,
    parent_count_column: str,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    flashblocks: pa.Table,
    result: ValidationResult,
) -> None:
    child = read_table(dataset, ("payload_id", "index", child_column))
    grouped = child.group_by(["payload_id", "index"]).aggregate(
        [(child_column, "count")]
    )
    joined = flashblocks.select(["payload_id", "index", parent_count_column]).join(
        grouped.rename_columns(["payload_id", "index", "child_count"]),
        keys=["payload_id", "index"],
        join_type="left outer",
    )
    mismatch_count = joined.filter(
        pc.not_equal(
            joined[parent_count_column], pc.fill_null(joined["child_count"], 0)
        )
    ).num_rows
    if mismatch_count > 0:
        result.add(
            make_finding(
                "flashblock_count_mismatch",
                f"{parent_count_column} does not match rows in {dataset}",
                day=day,
                dataset=dataset,
                count=mismatch_count,
            )
        )


def _check_orphan_keys(
    day: PartitionIdentity,
    dataset: str,
    child: pa.Table,
    parent_keys: pa.Table,
    result: ValidationResult,
) -> None:
    orphan_keys = (
        child.group_by(["payload_id", "index"])
        .aggregate([])
        .join(
            parent_keys.group_by(["payload_id", "index"]).aggregate([]),
            keys=["payload_id", "index"],
            join_type="left anti",
        )
        .num_rows
    )
    if orphan_keys > 0:
        result.add(
            make_finding(
                "orphan_child_flashblock",
                f"{dataset} contains rows whose (payload_id, index) parent is missing",
                day=day,
                dataset=dataset,
                count=orphan_keys,
            )
        )


def _check_child_block_number(
    day: PartitionIdentity,
    dataset: str,
    child: pa.Table,
    parent_keys: pa.Table,
    result: ValidationResult,
) -> None:
    joined = child.select(["payload_id", "index", "block_number"]).join(
        parent_keys,
        keys=["payload_id", "index"],
        join_type="inner",
    )
    mismatches = joined.filter(
        pc.not_equal(joined["block_number"], joined["metadata_block_number"])
    ).num_rows
    if mismatches > 0:
        result.add(
            make_finding(
                "child_block_number_mismatch",
                f"{dataset}.block_number does not match parent metadata_block_number",
                day=day,
                dataset=dataset,
                count=mismatches,
            )
        )


def _count_duplicates(table: pa.Table, keys: tuple[str, ...]) -> int:
    if table.num_rows == 0:
        return 0
    grouped = table.group_by(list(keys)).aggregate([(keys[0], "count")])
    return grouped.filter(
        pc.greater(grouped[f"{keys[0]}_count"], pa.scalar(1, type=I64))
    ).num_rows


def _count_group_distinct_gt_one(
    table: pa.Table,
    group_keys: tuple[str, ...],
    distinct_column: str,
) -> int:
    if table.num_rows == 0:
        return 0
    grouped = table.group_by(list(group_keys)).aggregate(
        [(distinct_column, "count_distinct")]
    )
    return grouped.filter(
        pc.greater(grouped[f"{distinct_column}_count_distinct"], pa.scalar(1, type=I64))
    ).num_rows


def _count_value_mismatches(table: pa.Table, left: str, right: str) -> int:
    return table.filter(pc.not_equal(table[left], table[right])).num_rows
