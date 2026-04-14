"""Field-level semantic checks for feed-base parquet partitions."""

from __future__ import annotations

from collections.abc import Callable
from itertools import groupby

import pyarrow as pa

from . import arrow_compute as pc
from .common import iter_table_rows
from .common import PartitionIdentity
from .common import ValidationResult
from .keccak import keccak256
from .rules import make_finding

TX_TYPE_LABELS: dict[int, str] = {
    0: "legacy",
    1: "eip2930",
    2: "eip1559",
    4: "eip7702",
    126: "deposit",
}

BASE_COLUMNS: tuple[str, ...] = (
    "parent_beacon_block_root",
    "parent_hash",
    "fee_recipient",
    "prev_randao",
    "base_block_number",
    "gas_limit",
    "block_timestamp",
    "extra_data",
    "base_fee_per_gas",
)

U64 = pa.uint64()
I64 = pa.int64()


def run_semantic_checks(
    partition: PartitionIdentity,
    verify_transaction_hashes: bool,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    result: ValidationResult,
) -> None:
    """Run field-level semantic checks for one validated partition."""
    flashblocks = read_table(
        "flashblocks",
        (
            "payload_id",
            "index",
            "content_hash",
            "received_time_a",
            "received_time_b",
            "received_time_c",
            "metadata_block_number",
            "parent_beacon_block_root",
            "parent_hash",
            "fee_recipient",
            "prev_randao",
            "base_block_number",
            "gas_limit",
            "block_timestamp",
            "extra_data",
            "base_fee_per_gas",
        ),
    )
    _check_received_time_coverage(partition, flashblocks, result)
    _check_partition_date_against_block_timestamp(partition, flashblocks, result)
    _check_base_field_contract(partition, flashblocks, result)
    _check_transaction_semantics(
        partition, verify_transaction_hashes, read_table, result
    )
    _check_receipt_semantics(partition, read_table, result)
    _check_access_list_semantics(partition, read_table, result)
    _check_authorization_semantics(partition, read_table, result)


def _check_received_time_coverage(
    partition: PartitionIdentity,
    flashblocks: pa.Table,
    result: ValidationResult,
) -> None:
    total_rows = flashblocks.num_rows
    for slot in ("a", "b", "c"):
        column_name = f"received_time_{slot}"
        present_count = (
            total_rows
            - flashblocks.filter(pc.is_null(flashblocks[column_name])).num_rows
        )
        result.add(
            make_finding(
                "received_time_slot_coverage",
                f"{column_name} coverage is {present_count}/{total_rows}",
                day=partition,
                dataset="flashblocks",
                count=present_count,
                example={"slot": slot, "total_rows": total_rows},
            )
        )
    missing_all_slots = flashblocks.filter(
        pc.and_kleene(
            pc.is_null(flashblocks["received_time_a"]),
            pc.and_kleene(
                pc.is_null(flashblocks["received_time_b"]),
                pc.is_null(flashblocks["received_time_c"]),
            ),
        )
    ).num_rows
    if missing_all_slots > 0:
        result.add(
            make_finding(
                "all_received_time_slots_missing",
                "flashblocks rows are missing every received_time slot",
                day=partition,
                dataset="flashblocks",
                count=missing_all_slots,
            )
        )


def _check_partition_date_against_block_timestamp(
    partition: PartitionIdentity,
    flashblocks: pa.Table,
    result: ValidationResult,
) -> None:
    index_zero_rows = flashblocks.filter(
        pc.equal(flashblocks["index"], pa.scalar(0, type=U64))
    )
    mismatches = 0
    for row in iter_table_rows(index_zero_rows.select(["block_timestamp"])):
        block_timestamp = row["block_timestamp"]
        if block_timestamp is None:
            continue
        if block_timestamp.date() != partition.partition_date:
            mismatches += 1
            continue
        if (
            partition.partition_hour is not None
            and block_timestamp.hour != partition.partition_hour
        ):
            mismatches += 1
    if mismatches > 0:
        result.add(
            make_finding(
                "partition_date_mismatch",
                "block_timestamp does not match the storage partition identity",
                day=partition,
                dataset="flashblocks",
                count=mismatches,
            )
        )


def _check_base_field_contract(
    partition: PartitionIdentity,
    flashblocks: pa.Table,
    result: ValidationResult,
) -> None:
    index_zero = flashblocks.filter(
        pc.equal(flashblocks["index"], pa.scalar(0, type=U64))
    )
    nonzero = flashblocks.filter(
        pc.greater(flashblocks["index"], pa.scalar(0, type=U64))
    )

    missing_zero = 0
    present_nonzero = 0
    for column_name in BASE_COLUMNS:
        missing_zero += index_zero.filter(pc.is_null(index_zero[column_name])).num_rows
        present_nonzero += nonzero.filter(
            pc.invert(pc.is_null(nonzero[column_name]))
        ).num_rows
    if missing_zero > 0:
        result.add(
            make_finding(
                "base_fields_missing_on_index_zero",
                "base-only flashblock fields are missing on index zero rows",
                day=partition,
                dataset="flashblocks",
                count=missing_zero,
            )
        )
    if present_nonzero > 0:
        result.add(
            make_finding(
                "base_fields_present_on_nonzero_index",
                "base-only flashblock fields appear on nonzero index rows",
                day=partition,
                dataset="flashblocks",
                count=present_nonzero,
            )
        )
    base_mismatches = index_zero.filter(
        pc.not_equal(
            index_zero["base_block_number"], index_zero["metadata_block_number"]
        )
    ).num_rows
    if base_mismatches > 0:
        result.add(
            make_finding(
                "base_block_number_mismatch",
                "base_block_number does not equal metadata_block_number on index zero rows",
                day=partition,
                dataset="flashblocks",
                count=base_mismatches,
            )
        )


def _check_transaction_semantics(
    partition: PartitionIdentity,
    verify_transaction_hashes: bool,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    result: ValidationResult,
) -> None:
    transactions = read_table(
        "flashblock_transactions",
        (
            "payload_id",
            "index",
            "transaction_ordinal",
            "transaction_type",
            "transaction_type_label",
            "to_address",
            "chain_id",
            "nonce",
            "gas_price",
            "max_fee_per_gas",
            "max_priority_fee_per_gas",
            "signature_y_parity",
            "signature_r",
            "signature_s",
            "deposit_source_hash",
            "deposit_mint",
            "deposit_is_system_transaction",
            "is_contract_creation",
            "input_selector",
            "input_length",
            "input",
            "transaction_hash",
            "transaction_rlp",
        ),
    )
    type_mismatches = 0
    for row in iter_table_rows(
        transactions.select(["transaction_type", "transaction_type_label"])
    ):
        expected_label = TX_TYPE_LABELS.get(int(row["transaction_type"]))
        if expected_label != row["transaction_type_label"]:
            type_mismatches += 1
    if type_mismatches > 0:
        result.add(
            make_finding(
                "transaction_type_mismatch",
                "transaction_type code does not match transaction_type_label",
                day=partition,
                dataset="flashblock_transactions",
                count=type_mismatches,
            )
        )

    contract_creation_mismatches = transactions.filter(
        pc.not_equal(
            pc.is_null(transactions["to_address"]), transactions["is_contract_creation"]
        )
    ).num_rows
    if contract_creation_mismatches > 0:
        result.add(
            make_finding(
                "contract_creation_flag_mismatch",
                "is_contract_creation does not match to_address nullability",
                day=partition,
                dataset="flashblock_transactions",
                count=contract_creation_mismatches,
            )
        )

    input_length_mismatches = transactions.filter(
        pc.not_equal(
            transactions["input_length"],
            pc.cast(pc.binary_length(transactions["input"]), U64),
        )
    ).num_rows
    if input_length_mismatches > 0:
        result.add(
            make_finding(
                "transaction_input_length_mismatch",
                "input_length does not equal the input byte length",
                day=partition,
                dataset="flashblock_transactions",
                count=input_length_mismatches,
            )
        )

    input_selector_mismatches = 0
    optional_field_mismatches = 0
    rlp_hash_mismatches = 0
    for row in iter_table_rows(transactions):
        input_bytes = row["input"]
        input_length = int(row["input_length"])
        selector = row["input_selector"]
        if input_length < 4:
            if selector is not None:
                input_selector_mismatches += 1
        else:
            if selector != input_bytes[:4]:
                input_selector_mismatches += 1
        if not _transaction_fields_match_type(row):
            optional_field_mismatches += 1
        if (
            verify_transaction_hashes
            and keccak256(input_bytes=row["transaction_rlp"]) != row["transaction_hash"]
        ):
            rlp_hash_mismatches += 1

    if input_selector_mismatches > 0:
        result.add(
            make_finding(
                "transaction_input_selector_mismatch",
                "input_selector does not match the first four input bytes contract",
                day=partition,
                dataset="flashblock_transactions",
                count=input_selector_mismatches,
            )
        )
    if optional_field_mismatches > 0:
        result.add(
            make_finding(
                "transaction_optional_field_shape_mismatch",
                "transaction optional fields do not match the transaction_type_label contract",
                day=partition,
                dataset="flashblock_transactions",
                count=optional_field_mismatches,
            )
        )
    if verify_transaction_hashes and rlp_hash_mismatches > 0:
        result.add(
            make_finding(
                "transaction_rlp_hash_mismatch",
                "keccak(transaction_rlp) does not match transaction_hash",
                day=partition,
                dataset="flashblock_transactions",
                count=rlp_hash_mismatches,
            )
        )


def _check_receipt_semantics(
    partition: PartitionIdentity,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    result: ValidationResult,
) -> None:
    receipts = read_table(
        "flashblock_receipts",
        (
            "payload_id",
            "index",
            "transaction_hash",
            "receipt_type",
            "receipt_type_label",
            "status_kind",
            "status",
            "post_state_root",
            "deposit_nonce",
            "deposit_receipt_version",
        ),
    )
    receipt_type_mismatches = 0
    status_shape_mismatches = 0
    deposit_shape_mismatches = 0
    for row in iter_table_rows(receipts):
        expected_label = TX_TYPE_LABELS.get(int(row["receipt_type"]))
        if expected_label != row["receipt_type_label"]:
            receipt_type_mismatches += 1
        if not _receipt_status_shape_matches(row):
            status_shape_mismatches += 1
        if not _deposit_receipt_fields_match(row):
            deposit_shape_mismatches += 1
    if receipt_type_mismatches > 0:
        result.add(
            make_finding(
                "receipt_type_mismatch",
                "receipt_type code does not match receipt_type_label",
                day=partition,
                dataset="flashblock_receipts",
                count=receipt_type_mismatches,
            )
        )
    if status_shape_mismatches > 0:
        result.add(
            make_finding(
                "receipt_status_shape_mismatch",
                "receipt status fields do not match status_kind",
                day=partition,
                dataset="flashblock_receipts",
                count=status_shape_mismatches,
            )
        )
    if deposit_shape_mismatches > 0:
        result.add(
            make_finding(
                "deposit_receipt_field_mismatch",
                "deposit receipt fields do not match receipt_type_label",
                day=partition,
                dataset="flashblock_receipts",
                count=deposit_shape_mismatches,
            )
        )

    logs = read_table(
        "flashblock_receipt_logs",
        (
            "payload_id",
            "index",
            "transaction_hash",
            "topic_count",
            "topic_0",
            "topic_1",
            "topic_2",
            "topic_3",
        ),
    )
    topic_out_of_range = logs.filter(
        pc.greater(logs["topic_count"], pa.scalar(4, type=U64))
    ).num_rows
    if topic_out_of_range > 0:
        result.add(
            make_finding(
                "topic_count_out_of_range",
                "topic_count is outside the supported 0..4 range",
                day=partition,
                dataset="flashblock_receipt_logs",
                count=topic_out_of_range,
            )
        )

    topic_shape_mismatches = 0
    for row in iter_table_rows(logs):
        topic_count = int(row["topic_count"])
        topics = [row["topic_0"], row["topic_1"], row["topic_2"], row["topic_3"]]
        for topic_index, topic in enumerate(topics, start=1):
            should_be_present = topic_count >= topic_index
            if should_be_present != (topic is not None):
                topic_shape_mismatches += 1
                break
    if topic_shape_mismatches > 0:
        result.add(
            make_finding(
                "topic_count_shape_mismatch",
                "topic_0..topic_3 nullability does not match topic_count",
                day=partition,
                dataset="flashblock_receipt_logs",
                count=topic_shape_mismatches,
            )
        )


def _check_access_list_semantics(
    partition: PartitionIdentity,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    result: ValidationResult,
) -> None:
    access = read_table(
        "flashblock_transaction_access_list",
        (
            "payload_id",
            "index",
            "transaction_ordinal",
            "access_list_ordinal",
            "address",
            "storage_key_ordinal",
            "storage_key",
        ),
    )
    transactions = read_table(
        "flashblock_transactions",
        ("payload_id", "index", "transaction_ordinal"),
    )
    orphan_rows = (
        access.group_by(["payload_id", "index", "transaction_ordinal"])
        .aggregate([])
        .join(
            transactions.group_by(
                ["payload_id", "index", "transaction_ordinal"]
            ).aggregate([]),
            keys=["payload_id", "index", "transaction_ordinal"],
            join_type="left anti",
        )
        .num_rows
    )
    if orphan_rows > 0:
        result.add(
            make_finding(
                "orphan_access_list_row",
                "access-list rows reference a missing transaction",
                day=partition,
                dataset="flashblock_transaction_access_list",
                count=orphan_rows,
            )
        )

    duplicate_keys = access.group_by(
        [
            "payload_id",
            "index",
            "transaction_ordinal",
            "access_list_ordinal",
            "storage_key_ordinal",
        ]
    ).aggregate([("address", "count")])
    duplicate_key_count = duplicate_keys.filter(
        pc.greater(duplicate_keys["address_count"], pa.scalar(1, type=I64))
    ).num_rows
    if duplicate_key_count > 0:
        result.add(
            make_finding(
                "duplicate_access_list_key",
                "access-list logical key is duplicated",
                day=partition,
                dataset="flashblock_transaction_access_list",
                count=duplicate_key_count,
            )
        )

    access_rows = access.sort_by(
        [
            ("payload_id", "ascending"),
            ("index", "ascending"),
            ("transaction_ordinal", "ascending"),
            ("access_list_ordinal", "ascending"),
            ("storage_key_ordinal", "ascending"),
        ]
    )
    ordinal_failures = 0
    storage_shape_failures = 0
    storage_ordinal_failures = 0
    for _, transaction_rows_iter in groupby(
        iter_table_rows(access_rows),
        key=lambda row: (row["payload_id"], row["index"], row["transaction_ordinal"]),
    ):
        transaction_rows = list(transaction_rows_iter)
        access_ordinals = sorted(
            {int(row["access_list_ordinal"]) for row in transaction_rows}
        )
        if access_ordinals and access_ordinals != list(range(len(access_ordinals))):
            ordinal_failures += 1
        for _, access_group_iter in groupby(
            transaction_rows,
            key=lambda row: row["access_list_ordinal"],
        ):
            access_group = list(access_group_iter)
            if not _access_group_matches_writer_contract(access_group):
                storage_shape_failures += 1
            storage_ordinals = [row["storage_key_ordinal"] for row in access_group]
            if storage_ordinals and storage_ordinals[0] is not None:
                normalized = [int(value) for value in storage_ordinals]
                if normalized != list(range(len(normalized))):
                    storage_ordinal_failures += 1
    if ordinal_failures > 0:
        result.add(
            make_finding(
                "non_contiguous_access_list_ordinals",
                "access_list_ordinal values are not contiguous within the transaction",
                day=partition,
                dataset="flashblock_transaction_access_list",
                count=ordinal_failures,
            )
        )
    if storage_shape_failures > 0:
        result.add(
            make_finding(
                "access_list_storage_key_shape_mismatch",
                "access-list storage key rows do not match the writer contract",
                day=partition,
                dataset="flashblock_transaction_access_list",
                count=storage_shape_failures,
            )
        )
    if storage_ordinal_failures > 0:
        result.add(
            make_finding(
                "non_contiguous_access_storage_key_ordinals",
                "storage_key_ordinal values are not contiguous within one access-list item",
                day=partition,
                dataset="flashblock_transaction_access_list",
                count=storage_ordinal_failures,
            )
        )


def _check_authorization_semantics(
    partition: PartitionIdentity,
    read_table: Callable[[str, tuple[str, ...]], pa.Table],
    result: ValidationResult,
) -> None:
    authorizations = read_table(
        "flashblock_transaction_authorizations",
        (
            "payload_id",
            "index",
            "transaction_ordinal",
            "authorization_ordinal",
            "y_parity",
        ),
    )
    transactions = read_table(
        "flashblock_transactions",
        (
            "payload_id",
            "index",
            "transaction_ordinal",
            "transaction_type_label",
        ),
    )
    orphan_rows = (
        authorizations.group_by(["payload_id", "index", "transaction_ordinal"])
        .aggregate([])
        .join(
            transactions.group_by(
                ["payload_id", "index", "transaction_ordinal"]
            ).aggregate([]),
            keys=["payload_id", "index", "transaction_ordinal"],
            join_type="left anti",
        )
        .num_rows
    )
    if orphan_rows > 0:
        result.add(
            make_finding(
                "orphan_authorization_row",
                "authorization rows reference a missing transaction",
                day=partition,
                dataset="flashblock_transaction_authorizations",
                count=orphan_rows,
            )
        )

    duplicate_keys = authorizations.group_by(
        ["payload_id", "index", "transaction_ordinal", "authorization_ordinal"]
    ).aggregate([("y_parity", "count")])
    duplicate_key_count = duplicate_keys.filter(
        pc.greater(duplicate_keys["y_parity_count"], pa.scalar(1, type=I64))
    ).num_rows
    if duplicate_key_count > 0:
        result.add(
            make_finding(
                "duplicate_authorization_key",
                "authorization logical key is duplicated",
                day=partition,
                dataset="flashblock_transaction_authorizations",
                count=duplicate_key_count,
            )
        )

    joined = authorizations.join(
        transactions,
        keys=["payload_id", "index", "transaction_ordinal"],
        join_type="inner",
    )
    non_7702 = joined.filter(
        pc.not_equal(joined["transaction_type_label"], "eip7702")
    ).num_rows
    if non_7702 > 0:
        result.add(
            make_finding(
                "authorization_non_7702",
                "authorization rows appear on non-eip7702 transactions",
                day=partition,
                dataset="flashblock_transaction_authorizations",
                count=non_7702,
            )
        )

    bad_y_parity = authorizations.filter(
        pc.greater(authorizations["y_parity"], pa.scalar(1, type=pa.uint8()))
    ).num_rows
    if bad_y_parity > 0:
        result.add(
            make_finding(
                "authorization_y_parity_out_of_range",
                "authorization y_parity is outside the supported 0/1 domain",
                day=partition,
                dataset="flashblock_transaction_authorizations",
                count=bad_y_parity,
            )
        )

    authorization_rows = authorizations.sort_by(
        [
            ("payload_id", "ascending"),
            ("index", "ascending"),
            ("transaction_ordinal", "ascending"),
            ("authorization_ordinal", "ascending"),
        ]
    )
    non_contiguous = 0
    for _, rows_iter in groupby(
        iter_table_rows(authorization_rows),
        key=lambda row: (row["payload_id"], row["index"], row["transaction_ordinal"]),
    ):
        rows = list(rows_iter)
        ordinals = [int(row["authorization_ordinal"]) for row in rows]
        if ordinals != list(range(len(ordinals))):
            non_contiguous += 1
    if non_contiguous > 0:
        result.add(
            make_finding(
                "non_contiguous_authorization_ordinals",
                "authorization_ordinal values are not contiguous within the transaction",
                day=partition,
                dataset="flashblock_transaction_authorizations",
                count=non_contiguous,
            )
        )


def _transaction_fields_match_type(row: dict[str, object]) -> bool:
    label = row["transaction_type_label"]
    gas_price = row["gas_price"] is not None
    max_fee = row["max_fee_per_gas"] is not None
    max_priority = row["max_priority_fee_per_gas"] is not None
    chain_id = row["chain_id"] is not None
    nonce = row["nonce"] is not None
    signature_fields = (
        row["signature_y_parity"],
        row["signature_r"],
        row["signature_s"],
    )
    deposit_fields = (
        row["deposit_source_hash"],
        row["deposit_mint"],
        row["deposit_is_system_transaction"],
    )
    signature_present = all(value is not None for value in signature_fields)
    signature_absent = all(value is None for value in signature_fields)
    deposit_present = all(value is not None for value in deposit_fields)
    deposit_absent = all(value is None for value in deposit_fields)
    if not signature_present and not signature_absent:
        return False
    if not deposit_present and not deposit_absent:
        return False
    if label == "legacy":
        return (
            gas_price
            and nonce
            and signature_present
            and not max_fee
            and not max_priority
            and deposit_absent
        )
    if label == "eip2930":
        return (
            gas_price
            and chain_id
            and nonce
            and signature_present
            and not max_fee
            and not max_priority
            and deposit_absent
        )
    if label in {"eip1559", "eip7702"}:
        return (
            not gas_price
            and max_fee
            and max_priority
            and chain_id
            and nonce
            and signature_present
            and deposit_absent
        )
    if label == "deposit":
        return (
            not gas_price
            and not max_fee
            and not max_priority
            and not chain_id
            and not nonce
            and signature_absent
            and deposit_present
        )
    return False


def _receipt_status_shape_matches(row: dict[str, object]) -> bool:
    status_kind = row["status_kind"]
    status = row["status"]
    post_state_root = row["post_state_root"]
    if status_kind == "eip658":
        return status is not None and post_state_root is None
    if status_kind == "post_state_root":
        return status is None and post_state_root is not None
    return False


def _deposit_receipt_fields_match(row: dict[str, object]) -> bool:
    is_deposit = row["receipt_type_label"] == "deposit"
    deposit_nonce = row["deposit_nonce"]
    deposit_receipt_version = row["deposit_receipt_version"]
    has_deposit_fields = (
        deposit_nonce is not None and deposit_receipt_version is not None
    )
    has_no_deposit_fields = deposit_nonce is None and deposit_receipt_version is None
    if not has_deposit_fields and not has_no_deposit_fields:
        return False
    if is_deposit:
        return has_deposit_fields
    return has_no_deposit_fields


def _access_group_matches_writer_contract(
    access_group: list[dict[str, object]],
) -> bool:
    storage_key_ordinals = [row["storage_key_ordinal"] for row in access_group]
    storage_keys = [row["storage_key"] for row in access_group]
    if storage_key_ordinals[0] is None:
        return len(access_group) == 1 and storage_keys[0] is None
    return all(
        ordinal is not None and storage_key is not None
        for ordinal, storage_key in zip(storage_key_ordinals, storage_keys)
    )
