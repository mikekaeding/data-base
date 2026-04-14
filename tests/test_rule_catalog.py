# pyright: reportAttributeAccessIssue=false, reportMissingTypeStubs=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import pyarrow as pa
import pytest

from flash_dataset.validator import RULES
from flash_dataset.validator import ValidatorConfig
from flash_dataset.validator import run_validation
from tests.helpers import DayTables
from tests.helpers import MutableTableData
from tests.helpers import append_row
from tests.helpers import append_payload_variant
from tests.helpers import b
from tests.helpers import hour_partition_file
from tests.helpers import valid_day_tables
from tests.helpers import validator_config
from tests.helpers import write_day_tables
from tests.helpers import write_parquet
from tests.helpers import write_raw_parquet
from tests.helpers import write_unknown_dataset

CURRENT_DAY = date(2026, 4, 5)


@dataclass(frozen=True)
class RuleScenario:
    """Describes one end-to-end rule trigger fixture."""

    rule_id: str
    arrange: Callable[[Path], ValidatorConfig]


def _tables_scenario(
    mutate: Callable[[DayTables], None] | None = None,
    *,
    verify_transaction_hashes: bool = False,
) -> Callable[[Path], ValidatorConfig]:
    def arrange(root: Path) -> ValidatorConfig:
        tables = valid_day_tables(CURRENT_DAY)
        if mutate is not None:
            mutate(tables)
        write_day_tables(root, CURRENT_DAY, layout="hour", tables=tables)
        return validator_config(
            root,
            verify_transaction_hashes=verify_transaction_hashes,
        )

    return arrange


def _rewrite_table(
    tables: DayTables, dataset: str, mutate: Callable[[MutableTableData], None]
) -> None:
    columns = {
        name: (data_type, list(values)) for name, data_type, values in tables[dataset]
    }
    mutate(columns)
    tables[dataset] = [
        (name, data_type, values) for name, (data_type, values) in columns.items()
    ]


def _set_cell(
    tables: DayTables, dataset: str, column: str, row_index: int, value: object
) -> None:
    def mutate(columns: MutableTableData) -> None:
        columns[column][1][row_index] = value

    _rewrite_table(tables, dataset, mutate)


def _append_existing_row(tables: DayTables, dataset: str, source_index: int) -> None:
    def mutate(columns: MutableTableData) -> None:
        append_row(columns, source_index)

    _rewrite_table(tables, dataset, mutate)


def _remap_indexes(tables: DayTables, mapping: dict[int, int]) -> None:
    for dataset in tables:

        def mutate(columns: MutableTableData) -> None:
            index_type, index_values = columns["index"]
            remapped_indexes = [
                mapping.get(int(value), int(value)) for value in index_values
            ]
            columns["index"] = (index_type, remapped_indexes)

        _rewrite_table(
            tables,
            dataset,
            mutate,
        )


def _set_payload_id_for_index(
    tables: DayTables, target_index: int, payload_id: bytes
) -> None:
    for dataset in tables:

        def mutate(columns: MutableTableData) -> None:
            indexes = columns["index"][1]
            payload_ids = columns["payload_id"][1]
            for row_index, index_value in enumerate(indexes):
                if int(index_value) == target_index:
                    payload_ids[row_index] = payload_id

        _rewrite_table(tables, dataset, mutate)


def _arrange_partition_parse_error(root: Path) -> ValidatorConfig:
    path = (
        root
        / "flashblocks"
        / "year=2026"
        / "month=04"
        / "day=xx"
        / "flashblocks_2026-04-05.parquet"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"bad path")
    return validator_config(root)


def _arrange_no_matching_parquet_files(root: Path) -> ValidatorConfig:
    return validator_config(root)


def _arrange_invalid_parquet(root: Path) -> ValidatorConfig:
    path = hour_partition_file(root, "flashblocks", CURRENT_DAY, 16)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"not parquet")
    return validator_config(root)


def _arrange_schema_mismatch(root: Path) -> ValidatorConfig:
    write_day_tables(root, CURRENT_DAY, layout="hour")
    path = hour_partition_file(root, "flashblocks", CURRENT_DAY, 16)
    write_raw_parquet(
        path,
        pa.table(
            {
                "payload_id": pa.array(["bad"], type=pa.string()),
                "index": pa.array([0], type=pa.uint64()),
            }
        ),
    )
    return validator_config(root)


def _arrange_partition_date_mismatch(root: Path) -> ValidatorConfig:
    tables = valid_day_tables(CURRENT_DAY)
    flashblocks = tables.pop("flashblocks")
    write_day_tables(root, CURRENT_DAY, layout="hour", tables=tables)
    mismatch_path = (
        root
        / "year=2026"
        / "month=04"
        / "day=05"
        / "hour=16"
        / "flashblocks_2026-04-06_16.parquet"
    )
    write_parquet(mismatch_path, "flashblocks", flashblocks)
    return validator_config(root)


def _arrange_mixed_storage_layout(root: Path) -> ValidatorConfig:
    tables = valid_day_tables(CURRENT_DAY)
    withdrawals = {"flashblock_withdrawals": tables.pop("flashblock_withdrawals")}
    write_day_tables(root, CURRENT_DAY, layout="table", tables=tables)
    write_day_tables(root, CURRENT_DAY, layout="date", tables=withdrawals)
    return validator_config(root)


def _arrange_unknown_dataset(root: Path) -> ValidatorConfig:
    write_day_tables(root, CURRENT_DAY, layout="hour")
    write_unknown_dataset(root, "unexpected_rows", CURRENT_DAY, layout="hour")
    return validator_config(root)


def _arrange_multiple_readable_files(root: Path) -> ValidatorConfig:
    tables = valid_day_tables(CURRENT_DAY)
    flashblocks = {"flashblocks": tables["flashblocks"]}
    write_day_tables(root, CURRENT_DAY, layout="table", tables=tables)
    write_day_tables(root, CURRENT_DAY, layout="date", tables=flashblocks)
    return validator_config(root)


def _mutate_missing_required_dataset(tables: DayTables) -> None:
    tables.pop("flashblock_receipts")


def _mutate_duplicate_flashblock_key(tables: DayTables) -> None:
    _remap_indexes(tables, {2: 1})


def _mutate_payload_maps_to_multiple_blocks(tables: DayTables) -> None:
    _set_cell(tables, "flashblocks", "metadata_block_number", 2, 101)


def _mutate_non_contiguous_payload_indexes(tables: DayTables) -> None:
    _remap_indexes(tables, {1: 2, 2: 3})


def _mutate_orphan_child_flashblock(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transactions", "payload_id", 0, b"\x99" * 8)


def _mutate_flashblock_count_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblocks", "transaction_count", 0, 3)


def _mutate_flashblock_transaction_receipt_count_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblocks", "receipt_count", 0, 3)


def _mutate_child_block_number_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transactions", "block_number", 0, 101)


def _mutate_duplicate_transaction_ordinal(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transactions", "transaction_ordinal", 1, 0)


def _mutate_duplicate_transaction_hash(tables: DayTables) -> None:
    duplicate_hash = tables["flashblock_transactions"][4][2][0]
    _set_cell(tables, "flashblock_transactions", "transaction_hash", 1, duplicate_hash)


def _mutate_non_contiguous_transaction_ordinals(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transactions", "transaction_ordinal", 1, 2)


def _mutate_duplicate_receipt_key(tables: DayTables) -> None:
    duplicate_hash = tables["flashblock_receipts"][3][2][0]
    _set_cell(tables, "flashblock_receipts", "transaction_hash", 1, duplicate_hash)


def _mutate_transaction_receipt_hash_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipts", "transaction_hash", 0, b(32, 240))


def _mutate_duplicate_receipt_log_key(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipt_logs", "log_ordinal", 2, 0)


def _mutate_receipt_log_count_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipts", "log_count", 1, 1)


def _mutate_non_contiguous_log_ordinals(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipt_logs", "log_ordinal", 2, 2)


def _mutate_duplicate_balance_change_key(tables: DayTables) -> None:
    _append_existing_row(tables, "flashblock_balance_changes", 0)


def _mutate_duplicate_withdrawal_ordinal(tables: DayTables) -> None:
    _append_existing_row(tables, "flashblock_withdrawals", 0)


def _mutate_non_contiguous_withdrawal_ordinals(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_withdrawals", "withdrawal_ordinal", 0, 1)


def _mutate_transaction_type_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transactions", "transaction_type_label", 0, "eip1559")


def _mutate_receipt_type_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipts", "receipt_type_label", 0, "eip1559")


def _mutate_transaction_receipt_type_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipts", "receipt_type", 0, 1)
    _set_cell(tables, "flashblock_receipts", "receipt_type_label", 0, "eip2930")


def _mutate_transaction_optional_field_shape_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transactions", "gas_price", 2, b"\x01" * 16)


def _mutate_contract_creation_flag_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transactions", "to_address", 0, None)


def _mutate_transaction_input_length_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transactions", "input_length", 0, 4)


def _mutate_transaction_input_selector_mismatch(tables: DayTables) -> None:
    _set_cell(
        tables, "flashblock_transactions", "input_selector", 1, b"\x00\x00\x00\x00"
    )


def _mutate_transaction_rlp_hash_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transactions", "transaction_hash", 0, b(32, 250))


def _mutate_base_fields_missing_on_index_zero(tables: DayTables) -> None:
    _set_cell(tables, "flashblocks", "parent_hash", 0, None)


def _mutate_base_fields_present_on_nonzero_index(tables: DayTables) -> None:
    _set_cell(tables, "flashblocks", "parent_hash", 1, b(32, 201))


def _mutate_base_block_number_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblocks", "base_block_number", 0, 101)


def _mutate_receipt_status_shape_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipts", "status_kind", 0, "eip658")


def _mutate_deposit_receipt_field_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipts", "deposit_nonce", 0, 1)
    _set_cell(tables, "flashblock_receipts", "deposit_receipt_version", 0, 1)


def _mutate_topic_count_out_of_range(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipt_logs", "topic_count", 0, 5)


def _mutate_topic_count_shape_mismatch(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipt_logs", "topic_3", 3, None)


def _mutate_orphan_access_list_row(tables: DayTables) -> None:
    _set_cell(
        tables, "flashblock_transaction_access_list", "transaction_ordinal", 0, 99
    )


def _mutate_duplicate_access_list_key(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transaction_access_list", "storage_key_ordinal", 1, 0)


def _mutate_access_list_storage_key_shape_mismatch(tables: DayTables) -> None:
    _set_cell(
        tables, "flashblock_transaction_access_list", "storage_key", 2, b(32, 202)
    )


def _mutate_non_contiguous_access_list_ordinals(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transaction_access_list", "access_list_ordinal", 2, 2)


def _mutate_non_contiguous_access_storage_key_ordinals(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transaction_access_list", "storage_key_ordinal", 1, 2)


def _mutate_orphan_authorization_row(tables: DayTables) -> None:
    _set_cell(
        tables, "flashblock_transaction_authorizations", "transaction_ordinal", 0, 99
    )


def _mutate_duplicate_authorization_key(tables: DayTables) -> None:
    _set_cell(
        tables, "flashblock_transaction_authorizations", "authorization_ordinal", 1, 0
    )


def _mutate_authorization_non_7702(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transactions", "transaction_type", 3, 2)
    _set_cell(tables, "flashblock_transactions", "transaction_type_label", 3, "eip1559")
    _set_cell(tables, "flashblock_receipts", "receipt_type", 3, 2)
    _set_cell(tables, "flashblock_receipts", "receipt_type_label", 3, "eip1559")


def _mutate_authorization_y_parity_out_of_range(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_transaction_authorizations", "y_parity", 0, 2)


def _mutate_non_contiguous_authorization_ordinals(tables: DayTables) -> None:
    _set_cell(
        tables, "flashblock_transaction_authorizations", "authorization_ordinal", 1, 2
    )


def _mutate_block_maps_to_multiple_payloads(tables: DayTables) -> None:
    _set_payload_id_for_index(tables, 2, b(8, 88))


def _mutate_duplicate_content_hash(tables: DayTables) -> None:
    _set_cell(tables, "flashblocks", "content_hash", 1, b(32, 1))


def _mutate_all_received_time_slots_missing(tables: DayTables) -> None:
    _set_cell(tables, "flashblocks", "received_time_a", 0, None)
    _set_cell(tables, "flashblocks", "received_time_b", 0, None)
    _set_cell(tables, "flashblocks", "received_time_c", 0, None)


def _mutate_receipt_cumulative_gas_used_not_monotonic(tables: DayTables) -> None:
    _set_cell(tables, "flashblock_receipts", "cumulative_gas_used", 1, 20_000)


def _mutate_payload_starts_after_zero(tables: DayTables) -> None:
    _remap_indexes(tables, {0: 1, 1: 2, 2: 3})


def _mutate_all_transactions_legacy(tables: DayTables) -> None:
    def mutate_transactions(columns: MutableTableData) -> None:
        row_count = len(columns["transaction_type"][1])
        columns["transaction_type"][1][:] = [0] * row_count
        columns["transaction_type_label"][1][:] = ["legacy"] * row_count
        columns["gas_price"][1][:] = [b(16, 61)] * row_count
        columns["max_fee_per_gas"][1][:] = [None] * row_count
        columns["max_priority_fee_per_gas"][1][:] = [None] * row_count
        columns["deposit_source_hash"][1][:] = [None] * row_count
        columns["deposit_mint"][1][:] = [None] * row_count
        columns["deposit_is_system_transaction"][1][:] = [None] * row_count
        columns["nonce"][1][:] = [index + 1 for index in range(row_count)]
        columns["signature_y_parity"][1][:] = [
            bool(index % 2) for index in range(row_count)
        ]
        columns["signature_r"][1][:] = [
            b(32, 180 + index) for index in range(row_count)
        ]
        columns["signature_s"][1][:] = [
            b(32, 190 + index) for index in range(row_count)
        ]

    def mutate_receipts(columns: MutableTableData) -> None:
        row_count = len(columns["receipt_type"][1])
        columns["receipt_type"][1][:] = [0] * row_count
        columns["receipt_type_label"][1][:] = ["legacy"] * row_count
        columns["deposit_nonce"][1][:] = [None] * row_count
        columns["deposit_receipt_version"][1][:] = [None] * row_count

    def clear_rows(columns: MutableTableData) -> None:
        for _, values in columns.values():
            values.clear()

    _rewrite_table(tables, "flashblock_transactions", mutate_transactions)
    _rewrite_table(tables, "flashblock_receipts", mutate_receipts)
    _rewrite_table(tables, "flashblock_transaction_access_list", clear_rows)
    _rewrite_table(tables, "flashblock_transaction_authorizations", clear_rows)


def _arrange_dataset_physical_drift(root: Path) -> ValidatorConfig:
    for day_number in range(1, 4):
        write_day_tables(root, date(2026, 4, day_number), layout="hour")
    tables = valid_day_tables(date(2026, 4, 4))
    append_payload_variant(
        tables,
        new_payload_seed=20,
        new_block_number=200,
    )
    write_day_tables(root, date(2026, 4, 4), layout="hour", tables=tables)
    return validator_config(root)


def _arrange_short_payload_run_length(root: Path) -> ValidatorConfig:
    tables = valid_day_tables(CURRENT_DAY)
    append_payload_variant(tables, new_payload_seed=20, new_block_number=200)
    append_payload_variant(tables, new_payload_seed=21, new_block_number=201)
    append_payload_variant(tables, new_payload_seed=22, new_block_number=202)
    append_payload_variant(
        tables,
        new_payload_seed=23,
        new_block_number=203,
        included_indexes=(0,),
    )
    write_day_tables(root, CURRENT_DAY, layout="hour", tables=tables)
    return validator_config(root)


def _arrange_excess_tail_only_payloads(root: Path) -> ValidatorConfig:
    tables = valid_day_tables(CURRENT_DAY)
    append_payload_variant(
        tables,
        new_payload_seed=20,
        new_block_number=200,
        included_indexes=(1, 2),
    )
    append_payload_variant(
        tables,
        new_payload_seed=21,
        new_block_number=201,
        included_indexes=(1, 2),
    )
    append_payload_variant(
        tables,
        new_payload_seed=22,
        new_block_number=202,
        included_indexes=(1, 2),
    )
    write_day_tables(root, CURRENT_DAY, layout="hour", tables=tables)
    return validator_config(root)


def _arrange_transaction_type_mix_drift(root: Path) -> ValidatorConfig:
    for day_number in range(1, 4):
        write_day_tables(root, date(2026, 4, day_number), layout="hour")
    tables = valid_day_tables(date(2026, 4, 4))
    _mutate_all_transactions_legacy(tables)
    write_day_tables(root, date(2026, 4, 4), layout="hour", tables=tables)
    return validator_config(root)


RULE_SCENARIOS: list[RuleScenario] = [
    RuleScenario("partition_parse_error", _arrange_partition_parse_error),
    RuleScenario("no_matching_parquet_files", _arrange_no_matching_parquet_files),
    RuleScenario(
        "missing_required_dataset", _tables_scenario(_mutate_missing_required_dataset)
    ),
    RuleScenario("invalid_parquet_file", _arrange_invalid_parquet),
    RuleScenario(
        "multiple_readable_files_for_dataset_partition",
        _arrange_multiple_readable_files,
    ),
    RuleScenario("schema_mismatch", _arrange_schema_mismatch),
    RuleScenario(
        "duplicate_flashblock_key", _tables_scenario(_mutate_duplicate_flashblock_key)
    ),
    RuleScenario(
        "payload_maps_to_multiple_blocks",
        _tables_scenario(_mutate_payload_maps_to_multiple_blocks),
    ),
    RuleScenario(
        "non_contiguous_payload_indexes",
        _tables_scenario(_mutate_non_contiguous_payload_indexes),
    ),
    RuleScenario(
        "orphan_child_flashblock", _tables_scenario(_mutate_orphan_child_flashblock)
    ),
    RuleScenario(
        "flashblock_count_mismatch", _tables_scenario(_mutate_flashblock_count_mismatch)
    ),
    RuleScenario(
        "flashblock_transaction_receipt_count_mismatch",
        _tables_scenario(_mutate_flashblock_transaction_receipt_count_mismatch),
    ),
    RuleScenario(
        "child_block_number_mismatch",
        _tables_scenario(_mutate_child_block_number_mismatch),
    ),
    RuleScenario(
        "duplicate_transaction_ordinal",
        _tables_scenario(_mutate_duplicate_transaction_ordinal),
    ),
    RuleScenario(
        "duplicate_transaction_hash",
        _tables_scenario(_mutate_duplicate_transaction_hash),
    ),
    RuleScenario(
        "non_contiguous_transaction_ordinals",
        _tables_scenario(_mutate_non_contiguous_transaction_ordinals),
    ),
    RuleScenario(
        "duplicate_receipt_key", _tables_scenario(_mutate_duplicate_receipt_key)
    ),
    RuleScenario(
        "transaction_receipt_hash_mismatch",
        _tables_scenario(_mutate_transaction_receipt_hash_mismatch),
    ),
    RuleScenario(
        "duplicate_receipt_log_key", _tables_scenario(_mutate_duplicate_receipt_log_key)
    ),
    RuleScenario(
        "receipt_log_count_mismatch",
        _tables_scenario(_mutate_receipt_log_count_mismatch),
    ),
    RuleScenario(
        "non_contiguous_log_ordinals",
        _tables_scenario(_mutate_non_contiguous_log_ordinals),
    ),
    RuleScenario(
        "duplicate_balance_change_key",
        _tables_scenario(_mutate_duplicate_balance_change_key),
    ),
    RuleScenario(
        "duplicate_withdrawal_ordinal",
        _tables_scenario(_mutate_duplicate_withdrawal_ordinal),
    ),
    RuleScenario(
        "non_contiguous_withdrawal_ordinals",
        _tables_scenario(_mutate_non_contiguous_withdrawal_ordinals),
    ),
    RuleScenario(
        "transaction_type_mismatch", _tables_scenario(_mutate_transaction_type_mismatch)
    ),
    RuleScenario(
        "receipt_type_mismatch", _tables_scenario(_mutate_receipt_type_mismatch)
    ),
    RuleScenario(
        "transaction_receipt_type_mismatch",
        _tables_scenario(_mutate_transaction_receipt_type_mismatch),
    ),
    RuleScenario(
        "transaction_optional_field_shape_mismatch",
        _tables_scenario(_mutate_transaction_optional_field_shape_mismatch),
    ),
    RuleScenario(
        "contract_creation_flag_mismatch",
        _tables_scenario(_mutate_contract_creation_flag_mismatch),
    ),
    RuleScenario(
        "transaction_input_length_mismatch",
        _tables_scenario(_mutate_transaction_input_length_mismatch),
    ),
    RuleScenario(
        "transaction_input_selector_mismatch",
        _tables_scenario(_mutate_transaction_input_selector_mismatch),
    ),
    RuleScenario(
        "transaction_rlp_hash_mismatch",
        _tables_scenario(
            _mutate_transaction_rlp_hash_mismatch, verify_transaction_hashes=True
        ),
    ),
    RuleScenario(
        "base_fields_missing_on_index_zero",
        _tables_scenario(_mutate_base_fields_missing_on_index_zero),
    ),
    RuleScenario(
        "base_fields_present_on_nonzero_index",
        _tables_scenario(_mutate_base_fields_present_on_nonzero_index),
    ),
    RuleScenario(
        "base_block_number_mismatch",
        _tables_scenario(_mutate_base_block_number_mismatch),
    ),
    RuleScenario(
        "receipt_status_shape_mismatch",
        _tables_scenario(_mutate_receipt_status_shape_mismatch),
    ),
    RuleScenario(
        "deposit_receipt_field_mismatch",
        _tables_scenario(_mutate_deposit_receipt_field_mismatch),
    ),
    RuleScenario(
        "topic_count_out_of_range", _tables_scenario(_mutate_topic_count_out_of_range)
    ),
    RuleScenario(
        "topic_count_shape_mismatch",
        _tables_scenario(_mutate_topic_count_shape_mismatch),
    ),
    RuleScenario(
        "orphan_access_list_row", _tables_scenario(_mutate_orphan_access_list_row)
    ),
    RuleScenario(
        "duplicate_access_list_key", _tables_scenario(_mutate_duplicate_access_list_key)
    ),
    RuleScenario(
        "access_list_storage_key_shape_mismatch",
        _tables_scenario(_mutate_access_list_storage_key_shape_mismatch),
    ),
    RuleScenario(
        "non_contiguous_access_list_ordinals",
        _tables_scenario(_mutate_non_contiguous_access_list_ordinals),
    ),
    RuleScenario(
        "non_contiguous_access_storage_key_ordinals",
        _tables_scenario(_mutate_non_contiguous_access_storage_key_ordinals),
    ),
    RuleScenario(
        "orphan_authorization_row", _tables_scenario(_mutate_orphan_authorization_row)
    ),
    RuleScenario(
        "duplicate_authorization_key",
        _tables_scenario(_mutate_duplicate_authorization_key),
    ),
    RuleScenario(
        "authorization_non_7702", _tables_scenario(_mutate_authorization_non_7702)
    ),
    RuleScenario(
        "authorization_y_parity_out_of_range",
        _tables_scenario(_mutate_authorization_y_parity_out_of_range),
    ),
    RuleScenario(
        "non_contiguous_authorization_ordinals",
        _tables_scenario(_mutate_non_contiguous_authorization_ordinals),
    ),
    RuleScenario(
        "block_maps_to_multiple_payloads",
        _tables_scenario(_mutate_block_maps_to_multiple_payloads),
    ),
    RuleScenario(
        "duplicate_content_hash", _tables_scenario(_mutate_duplicate_content_hash)
    ),
    RuleScenario(
        "all_received_time_slots_missing",
        _tables_scenario(_mutate_all_received_time_slots_missing),
    ),
    RuleScenario("partition_date_mismatch", _arrange_partition_date_mismatch),
    RuleScenario(
        "receipt_cumulative_gas_used_not_monotonic",
        _tables_scenario(_mutate_receipt_cumulative_gas_used_not_monotonic),
    ),
    RuleScenario("mixed_storage_layout_for_partition", _arrange_mixed_storage_layout),
    RuleScenario("unknown_dataset", _arrange_unknown_dataset),
    RuleScenario(
        "payload_starts_after_zero", _tables_scenario(_mutate_payload_starts_after_zero)
    ),
    RuleScenario("short_payload_run_length", _arrange_short_payload_run_length),
    RuleScenario("excess_tail_only_payloads", _arrange_excess_tail_only_payloads),
    RuleScenario("dataset_physical_drift", _arrange_dataset_physical_drift),
    RuleScenario("transaction_type_mix_drift", _arrange_transaction_type_mix_drift),
    RuleScenario(
        "tail_only_payload_count", _tables_scenario(_mutate_payload_starts_after_zero)
    ),
    RuleScenario("received_time_slot_coverage", _tables_scenario()),
    RuleScenario("dataset_physical_stats", _tables_scenario()),
    RuleScenario("payload_run_length_distribution", _tables_scenario()),
    RuleScenario("flashblock_child_count_distribution", _tables_scenario()),
    RuleScenario("transaction_type_mix", _tables_scenario()),
    RuleScenario("empty_dataset_count", _tables_scenario()),
]


def test_rule_scenarios_cover_catalog() -> None:
    assert {scenario.rule_id for scenario in RULE_SCENARIOS} == set(RULES)


@pytest.mark.parametrize(
    "scenario",
    RULE_SCENARIOS,
    ids=[scenario.rule_id for scenario in RULE_SCENARIOS],
)
def test_rule_fixture_triggers_expected_rule(
    tmp_path: Path, scenario: RuleScenario
) -> None:
    result = run_validation(scenario.arrange(tmp_path))
    assert scenario.rule_id in {finding.rule_id for finding in result.findings}
