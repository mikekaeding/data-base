"""Validator rule catalog and finding helpers."""

from __future__ import annotations

from datetime import date

from .common import Finding
from .common import PartitionIdentity
from .common import RuleDefinition

RULES: dict[str, RuleDefinition] = {
    "partition_parse_error": RuleDefinition(
        "partition_parse_error",
        "high",
        "warn",
        "Parquet path does not match a supported storage layout.",
    ),
    "missing_required_dataset": RuleDefinition(
        "missing_required_dataset",
        "critical",
        "fail",
        "Required dataset is missing for the partition.",
    ),
    "invalid_parquet_file": RuleDefinition(
        "invalid_parquet_file",
        "critical",
        "fail",
        "Parquet footer or metadata could not be read.",
    ),
    "no_matching_parquet_files": RuleDefinition(
        "no_matching_parquet_files",
        "critical",
        "fail",
        "No parquet files matched the storage root and date filters.",
    ),
    "multiple_readable_files_for_dataset_partition": RuleDefinition(
        "multiple_readable_files_for_dataset_partition",
        "critical",
        "fail",
        "One partition contains multiple readable parquet files for the same dataset.",
    ),
    "schema_mismatch": RuleDefinition(
        "schema_mismatch",
        "critical",
        "fail",
        "Physical parquet schema does not match the writer contract.",
    ),
    "duplicate_flashblock_key": RuleDefinition(
        "duplicate_flashblock_key",
        "critical",
        "fail",
        "Flashblock logical key appears more than once.",
    ),
    "payload_maps_to_multiple_blocks": RuleDefinition(
        "payload_maps_to_multiple_blocks",
        "critical",
        "fail",
        "One payload_id maps to multiple metadata_block_number values.",
    ),
    "non_contiguous_payload_indexes": RuleDefinition(
        "non_contiguous_payload_indexes",
        "critical",
        "fail",
        "Payload indexes have an internal gap.",
    ),
    "orphan_child_flashblock": RuleDefinition(
        "orphan_child_flashblock",
        "critical",
        "fail",
        "Child rows reference a missing flashblock parent.",
    ),
    "flashblock_count_mismatch": RuleDefinition(
        "flashblock_count_mismatch",
        "critical",
        "fail",
        "Parent flashblock count does not match child-row count.",
    ),
    "flashblock_transaction_receipt_count_mismatch": RuleDefinition(
        "flashblock_transaction_receipt_count_mismatch",
        "critical",
        "fail",
        "Parent transaction_count and receipt_count differ.",
    ),
    "child_block_number_mismatch": RuleDefinition(
        "child_block_number_mismatch",
        "critical",
        "fail",
        "Child block_number does not match parent metadata_block_number.",
    ),
    "duplicate_transaction_ordinal": RuleDefinition(
        "duplicate_transaction_ordinal",
        "critical",
        "fail",
        "Transaction ordinal is duplicated within one flashblock.",
    ),
    "duplicate_transaction_hash": RuleDefinition(
        "duplicate_transaction_hash",
        "critical",
        "fail",
        "Transaction hash is duplicated within one flashblock.",
    ),
    "non_contiguous_transaction_ordinals": RuleDefinition(
        "non_contiguous_transaction_ordinals",
        "critical",
        "fail",
        "Transaction ordinals are not contiguous.",
    ),
    "duplicate_receipt_key": RuleDefinition(
        "duplicate_receipt_key",
        "critical",
        "fail",
        "Receipt key is duplicated within one flashblock.",
    ),
    "transaction_receipt_hash_mismatch": RuleDefinition(
        "transaction_receipt_hash_mismatch",
        "critical",
        "fail",
        "Transaction and receipt hash sets differ.",
    ),
    "duplicate_receipt_log_key": RuleDefinition(
        "duplicate_receipt_log_key",
        "critical",
        "fail",
        "Receipt log key is duplicated.",
    ),
    "receipt_log_count_mismatch": RuleDefinition(
        "receipt_log_count_mismatch",
        "critical",
        "fail",
        "Receipt log_count does not match actual log rows.",
    ),
    "non_contiguous_log_ordinals": RuleDefinition(
        "non_contiguous_log_ordinals",
        "critical",
        "fail",
        "Receipt log ordinals are not contiguous.",
    ),
    "duplicate_balance_change_key": RuleDefinition(
        "duplicate_balance_change_key",
        "critical",
        "fail",
        "Balance-change address appears more than once within one flashblock.",
    ),
    "duplicate_withdrawal_ordinal": RuleDefinition(
        "duplicate_withdrawal_ordinal",
        "critical",
        "fail",
        "Withdrawal ordinal is duplicated within one flashblock.",
    ),
    "non_contiguous_withdrawal_ordinals": RuleDefinition(
        "non_contiguous_withdrawal_ordinals",
        "critical",
        "fail",
        "Withdrawal ordinals are not contiguous.",
    ),
    "transaction_type_mismatch": RuleDefinition(
        "transaction_type_mismatch",
        "critical",
        "fail",
        "Transaction type code and label do not match.",
    ),
    "receipt_type_mismatch": RuleDefinition(
        "receipt_type_mismatch",
        "critical",
        "fail",
        "Receipt type code and label do not match.",
    ),
    "transaction_receipt_type_mismatch": RuleDefinition(
        "transaction_receipt_type_mismatch",
        "critical",
        "fail",
        "Joined transaction and receipt types disagree.",
    ),
    "transaction_optional_field_shape_mismatch": RuleDefinition(
        "transaction_optional_field_shape_mismatch",
        "critical",
        "fail",
        "Transaction optional fields do not match the transaction kind.",
    ),
    "contract_creation_flag_mismatch": RuleDefinition(
        "contract_creation_flag_mismatch",
        "critical",
        "fail",
        "is_contract_creation does not match to_address nullability.",
    ),
    "transaction_input_length_mismatch": RuleDefinition(
        "transaction_input_length_mismatch",
        "critical",
        "fail",
        "input_length does not match the input byte length.",
    ),
    "transaction_input_selector_mismatch": RuleDefinition(
        "transaction_input_selector_mismatch",
        "critical",
        "fail",
        "input_selector does not match the input prefix contract.",
    ),
    "transaction_rlp_hash_mismatch": RuleDefinition(
        "transaction_rlp_hash_mismatch",
        "critical",
        "fail",
        "keccak(transaction_rlp) does not match transaction_hash.",
    ),
    "base_fields_missing_on_index_zero": RuleDefinition(
        "base_fields_missing_on_index_zero",
        "critical",
        "fail",
        "Base-only flashblock fields are missing on index zero rows.",
    ),
    "base_fields_present_on_nonzero_index": RuleDefinition(
        "base_fields_present_on_nonzero_index",
        "critical",
        "fail",
        "Base-only flashblock fields appear on nonzero index rows.",
    ),
    "base_block_number_mismatch": RuleDefinition(
        "base_block_number_mismatch",
        "critical",
        "fail",
        "base_block_number does not match metadata_block_number on index zero rows.",
    ),
    "receipt_status_shape_mismatch": RuleDefinition(
        "receipt_status_shape_mismatch",
        "critical",
        "fail",
        "Receipt status fields do not match status_kind.",
    ),
    "deposit_receipt_field_mismatch": RuleDefinition(
        "deposit_receipt_field_mismatch",
        "critical",
        "fail",
        "Deposit receipt fields do not match receipt type.",
    ),
    "topic_count_out_of_range": RuleDefinition(
        "topic_count_out_of_range",
        "critical",
        "fail",
        "Receipt topic_count is outside the supported 0..4 range.",
    ),
    "topic_count_shape_mismatch": RuleDefinition(
        "topic_count_shape_mismatch",
        "critical",
        "fail",
        "Receipt topic columns do not match topic_count.",
    ),
    "orphan_access_list_row": RuleDefinition(
        "orphan_access_list_row",
        "critical",
        "fail",
        "Access-list rows reference a missing transaction.",
    ),
    "duplicate_access_list_key": RuleDefinition(
        "duplicate_access_list_key",
        "critical",
        "fail",
        "Access-list logical key is duplicated.",
    ),
    "access_list_storage_key_shape_mismatch": RuleDefinition(
        "access_list_storage_key_shape_mismatch",
        "critical",
        "fail",
        "Access-list storage key fields do not match the writer contract.",
    ),
    "non_contiguous_access_list_ordinals": RuleDefinition(
        "non_contiguous_access_list_ordinals",
        "critical",
        "fail",
        "Access-list ordinals are not contiguous within the transaction.",
    ),
    "non_contiguous_access_storage_key_ordinals": RuleDefinition(
        "non_contiguous_access_storage_key_ordinals",
        "critical",
        "fail",
        "Access-list storage key ordinals are not contiguous.",
    ),
    "orphan_authorization_row": RuleDefinition(
        "orphan_authorization_row",
        "critical",
        "fail",
        "Authorization rows reference a missing transaction.",
    ),
    "duplicate_authorization_key": RuleDefinition(
        "duplicate_authorization_key",
        "critical",
        "fail",
        "Authorization logical key is duplicated.",
    ),
    "authorization_non_7702": RuleDefinition(
        "authorization_non_7702",
        "critical",
        "fail",
        "Authorization rows appear on a non-eip7702 transaction.",
    ),
    "authorization_y_parity_out_of_range": RuleDefinition(
        "authorization_y_parity_out_of_range",
        "critical",
        "fail",
        "Authorization y_parity is outside the supported 0/1 domain.",
    ),
    "non_contiguous_authorization_ordinals": RuleDefinition(
        "non_contiguous_authorization_ordinals",
        "critical",
        "fail",
        "Authorization ordinals are not contiguous.",
    ),
    "block_maps_to_multiple_payloads": RuleDefinition(
        "block_maps_to_multiple_payloads",
        "high",
        "warn",
        "One block maps to multiple payload_id values.",
    ),
    "duplicate_content_hash": RuleDefinition(
        "duplicate_content_hash",
        "high",
        "warn",
        "content_hash is duplicated across flashblocks.",
    ),
    "all_received_time_slots_missing": RuleDefinition(
        "all_received_time_slots_missing",
        "high",
        "warn",
        "Flashblock rows are missing every received_time slot.",
    ),
    "partition_date_mismatch": RuleDefinition(
        "partition_date_mismatch",
        "high",
        "warn",
        "Partition identity disagrees with file naming or block timestamp.",
    ),
    "receipt_cumulative_gas_used_not_monotonic": RuleDefinition(
        "receipt_cumulative_gas_used_not_monotonic",
        "high",
        "warn",
        "Receipt cumulative_gas_used moves backward within one flashblock.",
    ),
    "mixed_storage_layout_for_partition": RuleDefinition(
        "mixed_storage_layout_for_partition",
        "high",
        "warn",
        "One partition mixes multiple storage layouts.",
    ),
    "unknown_dataset": RuleDefinition(
        "unknown_dataset",
        "high",
        "warn",
        "Storage contains a parquet dataset that is not part of the feed-base contract.",
    ),
    "payload_starts_after_zero": RuleDefinition(
        "payload_starts_after_zero",
        "medium",
        "warn",
        "Payload appears only as a tail interval without index zero.",
    ),
    "short_payload_run_length": RuleDefinition(
        "short_payload_run_length",
        "medium",
        "warn",
        "Payload run lengths contain unusually short intervals.",
    ),
    "excess_tail_only_payloads": RuleDefinition(
        "excess_tail_only_payloads",
        "medium",
        "warn",
        "Partition contains an unusually large number of tail-only payloads.",
    ),
    "dataset_physical_drift": RuleDefinition(
        "dataset_physical_drift",
        "medium",
        "warn",
        "Physical parquet stats drift from comparable partitions.",
    ),
    "transaction_type_mix_drift": RuleDefinition(
        "transaction_type_mix_drift",
        "medium",
        "warn",
        "Transaction-type mix drifts from comparable partitions.",
    ),
    "tail_only_payload_count": RuleDefinition(
        "tail_only_payload_count",
        "low",
        "metric",
        "Count metric for payloads that start after index zero.",
    ),
    "received_time_slot_coverage": RuleDefinition(
        "received_time_slot_coverage",
        "low",
        "metric",
        "Coverage metric for each received_time slot.",
    ),
    "dataset_physical_stats": RuleDefinition(
        "dataset_physical_stats",
        "low",
        "metric",
        "Physical parquet stats recorded for each dataset partition.",
    ),
    "payload_run_length_distribution": RuleDefinition(
        "payload_run_length_distribution",
        "low",
        "metric",
        "Payload run-length distribution recorded for the partition.",
    ),
    "flashblock_child_count_distribution": RuleDefinition(
        "flashblock_child_count_distribution",
        "low",
        "metric",
        "Child-count distributions recorded for the partition.",
    ),
    "transaction_type_mix": RuleDefinition(
        "transaction_type_mix",
        "low",
        "metric",
        "Transaction-type mix recorded for the partition.",
    ),
    "empty_dataset_count": RuleDefinition(
        "empty_dataset_count",
        "low",
        "metric",
        "Count metric for empty parquet datasets within a partition.",
    ),
}


def make_finding(
    rule_id: str,
    message: str,
    day: date | PartitionIdentity | None = None,
    dataset: str | None = None,
    file: str | None = None,
    count: int | None = None,
    example: dict[str, str | int | float | bool | None] | None = None,
) -> Finding:
    """Build one finding using rule-catalog defaults."""
    definition = RULES[rule_id]
    partition_label: str | None = None
    day_label: str | None = None
    if isinstance(day, PartitionIdentity):
        partition_label = day.label()
        day_label = day.partition_date.isoformat()
    elif day is not None:
        partition_label = day.isoformat()
        day_label = day.isoformat()
    return Finding(
        rule_id=definition.rule_id,
        status=definition.status,
        severity=definition.severity,
        message=message,
        day=day_label,
        partition=partition_label,
        dataset=dataset,
        file=file,
        count=count,
        example=example,
    )
