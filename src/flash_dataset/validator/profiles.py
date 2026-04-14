"""Exact parquet schema profiles for feed-base datasets."""

from __future__ import annotations

from .common import ColumnProfile
from .common import TableProfile


def _column(name: str, type_name: str, nullable: bool) -> ColumnProfile:
    return ColumnProfile(name=name, type_name=type_name, nullable=nullable)


REQUIRED_DATASETS: tuple[str, ...] = (
    "flashblocks",
    "flashblock_withdrawals",
    "flashblock_transactions",
    "flashblock_transaction_access_list",
    "flashblock_transaction_authorizations",
    "flashblock_receipts",
    "flashblock_receipt_logs",
    "flashblock_balance_changes",
)

TABLE_PROFILES: dict[str, TableProfile] = {
    "flashblocks": TableProfile(
        columns=(
            _column("payload_id", "fixed_size_binary[8]", False),
            _column("index", "uint64", False),
            _column("content_hash", "fixed_size_binary[32]", False),
            _column("received_time_a", "timestamp[ns, tz=UTC]", True),
            _column("received_time_b", "timestamp[ns, tz=UTC]", True),
            _column("received_time_c", "timestamp[ns, tz=UTC]", True),
            _column("metadata_block_number", "uint64", False),
            _column("parent_beacon_block_root", "fixed_size_binary[32]", True),
            _column("parent_hash", "fixed_size_binary[32]", True),
            _column("fee_recipient", "fixed_size_binary[20]", True),
            _column("prev_randao", "fixed_size_binary[32]", True),
            _column("base_block_number", "uint64", True),
            _column("gas_limit", "uint64", True),
            _column("block_timestamp", "timestamp[ms, tz=UTC]", True),
            _column("extra_data", "binary", True),
            _column("base_fee_per_gas", "fixed_size_binary[32]", True),
            _column("state_root", "fixed_size_binary[32]", False),
            _column("receipts_root", "fixed_size_binary[32]", False),
            _column("logs_bloom", "fixed_size_binary[256]", False),
            _column("gas_used", "uint64", False),
            _column("block_hash", "fixed_size_binary[32]", False),
            _column("withdrawals_root", "fixed_size_binary[32]", False),
            _column("blob_gas_used", "uint64", True),
            _column("transaction_count", "uint64", False),
            _column("withdrawal_count", "uint64", False),
            _column("balance_change_count", "uint64", False),
            _column("receipt_count", "uint64", False),
        )
    ),
    "flashblock_withdrawals": TableProfile(
        columns=(
            _column("payload_id", "fixed_size_binary[8]", False),
            _column("index", "uint64", False),
            _column("block_number", "uint64", False),
            _column("withdrawal_ordinal", "uint64", False),
            _column("withdrawal_index", "uint64", False),
            _column("validator_index", "uint64", False),
            _column("address", "fixed_size_binary[20]", False),
            _column("amount", "uint64", False),
        )
    ),
    "flashblock_transactions": TableProfile(
        columns=(
            _column("payload_id", "fixed_size_binary[8]", False),
            _column("index", "uint64", False),
            _column("transaction_ordinal", "uint64", False),
            _column("block_number", "uint64", False),
            _column("transaction_hash", "fixed_size_binary[32]", False),
            _column("transaction_type", "uint8", False),
            _column("transaction_type_label", "string", False),
            _column("from_address", "fixed_size_binary[20]", False),
            _column("to_address", "fixed_size_binary[20]", True),
            _column("chain_id", "uint64", True),
            _column("nonce", "uint64", True),
            _column("gas_limit", "uint64", False),
            _column("value", "fixed_size_binary[32]", False),
            _column("gas_price", "fixed_size_binary[16]", True),
            _column("max_fee_per_gas", "fixed_size_binary[16]", True),
            _column("max_priority_fee_per_gas", "fixed_size_binary[16]", True),
            _column("input", "binary", False),
            _column("signature_y_parity", "bool", True),
            _column("signature_r", "fixed_size_binary[32]", True),
            _column("signature_s", "fixed_size_binary[32]", True),
            _column("deposit_source_hash", "fixed_size_binary[32]", True),
            _column("deposit_mint", "fixed_size_binary[16]", True),
            _column("deposit_is_system_transaction", "bool", True),
            _column("is_contract_creation", "bool", False),
            _column("input_selector", "fixed_size_binary[4]", True),
            _column("input_length", "uint64", False),
            _column("transaction_rlp", "binary", False),
        )
    ),
    "flashblock_transaction_access_list": TableProfile(
        columns=(
            _column("payload_id", "fixed_size_binary[8]", False),
            _column("index", "uint64", False),
            _column("transaction_ordinal", "uint64", False),
            _column("access_list_ordinal", "uint64", False),
            _column("address", "fixed_size_binary[20]", False),
            _column("storage_key_ordinal", "uint64", True),
            _column("storage_key", "fixed_size_binary[32]", True),
        )
    ),
    "flashblock_transaction_authorizations": TableProfile(
        columns=(
            _column("payload_id", "fixed_size_binary[8]", False),
            _column("index", "uint64", False),
            _column("transaction_ordinal", "uint64", False),
            _column("authorization_ordinal", "uint64", False),
            _column("chain_id", "fixed_size_binary[32]", False),
            _column("address", "fixed_size_binary[20]", False),
            _column("nonce", "uint64", False),
            _column("y_parity", "uint8", False),
            _column("r", "fixed_size_binary[32]", False),
            _column("s", "fixed_size_binary[32]", False),
        )
    ),
    "flashblock_receipts": TableProfile(
        columns=(
            _column("payload_id", "fixed_size_binary[8]", False),
            _column("index", "uint64", False),
            _column("block_number", "uint64", False),
            _column("transaction_hash", "fixed_size_binary[32]", False),
            _column("receipt_type", "uint8", False),
            _column("receipt_type_label", "string", False),
            _column("status_kind", "string", False),
            _column("status", "bool", True),
            _column("post_state_root", "fixed_size_binary[32]", True),
            _column("cumulative_gas_used", "uint64", False),
            _column("deposit_nonce", "uint64", True),
            _column("deposit_receipt_version", "uint64", True),
            _column("log_count", "uint64", False),
        )
    ),
    "flashblock_receipt_logs": TableProfile(
        columns=(
            _column("payload_id", "fixed_size_binary[8]", False),
            _column("index", "uint64", False),
            _column("block_number", "uint64", False),
            _column("transaction_hash", "fixed_size_binary[32]", False),
            _column("log_ordinal", "uint64", False),
            _column("address", "fixed_size_binary[20]", False),
            _column("topic_count", "uint64", False),
            _column("topic_0", "fixed_size_binary[32]", True),
            _column("topic_1", "fixed_size_binary[32]", True),
            _column("topic_2", "fixed_size_binary[32]", True),
            _column("topic_3", "fixed_size_binary[32]", True),
            _column("data", "binary", False),
        )
    ),
    "flashblock_balance_changes": TableProfile(
        columns=(
            _column("payload_id", "fixed_size_binary[8]", False),
            _column("index", "uint64", False),
            _column("block_number", "uint64", False),
            _column("address", "fixed_size_binary[20]", False),
            _column("balance", "fixed_size_binary[32]", False),
        )
    ),
}


def table_profile_for(dataset: str) -> TableProfile | None:
    """Return the schema profile for one logical dataset."""
    return TABLE_PROFILES.get(dataset)


KNOWN_DATASETS: frozenset[str] = frozenset(TABLE_PROFILES)
