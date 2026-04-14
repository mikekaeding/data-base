# pyright: reportAttributeAccessIssue=false, reportMissingTypeStubs=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
"""Shared test helpers for feed-base parquet validation."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC
from datetime import date
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import TypeAlias

import pyarrow as pa
import pyarrow.parquet as pq

from flash_dataset.validator import ValidatorConfig
from flash_dataset.validator.keccak import keccak256
from flash_dataset.validator.profiles import table_profile_for

ColumnValues: TypeAlias = list[Any]
ArrowDataType: TypeAlias = Any
ColumnData: TypeAlias = tuple[str, ArrowDataType, ColumnValues]
TableData: TypeAlias = list[ColumnData]
DayTables: TypeAlias = dict[str, TableData]
MutableTableData: TypeAlias = dict[str, tuple[ArrowDataType, ColumnValues]]


def b(size: int, seed: int) -> bytes:
    """Return one fixed-width byte string for deterministic test data."""
    return bytes([(seed % 251) + 1]) * size


def table_partition_file(root: Path, dataset: str, current_day: date) -> Path:
    """Return one table-first parquet path."""
    return (
        root
        / dataset
        / f"year={current_day.year:04d}"
        / f"month={current_day.month:02d}"
        / f"day={current_day.day:02d}"
        / f"{dataset}_{current_day.isoformat()}.parquet"
    )


def date_partition_file(root: Path, dataset: str, current_day: date) -> Path:
    """Return one production date-first parquet path."""
    return (
        root
        / f"date={current_day.isoformat()}"
        / f"{dataset}_{current_day.isoformat()}.parquet"
    )


def hour_partition_file(root: Path, dataset: str, current_day: date, hour: int) -> Path:
    """Return one production hour-partition parquet path."""
    return (
        root
        / f"year={current_day.year:04d}"
        / f"month={current_day.month:02d}"
        / f"day={current_day.day:02d}"
        / f"hour={hour:02d}"
        / f"{dataset}_{current_day.isoformat()}_{hour:02d}.parquet"
    )


def dataset_file_path(
    root: Path,
    dataset: str,
    current_day: date,
    layout: str,
    hour: int = 16,
) -> Path:
    """Return one parquet path for the requested test storage layout."""
    if layout == "table":
        return table_partition_file(root, dataset, current_day)
    if layout == "date":
        return date_partition_file(root, dataset, current_day)
    if layout == "hour":
        return hour_partition_file(root, dataset, current_day, hour)
    raise ValueError(f"unsupported test layout: {layout}")


def write_parquet(
    path: Path,
    dataset: str,
    columns: TableData,
) -> None:
    """Write one parquet file with an explicit schema and column order."""
    path.parent.mkdir(parents=True, exist_ok=True)
    profile = table_profile_for(dataset)
    assert profile is not None
    arrays = []
    fields = []
    for name, data_type, values in columns:
        profile_column = next(
            column for column in profile.columns if column.name == name
        )
        arrays.append(pa.array(values, type=data_type))
        fields.append(pa.field(name, data_type, nullable=profile_column.nullable))
    pq.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields)), path)


def write_raw_parquet(path: Path, table: pa.Table) -> None:
    """Write one parquet file without applying a feed-base schema profile."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, path)


def validator_config(
    root: Path,
    verify_transaction_hashes: bool = False,
    output_directory: Path | None = None,
) -> ValidatorConfig:
    """Build a standard validator config for tests rooted at one temp directory."""
    return ValidatorConfig(
        storage_root=root,
        output_directory=(
            root / "reports" if output_directory is None else output_directory
        ),
        date_from=None,
        date_to=None,
        verify_transaction_hashes=verify_transaction_hashes,
    )


def mutable_table(tables: DayTables, dataset: str) -> MutableTableData:
    """Return one mutable column map while preserving the table's column order."""
    return {
        name: (data_type, list(values)) for name, data_type, values in tables[dataset]
    }


def replace_table(tables: DayTables, dataset: str, columns: MutableTableData) -> None:
    """Write one mutable column map back into the table list structure."""
    tables[dataset] = [
        (name, data_type, values) for name, (data_type, values) in columns.items()
    ]


def append_row(columns: MutableTableData, source_index: int) -> None:
    """Append a deep-copied row from one existing row index."""
    for _, values in columns.values():
        values.append(deepcopy(values[source_index]))


def append_payload_variant(
    tables: DayTables,
    *,
    new_payload_seed: int,
    new_block_number: int,
    included_indexes: tuple[int, ...] = (0, 1, 2),
) -> None:
    """Append one consistent payload copy derived from the base synthetic payload."""
    source_payload_id = b(8, 1)
    new_payload_id = b(8, new_payload_seed)
    included_index_set = {int(index) for index in included_indexes}
    transaction_hash_map: dict[bytes, bytes] = {}
    transaction_rlp_map: dict[bytes, bytes] = {}

    transaction_columns = mutable_table(tables, "flashblock_transactions")
    for row_index in _matching_row_indexes(
        transaction_columns, source_payload_id, included_index_set
    ):
        original_hash = transaction_columns["transaction_hash"][1][row_index]
        original_rlp = transaction_columns["transaction_rlp"][1][row_index]
        suffix = bytes([new_payload_seed % 251, row_index % 251])
        new_rlp = original_rlp + suffix
        transaction_rlp_map[original_hash] = new_rlp
        transaction_hash_map[original_hash] = keccak256(new_rlp)

    for dataset in tuple(tables):
        columns = mutable_table(tables, dataset)
        source_row_indexes = _matching_row_indexes(
            columns, source_payload_id, included_index_set
        )
        for appended_row_index, source_row_index in enumerate(source_row_indexes):
            source_transaction_hash = None
            if "transaction_hash" in columns:
                source_transaction_hash = columns["transaction_hash"][1][
                    source_row_index
                ]
            for name, (_, values) in columns.items():
                value = deepcopy(values[source_row_index])
                if name == "payload_id":
                    value = new_payload_id
                elif name == "metadata_block_number" and value is not None:
                    value = new_block_number
                elif name == "base_block_number" and value is not None:
                    value = new_block_number
                elif name == "block_number" and value is not None:
                    value = new_block_number
                elif name == "content_hash" and value is not None:
                    value = b(32, (new_payload_seed * 10) + appended_row_index + 1)
                elif name == "transaction_rlp" and source_transaction_hash is not None:
                    value = transaction_rlp_map[source_transaction_hash]
                elif name == "transaction_hash" and source_transaction_hash is not None:
                    value = transaction_hash_map[source_transaction_hash]
                values.append(value)
        replace_table(tables, dataset, columns)


def valid_day_tables(current_day: date, hour: int = 16) -> DayTables:
    """Return one internally consistent synthetic feed-base partition fixture."""
    start = datetime(
        current_day.year,
        current_day.month,
        current_day.day,
        hour,
        0,
        0,
        tzinfo=UTC,
    )
    transaction_rlps = [b"legacy", b"eip2930", b"eip1559", b"eip7702", b"deposit"]
    tx_hashes = [keccak256(transaction_rlp) for transaction_rlp in transaction_rlps]
    return {
        "flashblocks": [
            ("payload_id", pa.binary(8), [b(8, 1), b(8, 1), b(8, 1)]),
            ("index", pa.uint64(), [0, 1, 2]),
            ("content_hash", pa.binary(32), [b(32, 1), b(32, 2), b(32, 3)]),
            (
                "received_time_a",
                pa.timestamp("ns", tz="UTC"),
                [start, start + timedelta(seconds=1), start + timedelta(seconds=2)],
            ),
            ("received_time_b", pa.timestamp("ns", tz="UTC"), [None, None, None]),
            ("received_time_c", pa.timestamp("ns", tz="UTC"), [None, None, None]),
            ("metadata_block_number", pa.uint64(), [100, 100, 100]),
            ("parent_beacon_block_root", pa.binary(32), [b(32, 10), None, None]),
            ("parent_hash", pa.binary(32), [b(32, 11), None, None]),
            ("fee_recipient", pa.binary(20), [b(20, 12), None, None]),
            ("prev_randao", pa.binary(32), [b(32, 13), None, None]),
            ("base_block_number", pa.uint64(), [100, None, None]),
            ("gas_limit", pa.uint64(), [30_000_000, None, None]),
            (
                "block_timestamp",
                pa.timestamp("ms", tz="UTC"),
                [start, None, None],
            ),
            ("extra_data", pa.binary(), [b"base", None, None]),
            ("base_fee_per_gas", pa.binary(32), [b(32, 14), None, None]),
            ("state_root", pa.binary(32), [b(32, 15), b(32, 16), b(32, 17)]),
            ("receipts_root", pa.binary(32), [b(32, 18), b(32, 19), b(32, 20)]),
            ("logs_bloom", pa.binary(256), [b(256, 21), b(256, 22), b(256, 23)]),
            ("gas_used", pa.uint64(), [1000, 2000, 3000]),
            ("block_hash", pa.binary(32), [b(32, 24), b(32, 25), b(32, 26)]),
            ("withdrawals_root", pa.binary(32), [b(32, 27), b(32, 28), b(32, 29)]),
            ("blob_gas_used", pa.uint64(), [None, None, None]),
            ("transaction_count", pa.uint64(), [2, 2, 1]),
            ("withdrawal_count", pa.uint64(), [1, 0, 0]),
            ("balance_change_count", pa.uint64(), [1, 1, 1]),
            ("receipt_count", pa.uint64(), [2, 2, 1]),
        ],
        "flashblock_transactions": [
            ("payload_id", pa.binary(8), [b(8, 1)] * 5),
            ("index", pa.uint64(), [0, 0, 1, 1, 2]),
            ("transaction_ordinal", pa.uint64(), [0, 1, 0, 1, 0]),
            ("block_number", pa.uint64(), [100, 100, 100, 100, 100]),
            ("transaction_hash", pa.binary(32), tx_hashes),
            ("transaction_type", pa.uint8(), [0, 1, 2, 4, 126]),
            (
                "transaction_type_label",
                pa.string(),
                ["legacy", "eip2930", "eip1559", "eip7702", "deposit"],
            ),
            (
                "from_address",
                pa.binary(20),
                [b(20, 30), b(20, 31), b(20, 32), b(20, 33), b(20, 34)],
            ),
            (
                "to_address",
                pa.binary(20),
                [b(20, 40), b(20, 41), b(20, 42), b(20, 43), b(20, 44)],
            ),
            ("chain_id", pa.uint64(), [None, 8453, 8453, 8453, None]),
            ("nonce", pa.uint64(), [1, 2, 3, 4, None]),
            ("gas_limit", pa.uint64(), [21_000, 25_000, 30_000, 35_000, 1_000_000]),
            (
                "value",
                pa.binary(32),
                [b(32, 50), b(32, 51), b(32, 52), b(32, 53), b(32, 54)],
            ),
            ("gas_price", pa.binary(16), [b(16, 60), b(16, 61), None, None, None]),
            (
                "max_fee_per_gas",
                pa.binary(16),
                [None, None, b(16, 62), b(16, 63), None],
            ),
            (
                "max_priority_fee_per_gas",
                pa.binary(16),
                [None, None, b(16, 64), b(16, 65), None],
            ),
            (
                "input",
                pa.binary(),
                [
                    b"\x01\x02\x03",
                    b"\xaa\xbb\xcc\xdd\xee",
                    b"\x11\x22\x33\x44",
                    b"\x10\x20\x30\x40\x50",
                    b"\x99\x88\x77\x66",
                ],
            ),
            ("signature_y_parity", pa.bool_(), [True, False, True, False, None]),
            (
                "signature_r",
                pa.binary(32),
                [b(32, 70), b(32, 71), b(32, 72), b(32, 73), None],
            ),
            (
                "signature_s",
                pa.binary(32),
                [b(32, 74), b(32, 75), b(32, 76), b(32, 77), None],
            ),
            ("deposit_source_hash", pa.binary(32), [None, None, None, None, b(32, 78)]),
            ("deposit_mint", pa.binary(16), [None, None, None, None, b(16, 79)]),
            (
                "deposit_is_system_transaction",
                pa.bool_(),
                [None, None, None, None, False],
            ),
            ("is_contract_creation", pa.bool_(), [False, False, False, False, False]),
            (
                "input_selector",
                pa.binary(4),
                [
                    None,
                    b"\xaa\xbb\xcc\xdd",
                    b"\x11\x22\x33\x44",
                    b"\x10\x20\x30\x40",
                    b"\x99\x88\x77\x66",
                ],
            ),
            ("input_length", pa.uint64(), [3, 5, 4, 5, 4]),
            (
                "transaction_rlp",
                pa.binary(),
                transaction_rlps,
            ),
        ],
        "flashblock_receipts": [
            ("payload_id", pa.binary(8), [b(8, 1)] * 5),
            ("index", pa.uint64(), [0, 0, 1, 1, 2]),
            ("block_number", pa.uint64(), [100, 100, 100, 100, 100]),
            ("transaction_hash", pa.binary(32), tx_hashes),
            ("receipt_type", pa.uint8(), [0, 1, 2, 4, 126]),
            (
                "receipt_type_label",
                pa.string(),
                ["legacy", "eip2930", "eip1559", "eip7702", "deposit"],
            ),
            (
                "status_kind",
                pa.string(),
                ["post_state_root", "eip658", "eip658", "eip658", "eip658"],
            ),
            ("status", pa.bool_(), [None, True, True, False, True]),
            ("post_state_root", pa.binary(32), [b(32, 80), None, None, None, None]),
            (
                "cumulative_gas_used",
                pa.uint64(),
                [21_000, 45_000, 30_000, 50_000, 6_000],
            ),
            ("deposit_nonce", pa.uint64(), [None, None, None, None, 7]),
            ("deposit_receipt_version", pa.uint64(), [None, None, None, None, 1]),
            ("log_count", pa.uint64(), [1, 2, 1, 0, 1]),
        ],
        "flashblock_receipt_logs": [
            ("payload_id", pa.binary(8), [b(8, 1)] * 5),
            ("index", pa.uint64(), [0, 0, 0, 1, 2]),
            ("block_number", pa.uint64(), [100, 100, 100, 100, 100]),
            (
                "transaction_hash",
                pa.binary(32),
                [tx_hashes[0], tx_hashes[1], tx_hashes[1], tx_hashes[2], tx_hashes[4]],
            ),
            ("log_ordinal", pa.uint64(), [0, 0, 1, 0, 0]),
            (
                "address",
                pa.binary(20),
                [b(20, 81), b(20, 82), b(20, 82), b(20, 83), b(20, 84)],
            ),
            ("topic_count", pa.uint64(), [0, 1, 2, 4, 1]),
            (
                "topic_0",
                pa.binary(32),
                [None, b(32, 90), b(32, 91), b(32, 92), b(32, 96)],
            ),
            ("topic_1", pa.binary(32), [None, None, b(32, 93), b(32, 93), None]),
            ("topic_2", pa.binary(32), [None, None, None, b(32, 94), None]),
            ("topic_3", pa.binary(32), [None, None, None, b(32, 95), None]),
            ("data", pa.binary(), [b"", b"a", b"b", b"c", b"d"]),
        ],
        "flashblock_transaction_access_list": [
            ("payload_id", pa.binary(8), [b(8, 1)] * 5),
            ("index", pa.uint64(), [0, 0, 0, 1, 1]),
            ("transaction_ordinal", pa.uint64(), [1, 1, 1, 0, 1]),
            ("access_list_ordinal", pa.uint64(), [0, 0, 1, 0, 0]),
            (
                "address",
                pa.binary(20),
                [b(20, 100), b(20, 100), b(20, 101), b(20, 102), b(20, 103)],
            ),
            ("storage_key_ordinal", pa.uint64(), [0, 1, None, None, 0]),
            (
                "storage_key",
                pa.binary(32),
                [b(32, 101), b(32, 102), None, None, b(32, 103)],
            ),
        ],
        "flashblock_transaction_authorizations": [
            ("payload_id", pa.binary(8), [b(8, 1), b(8, 1)]),
            ("index", pa.uint64(), [1, 1]),
            ("transaction_ordinal", pa.uint64(), [1, 1]),
            ("authorization_ordinal", pa.uint64(), [0, 1]),
            ("chain_id", pa.binary(32), [b(32, 110), b(32, 111)]),
            ("address", pa.binary(20), [b(20, 110), b(20, 111)]),
            ("nonce", pa.uint64(), [10, 11]),
            ("y_parity", pa.uint8(), [0, 1]),
            ("r", pa.binary(32), [b(32, 112), b(32, 113)]),
            ("s", pa.binary(32), [b(32, 114), b(32, 115)]),
        ],
        "flashblock_balance_changes": [
            ("payload_id", pa.binary(8), [b(8, 1), b(8, 1), b(8, 1)]),
            ("index", pa.uint64(), [0, 1, 2]),
            ("block_number", pa.uint64(), [100, 100, 100]),
            ("address", pa.binary(20), [b(20, 120), b(20, 121), b(20, 122)]),
            ("balance", pa.binary(32), [b(32, 120), b(32, 121), b(32, 122)]),
        ],
        "flashblock_withdrawals": [
            ("payload_id", pa.binary(8), [b(8, 1)]),
            ("index", pa.uint64(), [0]),
            ("block_number", pa.uint64(), [100]),
            ("withdrawal_ordinal", pa.uint64(), [0]),
            ("withdrawal_index", pa.uint64(), [7]),
            ("validator_index", pa.uint64(), [9]),
            ("address", pa.binary(20), [b(20, 130)]),
            ("amount", pa.uint64(), [12345]),
        ],
    }


def write_day_tables(
    root: Path,
    current_day: date,
    layout: str = "table",
    hour: int = 16,
    tables: DayTables | None = None,
) -> DayTables:
    """Write one synthetic partition fixture into the requested storage layout."""
    rendered = deepcopy(
        valid_day_tables(current_day, hour=hour) if tables is None else tables
    )
    for dataset, columns in rendered.items():
        path = dataset_file_path(root, dataset, current_day, layout, hour)
        write_parquet(path, dataset, columns)
    return rendered


def write_unknown_dataset(
    root: Path,
    dataset: str,
    current_day: date,
    layout: str = "table",
    hour: int = 16,
) -> Path:
    """Write one unsupported parquet dataset into an otherwise supported layout."""
    path = dataset_file_path(root, dataset, current_day, layout, hour)
    write_raw_parquet(
        path,
        pa.table(
            {
                "marker": pa.array([1], type=pa.uint64()),
            }
        ),
    )
    return path


def _matching_row_indexes(
    columns: MutableTableData, source_payload_id: bytes, included_index_set: set[int]
) -> list[int]:
    payload_ids = columns["payload_id"][1]
    indexes = columns["index"][1]
    return [
        row_index
        for row_index, payload_id in enumerate(payload_ids)
        if payload_id == source_payload_id
        and int(indexes[row_index]) in included_index_set
    ]
