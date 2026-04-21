from __future__ import annotations

from datetime import date
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from flash_dataset.errors import ValidatorConfigurationError
from flash_dataset.validator import baselines as baseline_module
from flash_dataset.validator.baselines import DistributionSummary
from flash_dataset.validator.baselines import PartitionSnapshot
from flash_dataset.validator import run_validation
from flash_dataset.validator import ValidatorConfig
from flash_dataset.validator.common import PartitionIdentity

from tests.helpers import append_payload_variant
from tests.helpers import mutable_table
from tests.helpers import replace_table
from tests.helpers import valid_day_tables
from tests.helpers import validator_config
from tests.helpers import write_day_tables


class ValidationTests(unittest.TestCase):
    @staticmethod
    def _distribution_summary(values: tuple[int, ...]) -> DistributionSummary:
        if not values:
            return DistributionSummary(0, None, None, None)
        sorted_values = sorted(values)
        middle = len(sorted_values) // 2
        if len(sorted_values) % 2 == 0:
            median_value = (sorted_values[middle - 1] + sorted_values[middle]) / 2
        else:
            median_value = float(sorted_values[middle])
        return DistributionSummary(
            count=len(sorted_values),
            minimum=sorted_values[0],
            median_value=median_value,
            maximum=sorted_values[-1],
        )

    @classmethod
    def _snapshot(
        cls, current_day: date, payload_lengths: tuple[int, ...]
    ) -> PartitionSnapshot:
        empty_summary = DistributionSummary(0, None, None, None)
        return PartitionSnapshot(
            partition=PartitionIdentity(current_day, 16),
            dataset_snapshots=(),
            empty_datasets=(),
            payload_lengths=payload_lengths,
            payload_length_summary=cls._distribution_summary(payload_lengths),
            tail_only_payload_count=0,
            transaction_count_summary=empty_summary,
            receipt_log_count_summary=empty_summary,
            balance_change_count_summary=empty_summary,
            withdrawal_count_summary=empty_summary,
            transaction_type_counts=(),
            total_transactions=0,
            peer_eligible=True,
        )

    def _config(self, root: Path) -> ValidatorConfig:
        return validator_config(root)

    def test_validation_passes_valid_synthetic_day(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_day_tables(root, date(2026, 4, 5), layout="hour")

            result = run_validation(self._config(root))

            self.assertFalse(result.has_failures())
            self.assertFalse(
                any(finding.status == "warn" for finding in result.findings)
            )
            self.assertTrue(
                any(
                    finding.rule_id == "received_time_slot_coverage"
                    for finding in result.findings
                )
            )
            self.assertTrue(
                any(
                    finding.rule_id == "dataset_physical_stats"
                    for finding in result.findings
                )
            )
            self.assertTrue(
                any(
                    finding.rule_id == "transaction_type_mix"
                    for finding in result.findings
                )
            )

    def test_missing_required_dataset_fails(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            tables.pop("flashblock_receipts")
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            self.assertTrue(result.has_failures())
            self.assertTrue(
                any(
                    finding.rule_id == "missing_required_dataset"
                    for finding in result.findings
                )
            )
            self.assertEqual(result.reviewed_partitions, [])

    def test_orphan_child_row_fails(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            transaction_columns = mutable_table(tables, "flashblock_transactions")
            transaction_columns["payload_id"][1][0] = b"\x99" * 8
            replace_table(tables, "flashblock_transactions", transaction_columns)
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            self.assertTrue(
                any(
                    finding.rule_id == "orphan_child_flashblock"
                    for finding in result.findings
                )
            )

    def test_semantic_shape_failures_are_detected(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))

            flashblock_columns = mutable_table(tables, "flashblocks")
            flashblock_columns["base_block_number"][1][1] = 101
            replace_table(tables, "flashblocks", flashblock_columns)

            transaction_columns = mutable_table(tables, "flashblock_transactions")
            transaction_columns["gas_price"][1][2] = b"\x01" * 16
            replace_table(tables, "flashblock_transactions", transaction_columns)

            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            rule_ids = {finding.rule_id for finding in result.findings}
            self.assertIn("base_fields_present_on_nonzero_index", rule_ids)
            self.assertIn("transaction_optional_field_shape_mismatch", rule_ids)

    def test_all_received_time_slots_missing_warns(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            flashblock_columns = mutable_table(tables, "flashblocks")
            flashblock_columns["received_time_a"][1][0] = None
            flashblock_columns["received_time_b"][1][0] = None
            flashblock_columns["received_time_c"][1][0] = None
            replace_table(tables, "flashblocks", flashblock_columns)
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(validator_config(root))

            self.assertTrue(
                any(
                    finding.rule_id == "all_received_time_slots_missing"
                    for finding in result.findings
                )
            )

    def test_partition_hour_mismatch_warns(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            flashblock_columns = mutable_table(tables, "flashblocks")
            block_timestamps = flashblock_columns["block_timestamp"][1]
            block_timestamps[0] = block_timestamps[0].replace(hour=15)
            replace_table(tables, "flashblocks", flashblock_columns)
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            self.assertTrue(
                any(
                    finding.rule_id == "partition_date_mismatch"
                    for finding in result.findings
                )
            )

    def test_receipt_cumulative_gas_used_backwards_warns(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            receipt_columns = mutable_table(tables, "flashblock_receipts")
            receipt_columns["cumulative_gas_used"][1][1] = 20_000
            replace_table(tables, "flashblock_receipts", receipt_columns)
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            self.assertTrue(
                any(
                    finding.rule_id == "receipt_cumulative_gas_used_not_monotonic"
                    for finding in result.findings
                )
            )

    def test_authorization_non_7702_fails(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            transaction_columns = mutable_table(tables, "flashblock_transactions")
            transaction_columns["transaction_type_label"][1][3] = "eip1559"
            replace_table(tables, "flashblock_transactions", transaction_columns)
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            self.assertTrue(
                any(
                    finding.rule_id == "authorization_non_7702"
                    for finding in result.findings
                )
            )

    def test_invalid_parquet_file_does_not_add_no_matching_failure(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current_day = date(2026, 4, 5)
            path = (
                root
                / f"year={current_day.year:04d}"
                / f"month={current_day.month:02d}"
                / f"day={current_day.day:02d}"
                / "hour=16"
                / f"flashblocks_{current_day.isoformat()}_16.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"not parquet")

            result = run_validation(self._config(root))

            rule_ids = {finding.rule_id for finding in result.findings}
            self.assertIn("invalid_parquet_file", rule_ids)
            self.assertNotIn("no_matching_parquet_files", rule_ids)

    def test_out_of_range_invalid_parquet_is_ignored(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current_day = date(2026, 4, 5)
            path = (
                root
                / f"year={current_day.year:04d}"
                / f"month={current_day.month:02d}"
                / f"day={current_day.day:02d}"
                / "hour=16"
                / f"flashblocks_{current_day.isoformat()}_16.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"not parquet")

            config = self._config(root)
            config = ValidatorConfig(
                storage_root=config.storage_root,
                output_directory=config.output_directory,
                date_from=date(2026, 4, 6),
                date_to=None,
                verify_transaction_hashes=config.verify_transaction_hashes,
            )

            result = run_validation(config)

            rule_ids = {finding.rule_id for finding in result.findings}
            self.assertNotIn("invalid_parquet_file", rule_ids)
            self.assertIn("no_matching_parquet_files", rule_ids)

    def test_filtered_partition_parse_error_does_not_add_no_matching_failure(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            path = (
                root
                / "year=2026"
                / "month=04"
                / "day=xx"
                / "hour=16"
                / "flashblocks_2026-04-06_16.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"bad path")

            result = run_validation(
                ValidatorConfig(
                    storage_root=root,
                    output_directory=root / "reports",
                    date_from=date(2026, 4, 6),
                    date_to=date(2026, 4, 6),
                    verify_transaction_hashes=False,
                )
            )

            rule_ids = {finding.rule_id for finding in result.findings}
            self.assertIn("partition_parse_error", rule_ids)
            self.assertNotIn("no_matching_parquet_files", rule_ids)

    def test_balance_change_orphan_and_block_number_checks_are_enforced(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            balance_columns = mutable_table(tables, "flashblock_balance_changes")
            balance_columns["block_number"][1][0] = 101
            balance_columns["payload_id"][1][1] = b"\x42" * 8
            replace_table(tables, "flashblock_balance_changes", balance_columns)
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            rule_ids = {finding.rule_id for finding in result.findings}
            self.assertIn("child_block_number_mismatch", rule_ids)
            self.assertIn("orphan_child_flashblock", rule_ids)

    def test_unreadable_storage_scan_raises_configuration_error(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)

            def unreadable_walk(*args: object, **kwargs: object) -> object:
                onerror = kwargs["onerror"]
                assert callable(onerror)
                onerror(PermissionError("permission denied"))
                return iter(())

            with patch(
                "flash_dataset.validator.discovery.os.walk", side_effect=unreadable_walk
            ):
                with self.assertRaisesRegex(
                    ValidatorConfigurationError, "failed to scan storage root"
                ):
                    run_validation(self._config(root))

    def test_transaction_rlp_hash_mismatch_fails(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            transaction_columns = mutable_table(tables, "flashblock_transactions")
            transaction_columns["transaction_hash"][1][0] = b"\x10" * 32
            replace_table(tables, "flashblock_transactions", transaction_columns)
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(
                validator_config(root, verify_transaction_hashes=True)
            )

            self.assertTrue(
                any(
                    finding.rule_id == "transaction_rlp_hash_mismatch"
                    for finding in result.findings
                )
            )

    def test_transaction_rlp_hash_mismatch_is_skipped_by_default(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            transaction_columns = mutable_table(tables, "flashblock_transactions")
            transaction_columns["transaction_hash"][1][0] = b"\x10" * 32
            replace_table(tables, "flashblock_transactions", transaction_columns)
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            self.assertFalse(
                any(
                    finding.rule_id == "transaction_rlp_hash_mismatch"
                    for finding in result.findings
                )
            )

    def test_transaction_optional_field_partial_presence_fails(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            transaction_columns = mutable_table(tables, "flashblock_transactions")
            transaction_columns["deposit_source_hash"][1][0] = b"\x01" * 32
            replace_table(tables, "flashblock_transactions", transaction_columns)
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            self.assertTrue(
                any(
                    finding.rule_id == "transaction_optional_field_shape_mismatch"
                    for finding in result.findings
                )
            )

    def test_dataset_physical_drift_requires_peer_partitions(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            append_payload_variant(
                tables,
                new_payload_seed=20,
                new_block_number=200,
            )
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            self.assertFalse(
                any(
                    finding.rule_id == "dataset_physical_drift"
                    for finding in result.findings
                )
            )

    def test_cross_partition_physical_drift_warns(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for day_number in range(1, 4):
                write_day_tables(root, date(2026, 4, day_number), layout="hour")
            tables = valid_day_tables(date(2026, 4, 4))
            append_payload_variant(
                tables,
                new_payload_seed=20,
                new_block_number=200,
            )
            write_day_tables(root, date(2026, 4, 4), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            self.assertTrue(
                any(
                    finding.rule_id == "dataset_physical_drift"
                    for finding in result.findings
                )
            )

    def test_failed_partition_is_excluded_from_drift_peers(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for day_number in range(1, 3):
                write_day_tables(root, date(2026, 4, day_number), layout="hour")

            failed_tables = valid_day_tables(date(2026, 4, 3))
            failed_flashblocks = mutable_table(failed_tables, "flashblocks")
            failed_flashblocks["base_block_number"][1][1] = 101
            replace_table(failed_tables, "flashblocks", failed_flashblocks)
            write_day_tables(
                root, date(2026, 4, 3), layout="hour", tables=failed_tables
            )

            drift_tables = valid_day_tables(date(2026, 4, 4))
            append_payload_variant(
                drift_tables,
                new_payload_seed=20,
                new_block_number=200,
            )
            write_day_tables(root, date(2026, 4, 4), layout="hour", tables=drift_tables)

            result = run_validation(self._config(root))

            self.assertTrue(result.has_failures())
            self.assertFalse(
                any(
                    finding.rule_id == "dataset_physical_drift"
                    for finding in result.findings
                )
            )

    def test_drift_requires_same_hour_peers(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current_day = date(2026, 4, 5)
            for hour in (17, 18, 19):
                write_day_tables(root, current_day, layout="hour", hour=hour)

            drift_tables = valid_day_tables(current_day)
            append_payload_variant(
                drift_tables,
                new_payload_seed=20,
                new_block_number=200,
            )
            write_day_tables(
                root, current_day, layout="hour", hour=16, tables=drift_tables
            )

            result = run_validation(self._config(root))

            rule_ids = {finding.rule_id for finding in result.findings}
            self.assertNotIn("dataset_physical_drift", rule_ids)
            self.assertNotIn("transaction_type_mix_drift", rule_ids)
            self.assertNotIn("partition_date_mismatch", rule_ids)

    def test_warned_partition_is_excluded_from_drift_peers(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for day_number in range(1, 3):
                write_day_tables(root, date(2026, 4, day_number), layout="hour")

            warned_tables = valid_day_tables(date(2026, 4, 3))
            append_payload_variant(
                warned_tables,
                new_payload_seed=20,
                new_block_number=200,
                included_indexes=(1, 2),
            )
            write_day_tables(
                root, date(2026, 4, 3), layout="hour", tables=warned_tables
            )

            drift_tables = valid_day_tables(date(2026, 4, 4))
            append_payload_variant(
                drift_tables,
                new_payload_seed=21,
                new_block_number=201,
            )
            write_day_tables(root, date(2026, 4, 4), layout="hour", tables=drift_tables)

            result = run_validation(self._config(root))

            self.assertTrue(
                any(
                    finding.rule_id == "payload_starts_after_zero"
                    for finding in result.findings
                )
            )
            self.assertFalse(
                any(
                    finding.rule_id == "dataset_physical_drift"
                    for finding in result.findings
                )
            )

    def test_discovery_warning_partition_is_excluded_from_drift_peers(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            for day_number in range(1, 3):
                write_day_tables(root, date(2026, 4, day_number), layout="hour")

            write_day_tables(root, date(2026, 4, 3), layout="hour")
            warned_partition = root / "year=2026" / "month=04" / "day=03" / "hour=16"
            shutil.copyfile(
                warned_partition / "flashblocks_2026-04-03_16.parquet",
                warned_partition / "unknown_dataset_2026-04-03_16.parquet",
            )

            drift_tables = valid_day_tables(date(2026, 4, 4))
            append_payload_variant(
                drift_tables,
                new_payload_seed=20,
                new_block_number=200,
            )
            write_day_tables(root, date(2026, 4, 4), layout="hour", tables=drift_tables)

            result = run_validation(self._config(root))

            rule_ids = {finding.rule_id for finding in result.findings}
            self.assertIn("unknown_dataset", rule_ids)
            self.assertNotIn("dataset_physical_drift", rule_ids)

    def test_payload_length_distribution_uses_interval_span(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            tables = valid_day_tables(date(2026, 4, 5))
            flashblock_columns = mutable_table(tables, "flashblocks")
            flashblock_columns["index"][1][:] = [0, 2, 3]
            replace_table(tables, "flashblocks", flashblock_columns)
            write_day_tables(root, date(2026, 4, 5), layout="hour", tables=tables)

            result = run_validation(self._config(root))

            finding = next(
                finding
                for finding in result.findings
                if finding.rule_id == "payload_run_length_distribution"
            )
            self.assertEqual(
                finding.example,
                {
                    "payload_length_count": 1,
                    "payload_length_min": 4,
                    "payload_length_median": 4.0,
                    "payload_length_max": 4,
                },
            )

    def test_short_payload_warning_partitions_are_not_drift_peers(self) -> None:
        current = self._snapshot(date(2026, 4, 5), (3, 3, 3))
        peer_one = self._snapshot(date(2026, 4, 4), (3, 3, 3))
        peer_two = self._snapshot(date(2026, 4, 3), (3, 3, 3))
        anomalous_peer = self._snapshot(date(2026, 4, 2), (1, 3, 3, 3, 3))

        comparison_snapshots = getattr(baseline_module, "_comparison_snapshots")
        peers, scope = comparison_snapshots(
            current, (current, peer_one, peer_two, anomalous_peer)
        )

        self.assertEqual(peers, ())
        self.assertEqual(scope, "insufficient_same_hour_peers")


if __name__ == "__main__":
    unittest.main()
