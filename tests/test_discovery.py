from __future__ import annotations

from collections.abc import Callable
from datetime import date
import os
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from flash_dataset.errors import ValidatorConfigurationError
from flash_dataset.validator import build_file_entry
from flash_dataset.validator import parse_storage_path
from flash_dataset.validator import ValidatorConfig
from flash_dataset.validator.common import ValidationResult
import flash_dataset.validator.discovery as discovery_module
from flash_dataset.validator.discovery import discover_files
from flash_dataset.validator.discovery import validate_runtime_config

from tests.helpers import validator_config
from tests.helpers import date_partition_file
from tests.helpers import hour_partition_file
from tests.helpers import table_partition_file
from tests.helpers import write_day_tables


class DiscoveryTests(unittest.TestCase):
    def test_parse_storage_path_supports_table_partition_layout(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current_day = date(2026, 4, 5)
            write_day_tables(root, current_day, layout="table")

            path = table_partition_file(root, "flashblocks", current_day)
            parsed = parse_storage_path(path, root)

            self.assertIsNotNone(parsed)
            assert parsed is not None
            self.assertEqual(parsed.dataset, "flashblocks")
            self.assertEqual(parsed.partition.partition_date, current_day)
            self.assertIsNone(parsed.partition.partition_hour)
            self.assertEqual(parsed.layout, "table_partition")

    def test_parse_storage_path_supports_date_partition_layout(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current_day = date(2026, 4, 5)
            write_day_tables(root, current_day, layout="date")

            path = date_partition_file(root, "flashblocks", current_day)
            parsed = parse_storage_path(path, root)

            self.assertIsNotNone(parsed)
            assert parsed is not None
            self.assertEqual(parsed.dataset, "flashblocks")
            self.assertEqual(parsed.partition.partition_date, current_day)
            self.assertIsNone(parsed.partition.partition_hour)
            self.assertEqual(parsed.layout, "date_partition")

    def test_parse_storage_path_supports_hour_partition_layout(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current_day = date(2026, 4, 5)
            write_day_tables(root, current_day, layout="hour", hour=16)

            path = hour_partition_file(root, "flashblocks", current_day, 16)
            parsed = parse_storage_path(path, root)

            self.assertIsNotNone(parsed)
            assert parsed is not None
            self.assertEqual(parsed.dataset, "flashblocks")
            self.assertEqual(parsed.partition.partition_date, current_day)
            self.assertEqual(parsed.partition.partition_hour, 16)
            self.assertEqual(parsed.layout, "hour_partition")

    def test_build_file_entry_reports_invalid_parquet(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            current_day = date(2026, 4, 5)
            path = table_partition_file(root, "flashblocks", current_day)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"not parquet")

            outcome = build_file_entry(path, root)

            self.assertIsNone(outcome.entry)
            self.assertIsNotNone(outcome.warning)
            assert outcome.warning is not None
            self.assertEqual(outcome.warning.rule_id, "invalid_parquet_file")

    def test_build_file_entry_reports_partition_parse_error(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bad_path = (
                root
                / "flashblocks"
                / "year=2026"
                / "month=04"
                / "day=xx"
                / "flashblocks_2026-04-05.parquet"
            )
            bad_path.parent.mkdir(parents=True, exist_ok=True)
            bad_path.write_bytes(b"still not parsed")

            outcome = build_file_entry(bad_path, root)

            self.assertIsNone(outcome.entry)
            self.assertIsNotNone(outcome.warning)
            assert outcome.warning is not None
            self.assertEqual(outcome.warning.rule_id, "partition_parse_error")

    def test_build_file_entry_rejects_invalid_filename_day_in_all_layouts(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            paths = (
                root / "date=2026-04-05" / "flashblocks_2026-02-30.parquet",
                root
                / "flashblocks"
                / "year=2026"
                / "month=04"
                / "day=05"
                / "flashblocks_2026-02-30.parquet",
                root
                / "year=2026"
                / "month=04"
                / "day=05"
                / "hour=16"
                / "flashblocks_2026-02-30_16.parquet",
            )

            for path in paths:
                outcome = build_file_entry(path, root)

                self.assertIsNone(outcome.entry)
                self.assertIsNotNone(outcome.warning)
                assert outcome.warning is not None
                self.assertEqual(outcome.warning.rule_id, "partition_parse_error")

    def test_parse_storage_path_rejects_extra_table_layout_segments(self) -> None:
        root = Path("/tmp/storage")
        path = (
            root
            / "flashblocks"
            / "year=2026"
            / "month=04"
            / "day=05"
            / "nested"
            / "flashblocks_2026-04-05.parquet"
        )

        parsed = parse_storage_path(path, root)

        self.assertIsNone(parsed)

    def test_parse_storage_path_rejects_table_filename_dataset_mismatch(self) -> None:
        root = Path("/tmp/storage")
        path = (
            root
            / "flashblocks"
            / "year=2026"
            / "month=04"
            / "day=05"
            / "flashblock_receipts_2026-04-05.parquet"
        )

        parsed = parse_storage_path(path, root)

        self.assertIsNone(parsed)

    def test_parse_storage_path_rejects_hour_out_of_range(self) -> None:
        root = Path("/tmp/storage")
        path = (
            root
            / "year=2026"
            / "month=04"
            / "day=05"
            / "hour=99"
            / "flashblocks_2026-04-05_99.parquet"
        )

        parsed = parse_storage_path(path, root)

        self.assertIsNone(parsed)

    def test_validate_runtime_config_rejects_date_range_inversion(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            config = validator_config(root)
            config = config.__class__(
                storage_root=config.storage_root,
                output_directory=config.output_directory,
                date_from=date(2026, 4, 6),
                date_to=date(2026, 4, 5),
                verify_transaction_hashes=config.verify_transaction_hashes,
            )

            with self.assertRaisesRegex(
                ValidatorConfigurationError, "date_from must be on or before date_to"
            ):
                validate_runtime_config(config)

    def test_discover_files_skips_footer_reads_for_filtered_days(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            write_day_tables(root, date(2026, 4, 6), layout="hour")
            config = ValidatorConfig(
                storage_root=root,
                output_directory=root / "reports",
                date_from=date(2026, 4, 6),
                date_to=date(2026, 4, 6),
                verify_transaction_hashes=False,
            )
            footer_reads: list[str] = []
            real_parquet_file = discovery_module.pq.ParquetFile

            def counted_parquet_file(path: Path) -> object:
                footer_reads.append(Path(path).name)
                return real_parquet_file(path)

            with patch.object(
                discovery_module.pq,
                "ParquetFile",
                side_effect=counted_parquet_file,
            ):
                entries = discover_files(config, ValidationResult())

            self.assertEqual(len(entries), 8)
            self.assertEqual(len(footer_reads), 8)
            self.assertTrue(
                all("_2026-04-06_" in file_name for file_name in footer_reads)
            )

    def test_discover_files_skips_full_tree_walk_for_filtered_days(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            write_day_tables(root, date(2026, 4, 6), layout="hour")
            config = ValidatorConfig(
                storage_root=root,
                output_directory=root / "reports",
                date_from=date(2026, 4, 6),
                date_to=date(2026, 4, 6),
                verify_transaction_hashes=False,
            )

            visited_roots: list[Path] = []
            real_walk = discovery_module.os.walk

            def counted_walk(
                top: str | os.PathLike[str],
                topdown: bool = True,
                onerror: Callable[[OSError], None] | None = None,
                followlinks: bool = False,
            ) -> object:
                for root_str, directory_names, file_names in real_walk(
                    top,
                    topdown=topdown,
                    onerror=onerror,
                    followlinks=followlinks,
                ):
                    visited_roots.append(Path(root_str).relative_to(root))
                    yield root_str, directory_names, file_names

            with patch.object(discovery_module.os, "walk", side_effect=counted_walk):
                entries = discover_files(config, ValidationResult())

            self.assertEqual(len(entries), 8)
            self.assertNotIn(
                Path("year=2026/month=04/day=05"),
                visited_roots,
            )

    def test_discover_files_reports_invalid_day_directory_with_filters(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bad_path = (
                root
                / "year=2026"
                / "month=04"
                / "day=xx"
                / "hour=16"
                / "flashblocks_2026-04-06_16.parquet"
            )
            bad_path.parent.mkdir(parents=True, exist_ok=True)
            bad_path.write_bytes(b"bad path")
            config = ValidatorConfig(
                storage_root=root,
                output_directory=root / "reports",
                date_from=date(2026, 4, 6),
                date_to=date(2026, 4, 6),
                verify_transaction_hashes=False,
            )

            result = ValidationResult()
            entries = discover_files(config, result)

            self.assertEqual(entries, [])
            self.assertEqual(
                [finding.rule_id for finding in result.findings],
                ["partition_parse_error"],
            )

    def test_discover_files_reports_invalid_date_partition_with_filters(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            bad_path = root / "date=bad" / "flashblocks_2026-04-06.parquet"
            bad_path.parent.mkdir(parents=True, exist_ok=True)
            bad_path.write_bytes(b"bad path")
            config = ValidatorConfig(
                storage_root=root,
                output_directory=root / "reports",
                date_from=date(2026, 4, 6),
                date_to=date(2026, 4, 6),
                verify_transaction_hashes=False,
            )

            result = ValidationResult()
            entries = discover_files(config, result)

            self.assertEqual(entries, [])
            self.assertEqual(
                [finding.rule_id for finding in result.findings],
                ["partition_parse_error"],
            )

    def test_discover_files_ignores_top_level_tmp_directory(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_day_tables(root, date(2026, 4, 6), layout="hour")
            write_day_tables(root / "tmp", date(2026, 4, 6), layout="hour")
            config = ValidatorConfig(
                storage_root=root,
                output_directory=root / "reports",
                date_from=None,
                date_to=None,
                verify_transaction_hashes=False,
            )

            result = ValidationResult()
            entries = discover_files(config, result)

            self.assertEqual(len(entries), 8)
            self.assertEqual(result.findings, [])
            self.assertTrue(
                all(entry.path.relative_to(root).parts[0] != "tmp" for entry in entries)
            )

    def test_discover_files_raises_when_storage_scan_is_unreadable(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            config = ValidatorConfig(
                storage_root=root,
                output_directory=root / "reports",
                date_from=date(2026, 4, 6),
                date_to=date(2026, 4, 6),
                verify_transaction_hashes=False,
            )

            def unreadable_walk(*args: object, **kwargs: object) -> object:
                onerror = kwargs["onerror"]
                assert callable(onerror)
                onerror(PermissionError("permission denied"))
                return iter(())

            with patch.object(discovery_module.os, "walk", side_effect=unreadable_walk):
                with self.assertRaisesRegex(
                    ValidatorConfigurationError, "failed to scan storage root"
                ):
                    discover_files(config, ValidationResult())

    def test_validate_runtime_config_rejects_unwritable_output_directory(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            root.mkdir()
            locked_parent = Path(directory) / "locked"
            locked_parent.mkdir()
            os.chmod(locked_parent, 0o555)
            config = ValidatorConfig(
                storage_root=root,
                output_directory=locked_parent / "reports",
                date_from=None,
                date_to=None,
                verify_transaction_hashes=False,
            )

            try:
                with self.assertRaisesRegex(
                    ValidatorConfigurationError, "output directory is not writable"
                ):
                    validate_runtime_config(config)
            finally:
                os.chmod(locked_parent, 0o755)


if __name__ == "__main__":
    unittest.main()
