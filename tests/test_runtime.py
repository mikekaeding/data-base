from __future__ import annotations

from datetime import UTC
from datetime import date
from datetime import datetime
import os
from pathlib import Path
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory
import textwrap
import unittest
from unittest.mock import patch

from flash_dataset.errors import ValidatorConfigurationError
from flash_dataset.runtime import discover_day_partitions
from flash_dataset.runtime import next_scheduled_run
from flash_dataset.runtime import parse_daily_utc_time
from flash_dataset.runtime import run_once
from flash_dataset.runtime import run_scheduled
from flash_dataset.runtime import RuntimeConfig

from tests.helpers import valid_day_tables
from tests.helpers import write_day_tables


class RuntimeTests(unittest.TestCase):
    def test_discover_day_partitions_returns_sorted_days(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_day_tables(root, date(2026, 4, 6), layout="hour")
            write_day_tables(root, date(2026, 4, 5), layout="hour")

            partitions = discover_day_partitions(root)

            self.assertEqual(
                [partition.partition_date.isoformat() for partition in partitions],
                ["2026-04-05", "2026-04-06"],
            )

    def test_run_once_updates_watermark_to_latest_reviewable_day(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            write_day_tables(root, date(2026, 4, 6), layout="hour")

            summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=1,
                )
            )

            self.assertEqual(summary.status, "validated")
            self.assertEqual(summary.reviewed_day_count, 2)
            self.assertEqual(summary.latest_reviewed_day, "2026-04-06")
            self.assertEqual(
                (working_directory / "latest-reviewed.txt").read_text(encoding="utf-8"),
                "2026-04-06\n",
            )
            self.assertTrue(
                (working_directory / "reports" / "latest" / "summary.md").is_file()
            )
            self.assertTrue(
                (working_directory / "reports" / "latest" / "findings.jsonl").is_file()
            )

    def test_run_once_rejects_date_partition_storage_for_runtime(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="date")

            with self.assertRaisesRegex(
                ValidatorConfigurationError,
                "run-daily requires top-level year=YYYY/month=MM/day=DD storage layout",
            ):
                run_once(
                    RuntimeConfig(
                        working_directory=working_directory,
                        storage_directory=root,
                        max_days_back=0,
                    )
                )

    def test_run_once_rejects_dataset_first_storage_for_runtime(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="table")

            with self.assertRaisesRegex(
                ValidatorConfigurationError,
                "run-daily requires top-level year=YYYY/month=MM/day=DD storage layout",
            ):
                run_once(
                    RuntimeConfig(
                        working_directory=working_directory,
                        storage_directory=root,
                        max_days_back=0,
                    )
                )

    def test_run_once_rejects_unknown_top_level_runtime_root(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            path = (
                root
                / "weird_dataset"
                / "year=2026"
                / "month=04"
                / "day=05"
                / "weird_dataset_2026-04-05.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"runtime should not ignore this tree")

            with self.assertRaisesRegex(
                ValidatorConfigurationError,
                "unsupported top-level directory",
            ):
                run_once(
                    RuntimeConfig(
                        working_directory=working_directory,
                        storage_directory=root,
                        max_days_back=0,
                    )
                )

    def test_run_once_rejects_top_level_runtime_file(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            root.mkdir(parents=True, exist_ok=True)
            (root / "junk.parquet").write_bytes(b"runtime should not ignore files")

            with self.assertRaisesRegex(
                ValidatorConfigurationError,
                "unsupported top-level entry",
            ):
                run_once(
                    RuntimeConfig(
                        working_directory=working_directory,
                        storage_directory=root,
                        max_days_back=0,
                    )
                )

    def test_run_once_rejects_invalid_runtime_day_directory(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            invalid_day = root / "year=2026" / "month=04" / "day=xx"
            invalid_day.mkdir(parents=True, exist_ok=True)

            with self.assertRaisesRegex(
                ValidatorConfigurationError, "invalid runtime day directory"
            ):
                run_once(
                    RuntimeConfig(
                        working_directory=working_directory,
                        storage_directory=root,
                        max_days_back=0,
                    )
                )

    def test_run_once_rejects_invalid_runtime_month_directory(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            invalid_path = (
                root / "year=2026" / "oops" / "day=05" / "hour=16" / "junk.parquet"
            )
            invalid_path.parent.mkdir(parents=True, exist_ok=True)
            invalid_path.write_bytes(b"runtime should not ignore malformed month trees")

            with self.assertRaisesRegex(
                ValidatorConfigurationError, "invalid runtime month directory"
            ):
                run_once(
                    RuntimeConfig(
                        working_directory=working_directory,
                        storage_directory=root,
                        max_days_back=0,
                    )
                )

    def test_run_once_does_not_advance_watermark_to_empty_newer_day(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            empty_day = root / "year=2026" / "month=04" / "day=06"
            empty_day.mkdir(parents=True, exist_ok=True)

            summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=1,
                )
            )

            self.assertEqual(summary.status, "blocked")
            self.assertEqual(summary.blocked_day, "2026-04-06")
            self.assertEqual(summary.pending_day_count, 2)
            self.assertEqual(summary.reviewed_day_count, 1)
            self.assertEqual(summary.latest_reviewed_day, "2026-04-05")
            self.assertEqual(summary.exit_code(), 1)

    def test_run_once_stops_before_later_day_when_earlier_pending_day_is_empty(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 7), layout="hour")
            empty_day = root / "year=2026" / "month=04" / "day=06"
            empty_day.mkdir(parents=True, exist_ok=True)
            working_directory.mkdir(parents=True, exist_ok=True)
            (working_directory / "latest-reviewed.txt").write_text(
                "2026-04-05\n", encoding="utf-8"
            )

            summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                )
            )

            self.assertEqual(summary.status, "blocked")
            self.assertEqual(summary.blocked_day, "2026-04-06")
            self.assertEqual(summary.pending_day_count, 2)
            self.assertEqual(summary.reviewed_day_count, 0)
            self.assertEqual(summary.latest_reviewed_day, "2026-04-05")
            self.assertEqual(
                (working_directory / "latest-reviewed.txt").read_text(encoding="utf-8"),
                "2026-04-05\n",
            )
            self.assertIn(
                "blocked by an earlier pending day",
                (Path(summary.report_directory) / "summary.md").read_text(
                    encoding="utf-8"
                ),
            )

    def test_run_once_does_not_advance_watermark_when_validation_fails(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            tables = valid_day_tables(date(2026, 4, 6))
            tables.pop("flashblock_receipts")
            write_day_tables(root, date(2026, 4, 6), layout="hour", tables=tables)
            working_directory.mkdir(parents=True, exist_ok=True)
            (working_directory / "latest-reviewed.txt").write_text(
                "2026-04-05\n", encoding="utf-8"
            )

            summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                )
            )

            self.assertEqual(summary.status, "blocked")
            self.assertEqual(summary.blocked_day, "2026-04-06")
            self.assertTrue(summary.has_failures)
            self.assertEqual(summary.reviewed_day_count, 0)
            self.assertEqual(summary.latest_reviewed_day, "2026-04-05")
            self.assertEqual(
                (working_directory / "latest-reviewed.txt").read_text(encoding="utf-8"),
                "2026-04-05\n",
            )
            self.assertTrue((Path(summary.report_directory) / "summary.md").is_file())

    def test_run_once_rolls_back_new_watermark_when_runtime_report_write_fails(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")

            with patch(
                "flash_dataset.runtime._write_runtime_report",
                side_effect=ValidatorConfigurationError("report write failed"),
            ):
                with self.assertRaisesRegex(
                    ValidatorConfigurationError, "report write failed"
                ):
                    run_once(
                        RuntimeConfig(
                            working_directory=working_directory,
                            storage_directory=root,
                            max_days_back=0,
                        )
                    )

            self.assertFalse((working_directory / "latest-reviewed.txt").exists())

    def test_run_once_restores_previous_watermark_when_latest_report_promotion_fails(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 6), layout="hour")
            working_directory.mkdir(parents=True, exist_ok=True)
            (working_directory / "latest-reviewed.txt").write_text(
                "2026-04-05\n", encoding="utf-8"
            )

            with patch(
                "flash_dataset.runtime._promote_latest_report",
                side_effect=ValidatorConfigurationError(
                    "latest report promotion failed"
                ),
            ):
                with self.assertRaisesRegex(
                    ValidatorConfigurationError, "latest report promotion failed"
                ):
                    run_once(
                        RuntimeConfig(
                            working_directory=working_directory,
                            storage_directory=root,
                            max_days_back=0,
                        )
                    )

            self.assertEqual(
                (working_directory / "latest-reviewed.txt").read_text(encoding="utf-8"),
                "2026-04-05\n",
            )

    def test_run_once_does_not_write_watermark_when_runtime_report_write_is_interrupted(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")

            with patch(
                "flash_dataset.runtime._write_runtime_report",
                side_effect=KeyboardInterrupt("report interrupted"),
            ):
                with self.assertRaisesRegex(KeyboardInterrupt, "report interrupted"):
                    run_once(
                        RuntimeConfig(
                            working_directory=working_directory,
                            storage_directory=root,
                            max_days_back=0,
                        )
                    )

            self.assertFalse((working_directory / "latest-reviewed.txt").exists())
            self.assertFalse(
                (working_directory / "reports" / "latest" / "summary.md").exists()
            )

    def test_run_once_does_not_write_watermark_when_process_is_killed_during_report_write(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            project_root = Path(__file__).resolve().parents[1]
            script = textwrap.dedent(
                f"""
                import os
                import signal
                from pathlib import Path
                from unittest.mock import patch

                from flash_dataset.runtime import RuntimeConfig
                from flash_dataset.runtime import run_once

                def kill_report(*args, **kwargs):
                    os.kill(os.getpid(), signal.SIGKILL)

                with patch(
                    "flash_dataset.runtime._write_runtime_report",
                    side_effect=kill_report,
                ):
                    run_once(
                        RuntimeConfig(
                            working_directory=Path({str(working_directory)!r}),
                            storage_directory=Path({str(root)!r}),
                            max_days_back=0,
                        )
                    )
                """
            )

            completed = subprocess.run(
                [sys.executable, "-c", script],
                cwd=project_root,
                env={**os.environ, "PYTHONPATH": "src"},
                check=False,
            )

            self.assertEqual(completed.returncode, -signal.SIGKILL)
            self.assertFalse((working_directory / "latest-reviewed.txt").exists())

    def test_run_once_keeps_latest_report_pinned_when_process_is_killed_during_state_write(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=0,
                )
            )
            write_day_tables(root, date(2026, 4, 6), layout="hour")
            project_root = Path(__file__).resolve().parents[1]
            script = textwrap.dedent(
                f"""
                import os
                import signal
                from pathlib import Path
                from unittest.mock import patch

                from flash_dataset.runtime import RuntimeConfig
                from flash_dataset.runtime import run_once

                def kill_state_write(*args, **kwargs):
                    os.kill(os.getpid(), signal.SIGKILL)

                with patch(
                    "flash_dataset.runtime.store_latest_reviewed_day",
                    side_effect=kill_state_write,
                ):
                    run_once(
                        RuntimeConfig(
                            working_directory=Path({str(working_directory)!r}),
                            storage_directory=Path({str(root)!r}),
                            max_days_back=0,
                        )
                    )
                """
            )

            completed = subprocess.run(
                [sys.executable, "-c", script],
                cwd=project_root,
                env={**os.environ, "PYTHONPATH": "src"},
                check=False,
            )

            self.assertEqual(completed.returncode, -signal.SIGKILL)
            summary_text = (
                working_directory / "reports" / "latest" / "summary.md"
            ).read_text(encoding="utf-8")
            self.assertIn("- latest reviewed day: `2026-04-05`", summary_text)
            self.assertNotIn("- latest reviewed day: `2026-04-06`", summary_text)
            self.assertEqual(
                (working_directory / "latest-reviewed.txt").read_text(encoding="utf-8"),
                "2026-04-05\n",
            )

    def test_run_once_keeps_latest_report_pinned_when_state_write_fails(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=0,
                )
            )
            write_day_tables(root, date(2026, 4, 6), layout="hour")

            with patch(
                "flash_dataset.runtime.store_latest_reviewed_day",
                side_effect=ValidatorConfigurationError("state write failed"),
            ):
                with self.assertRaisesRegex(
                    ValidatorConfigurationError, "state write failed"
                ):
                    run_once(
                        RuntimeConfig(
                            working_directory=working_directory,
                            storage_directory=root,
                            max_days_back=0,
                        )
                    )

            summary_text = (
                working_directory / "reports" / "latest" / "summary.md"
            ).read_text(encoding="utf-8")
            self.assertIn("- latest reviewed day: `2026-04-05`", summary_text)
            self.assertNotIn("- latest reviewed day: `2026-04-06`", summary_text)
            self.assertEqual(
                (working_directory / "latest-reviewed.txt").read_text(encoding="utf-8"),
                "2026-04-05\n",
            )

    def test_run_once_waits_for_missing_next_day_before_advancing_watermark(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 7), layout="hour")
            working_directory.mkdir(parents=True, exist_ok=True)
            (working_directory / "latest-reviewed.txt").write_text(
                "2026-04-05\n", encoding="utf-8"
            )

            first_summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                )
            )

            self.assertEqual(first_summary.status, "blocked")
            self.assertEqual(first_summary.blocked_day, "2026-04-06")
            self.assertEqual(first_summary.reviewed_day_count, 0)
            self.assertEqual(first_summary.latest_reviewed_day, "2026-04-05")

            write_day_tables(root, date(2026, 4, 6), layout="hour")

            second_summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                )
            )

            self.assertEqual(second_summary.status, "validated")
            self.assertIsNone(second_summary.blocked_day)
            self.assertEqual(second_summary.reviewed_day_count, 2)
            self.assertEqual(second_summary.latest_reviewed_day, "2026-04-07")

    def test_run_once_does_not_store_watermark_when_day_has_only_parse_warnings(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            path = (
                root
                / "year=2026"
                / "month=04"
                / "day=05"
                / "hour=16"
                / "nested"
                / "junk.parquet"
            )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(b"bad path")

            summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=0,
                )
            )

            self.assertEqual(summary.status, "blocked")
            self.assertEqual(summary.blocked_day, "2026-04-05")
            self.assertEqual(summary.reviewed_day_count, 0)
            self.assertEqual(summary.reviewed_partition_count, 0)
            self.assertEqual(summary.warn_count, 1)
            self.assertEqual(summary.rule_counts, {"partition_parse_error": 1})
            self.assertIsNone(summary.latest_reviewed_day)
            self.assertEqual(summary.exit_code(), 1)
            self.assertFalse((working_directory / "latest-reviewed.txt").exists())
            summary_text = (Path(summary.report_directory) / "summary.md").read_text(
                encoding="utf-8"
            )
            self.assertIn(
                "- status: `blocked by a selected day that did not validate cleanly`",
                summary_text,
            )
            self.assertIn("- blocked day: `2026-04-05`", summary_text)
            self.assertIn("- `partition_parse_error`: `1`", summary_text)
            self.assertIn(
                "- status: `blocked by a selected day that did not validate cleanly`",
                (working_directory / "reports" / "latest" / "summary.md").read_text(
                    encoding="utf-8"
                ),
            )

    def test_run_once_blocks_day_when_one_partition_warns_and_another_reviews(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour", hour=16)
            bad_path = (
                root
                / "year=2026"
                / "month=04"
                / "day=05"
                / "hour=16"
                / "nested"
                / "junk.parquet"
            )
            bad_path.parent.mkdir(parents=True, exist_ok=True)
            bad_path.write_bytes(b"bad path")

            summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=0,
                )
            )

            self.assertEqual(summary.status, "blocked")
            self.assertEqual(summary.blocked_day, "2026-04-05")
            self.assertEqual(summary.reviewed_day_count, 1)
            self.assertEqual(summary.reviewed_partition_count, 1)
            self.assertEqual(summary.warn_count, 1)
            self.assertIsNone(summary.latest_reviewed_day)
            self.assertFalse((working_directory / "latest-reviewed.txt").exists())

    def test_run_once_blocks_day_when_later_partition_in_same_day_fails(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour", hour=16)
            bad_tables = valid_day_tables(date(2026, 4, 5), hour=17)
            bad_tables.pop("flashblock_receipts")
            write_day_tables(
                root,
                date(2026, 4, 5),
                layout="hour",
                hour=17,
                tables=bad_tables,
            )

            summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=0,
                )
            )

            self.assertEqual(summary.status, "blocked")
            self.assertEqual(summary.blocked_day, "2026-04-05")
            self.assertTrue(summary.has_failures)
            self.assertEqual(summary.fail_count, 1)
            self.assertEqual(summary.reviewed_day_count, 1)
            self.assertEqual(summary.reviewed_partition_count, 1)
            self.assertIsNone(summary.latest_reviewed_day)
            self.assertFalse((working_directory / "latest-reviewed.txt").exists())

    def test_run_once_does_not_store_initial_watermark_before_window_start_exists(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 7), layout="hour")

            summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                )
            )

            self.assertEqual(summary.status, "validated")
            self.assertIsNone(summary.blocked_day)
            self.assertEqual(summary.reviewed_day_count, 1)
            self.assertIsNone(summary.latest_reviewed_day)
            self.assertFalse((working_directory / "latest-reviewed.txt").exists())
            self.assertFalse((working_directory / "latest-reviewed.txt").is_symlink())
            self.assertFalse(
                (
                    working_directory / "reports" / "latest" / "latest-reviewed.txt"
                ).exists()
            )

    def test_run_once_rejects_dangling_latest_reviewed_symlink(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=0,
                )
            )
            (working_directory / "reports" / "latest" / "current").unlink()

            with self.assertRaisesRegex(
                ValidatorConfigurationError,
                "latest reviewed day is a dangling symlink",
            ):
                run_once(
                    RuntimeConfig(
                        working_directory=working_directory,
                        storage_directory=root,
                        max_days_back=0,
                    )
                )

    def test_run_once_rejects_future_latest_reviewed_day(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            working_directory.mkdir(parents=True, exist_ok=True)
            (working_directory / "latest-reviewed.txt").write_text(
                "2099-01-01\n", encoding="utf-8"
            )

            with self.assertRaisesRegex(
                ValidatorConfigurationError,
                "latest reviewed day is after newest discovered day",
            ):
                run_once(
                    RuntimeConfig(
                        working_directory=working_directory,
                        storage_directory=root,
                        max_days_back=0,
                    )
                )

    def test_run_once_clamps_expected_day_to_oldest_allowed_window_day(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            working_directory.mkdir(parents=True, exist_ok=True)
            (working_directory / "latest-reviewed.txt").write_text(
                "2026-04-05\n", encoding="utf-8"
            )
            for day_number in (8, 9, 10):
                write_day_tables(root, date(2026, 4, day_number), layout="hour")

            summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=2,
                )
            )

            self.assertEqual(summary.status, "validated")
            self.assertIsNone(summary.blocked_day)
            self.assertEqual(summary.reviewed_day_count, 3)
            self.assertEqual(summary.latest_reviewed_day, "2026-04-10")
            self.assertEqual(
                (working_directory / "latest-reviewed.txt").read_text(encoding="utf-8"),
                "2026-04-10\n",
            )

    def test_run_once_ignores_days_older_than_max_days_back(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            write_day_tables(root, date(2026, 4, 6), layout="hour")
            write_day_tables(root, date(2026, 4, 7), layout="hour")
            write_day_tables(root, date(2026, 4, 8), layout="hour")

            summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=1,
                )
            )

            self.assertEqual(summary.status, "validated")
            self.assertIsNone(summary.blocked_day)
            self.assertEqual(summary.pending_day_count, 2)
            self.assertEqual(summary.reviewed_day_count, 2)
            self.assertEqual(summary.latest_reviewed_day, "2026-04-08")
            self.assertEqual(
                (working_directory / "latest-reviewed.txt").read_text(encoding="utf-8"),
                "2026-04-08\n",
            )

    def test_next_scheduled_run_rolls_forward_after_today_slot(self) -> None:
        run_at = parse_daily_utc_time("2:30AM")
        now = datetime(2026, 4, 13, 3, 0, tzinfo=UTC)

        scheduled_run = next_scheduled_run(now, run_at)

        self.assertEqual(
            scheduled_run,
            datetime(2026, 4, 14, 2, 30, tzinfo=UTC),
        )

    def test_next_scheduled_run_runs_immediately_at_exact_slot(self) -> None:
        run_at = parse_daily_utc_time("2:30AM")
        now = datetime(2026, 4, 13, 2, 30, tzinfo=UTC)

        scheduled_run = next_scheduled_run(now, run_at)

        self.assertEqual(
            scheduled_run,
            datetime(2026, 4, 13, 2, 30, tzinfo=UTC),
        )

    def test_run_once_uses_unique_report_directory_when_same_second_repeats(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            run_time = datetime(2026, 4, 13, 12, 0, 0, tzinfo=UTC)
            write_day_tables(root, date(2026, 4, 5), layout="hour")

            first_summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=0,
                ),
                run_time=run_time,
            )
            second_summary = run_once(
                RuntimeConfig(
                    working_directory=working_directory,
                    storage_directory=root,
                    max_days_back=0,
                ),
                run_time=run_time,
            )

            self.assertNotEqual(
                first_summary.report_directory, second_summary.report_directory
            )
            self.assertTrue(
                (Path(first_summary.report_directory) / "summary.md").is_file()
            )
            self.assertTrue(
                (Path(second_summary.report_directory) / "summary.md").is_file()
            )

    def test_run_once_rejects_unwritable_working_directory(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            locked_parent = Path(directory) / "locked"
            locked_parent.mkdir()
            os.chmod(locked_parent, 0o555)
            config = RuntimeConfig(
                working_directory=locked_parent / "runtime",
                storage_directory=root,
                run_at=None,
            )

            try:
                with self.assertRaisesRegex(
                    ValidatorConfigurationError, "working directory is not writable"
                ):
                    run_once(config)
            finally:
                os.chmod(locked_parent, 0o755)

    def test_run_once_rejects_unreadable_latest_reviewed_state(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            working_directory.mkdir(parents=True, exist_ok=True)
            state_path = working_directory / "latest-reviewed.txt"
            state_path.write_text("2026-04-04\n", encoding="utf-8")
            os.chmod(state_path, 0)

            try:
                with self.assertRaisesRegex(
                    ValidatorConfigurationError, "failed to read latest reviewed day"
                ):
                    run_once(
                        RuntimeConfig(
                            working_directory=working_directory,
                            storage_directory=root,
                            max_days_back=0,
                        )
                    )
            finally:
                os.chmod(state_path, 0o644)

    def test_run_once_rejects_unreadable_storage_tree(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            unreadable_year = root / "year=2026"
            unreadable_year.mkdir(parents=True, exist_ok=True)
            os.chmod(unreadable_year, 0)

            try:
                with self.assertRaisesRegex(
                    ValidatorConfigurationError, "failed to scan directory"
                ):
                    run_once(
                        RuntimeConfig(
                            working_directory=working_directory,
                            storage_directory=root,
                            max_days_back=0,
                        )
                    )
            finally:
                os.chmod(unreadable_year, 0o755)

    def test_run_once_rejects_unreadable_pending_day_scan(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            unreadable_hour = root / "year=2026" / "month=04" / "day=05" / "hour=16"
            unreadable_hour.mkdir(parents=True, exist_ok=True)
            os.chmod(unreadable_hour, 0)

            try:
                with self.assertRaisesRegex(
                    ValidatorConfigurationError, "failed to scan day directory"
                ):
                    run_once(
                        RuntimeConfig(
                            working_directory=working_directory,
                            storage_directory=root,
                            max_days_back=0,
                        )
                    )
            finally:
                os.chmod(unreadable_hour, 0o755)

    def test_run_scheduled_validates_config_before_first_sleep(self) -> None:
        with TemporaryDirectory() as directory:
            working_directory = Path(directory) / "runtime"
            sleep_calls: list[float] = []

            def record_sleep(seconds: float) -> None:
                sleep_calls.append(seconds)

            with self.assertRaisesRegex(
                ValidatorConfigurationError, "storage directory does not exist"
            ):
                run_scheduled(
                    RuntimeConfig(
                        working_directory=working_directory,
                        storage_directory=Path(directory) / "missing",
                    ),
                    emit_summary=lambda summary: None,
                    sleep=record_sleep,
                    now_provider=lambda: datetime(2026, 4, 13, 5, 0, tzinfo=UTC),
                )
            self.assertEqual(sleep_calls, [])

    def test_run_scheduled_validates_runtime_state_before_first_sleep(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            working_directory.mkdir(parents=True, exist_ok=True)
            (working_directory / "latest-reviewed.txt").write_text(
                "not-a-date\n", encoding="utf-8"
            )
            sleep_calls: list[float] = []

            def record_sleep(seconds: float) -> None:
                sleep_calls.append(seconds)

            with self.assertRaisesRegex(
                ValidatorConfigurationError, "invalid latest reviewed day"
            ):
                run_scheduled(
                    RuntimeConfig(
                        working_directory=working_directory,
                        storage_directory=root,
                    ),
                    emit_summary=lambda summary: None,
                    sleep=record_sleep,
                    now_provider=lambda: datetime(2026, 4, 13, 5, 0, tzinfo=UTC),
                )
            self.assertEqual(sleep_calls, [])


if __name__ == "__main__":
    unittest.main()
