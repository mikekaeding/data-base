from __future__ import annotations

import contextlib
from datetime import date
from io import StringIO
import json
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from flash_dataset.cli import RUN_DAILY_COMMAND
from flash_dataset.cli import RUN_ONCE_TOKEN
from flash_dataset.cli import VALIDATE_PARQUET_COMMAND
from flash_dataset.cli import main
from flash_dataset.runtime import DEFAULT_MAX_DAYS_BACK
from flash_dataset.runtime import DEFAULT_RUN_AT
from flash_dataset.runtime import DEFAULT_STORAGE_DIRECTORY
from flash_dataset.runtime import DEFAULT_WORKING_DIRECTORY

from tests.helpers import valid_day_tables
from tests.helpers import write_day_tables


class CliTests(unittest.TestCase):
    def test_main_prints_help_when_no_subcommand_is_given(self) -> None:
        stdout = StringIO()
        with contextlib.redirect_stdout(stdout):
            with patch.object(sys, "argv", ["flash-dataset"]):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertIn(RUN_DAILY_COMMAND, stdout.getvalue())
        self.assertIn(VALIDATE_PARQUET_COMMAND, stdout.getvalue())

    def test_run_daily_validates_new_day_and_prints_runtime_summary(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            stdout = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
                "--max-days-back",
                "0",
            ]
            with contextlib.redirect_stdout(stdout):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "validated")
            self.assertEqual(payload["storage_directory"], str(root))
            self.assertEqual(payload["working_directory"], str(working_directory))
            self.assertEqual(payload["reviewed_day_count"], 1)
            self.assertEqual(payload["fail_count"], 0)
            self.assertEqual(payload["latest_reviewed_day"], "2026-04-05")

    def test_run_daily_writes_idle_report_when_no_new_day_exists(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            (working_directory / "latest-reviewed.txt").parent.mkdir(parents=True)
            (working_directory / "latest-reviewed.txt").write_text(
                "2026-04-05\n", encoding="utf-8"
            )
            stdout = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
            ]
            with contextlib.redirect_stdout(stdout):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "idle")
            self.assertEqual(payload["reviewed_day_count"], 0)
            self.assertEqual(payload["latest_reviewed_day"], "2026-04-05")

    def test_run_daily_rejects_incompatible_date_partition_storage(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="date")
            stdout = StringIO()
            stderr = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
                "--max-days-back",
                "0",
            ]
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn(
                "run-daily requires top-level year=YYYY/month=MM/day=DD storage layout",
                stderr.getvalue(),
            )

    def test_run_daily_rejects_top_level_runtime_file(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            root.mkdir(parents=True, exist_ok=True)
            (root / "junk.parquet").write_bytes(b"runtime should not ignore files")
            stdout = StringIO()
            stderr = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
                "--max-days-back",
                "0",
            ]
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 2)
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("unsupported top-level entry", stderr.getvalue())

    def test_run_daily_returns_validated_status_for_parse_warning_only_day(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
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
            stdout = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
                "--max-days-back",
                "0",
            ]
            with contextlib.redirect_stdout(stdout):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "validated")
            self.assertIsNone(payload["blocked_day"])
            self.assertEqual(payload["latest_reviewed_day"], "2026-04-05")
            self.assertEqual(payload["reviewed_day_count"], 0)
            self.assertEqual(payload["warn_count"], 1)

    def test_run_daily_reports_failure_when_another_partition_in_same_day_fails(
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
            stdout = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
                "--max-days-back",
                "0",
            ]
            with contextlib.redirect_stdout(stdout):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 1)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "validated")
            self.assertIsNone(payload["blocked_day"])
            self.assertEqual(payload["fail_count"], 1)
            self.assertEqual(payload["latest_reviewed_day"], "2026-04-05")

    def test_run_daily_returns_blocked_status_when_earlier_pending_day_is_empty(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 7), layout="hour")
            empty_day = root / "year=2026" / "month=04" / "day=06"
            empty_day.mkdir(parents=True, exist_ok=True)
            (working_directory / "latest-reviewed.txt").parent.mkdir(parents=True)
            (working_directory / "latest-reviewed.txt").write_text(
                "2026-04-05\n", encoding="utf-8"
            )
            stdout = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
            ]
            with contextlib.redirect_stdout(stdout):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "validated")
            self.assertIsNone(payload["blocked_day"])
            self.assertEqual(payload["reviewed_day_count"], 1)
            self.assertEqual(payload["latest_reviewed_day"], "2026-04-07")

    def test_run_daily_skips_earlier_empty_day_when_later_day_is_reviewable(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 7), layout="hour")
            empty_day = root / "year=2026" / "month=04" / "day=06"
            empty_day.mkdir(parents=True, exist_ok=True)
            (working_directory / "latest-reviewed.txt").parent.mkdir(parents=True)
            (working_directory / "latest-reviewed.txt").write_text(
                "2026-04-05\n", encoding="utf-8"
            )
            stdout = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
            ]
            with contextlib.redirect_stdout(stdout):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "validated")
            self.assertIsNone(payload["blocked_day"])
            self.assertEqual(payload["reviewed_day_count"], 1)
            self.assertEqual(payload["latest_reviewed_day"], "2026-04-07")

    def test_run_daily_returns_validated_status_before_later_empty_day(
        self,
    ) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            empty_day = root / "year=2026" / "month=04" / "day=06"
            empty_day.mkdir(parents=True, exist_ok=True)
            stdout = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
                "--max-days-back",
                "1",
            ]
            with contextlib.redirect_stdout(stdout):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "validated")
            self.assertIsNone(payload["blocked_day"])
            self.assertEqual(payload["reviewed_day_count"], 1)
            self.assertEqual(payload["latest_reviewed_day"], "2026-04-05")

    def test_run_daily_allows_opt_in_transaction_hash_validation(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            stdout = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
                "--verify-transaction-hashes",
            ]
            with contextlib.redirect_stdout(stdout):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["status"], "validated")
            self.assertEqual(payload["fail_count"], 0)

    def test_run_daily_limits_review_window_relative_to_newest_day(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            working_directory = Path(directory) / "runtime"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            write_day_tables(root, date(2026, 4, 6), layout="hour")
            write_day_tables(root, date(2026, 4, 7), layout="hour")
            write_day_tables(root, date(2026, 4, 8), layout="hour")
            stdout = StringIO()
            argv = [
                "flash-dataset",
                RUN_DAILY_COMMAND,
                "--working-directory",
                str(working_directory),
                "--storage-directory",
                str(root),
                "--run-at",
                RUN_ONCE_TOKEN,
                "--max-days-back",
                "1",
            ]
            with contextlib.redirect_stdout(stdout):
                with patch.object(sys, "argv", argv):
                    exit_code = main()

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["reviewed_day_count"], 1)
            self.assertEqual(payload["latest_reviewed_day"], "2026-04-07")

    def test_run_daily_uses_default_scheduled_configuration(self) -> None:
        with patch("flash_dataset.cli.run_scheduled") as run_scheduled:
            with patch.object(sys, "argv", ["flash-dataset", RUN_DAILY_COMMAND]):
                exit_code = main()

        self.assertEqual(exit_code, 0)
        self.assertEqual(run_scheduled.call_count, 1)
        config = run_scheduled.call_args.args[0]
        self.assertEqual(config.working_directory, DEFAULT_WORKING_DIRECTORY)
        self.assertEqual(config.storage_directory, DEFAULT_STORAGE_DIRECTORY)
        self.assertEqual(config.run_at, DEFAULT_RUN_AT)
        self.assertEqual(config.max_days_back, DEFAULT_MAX_DAYS_BACK)

    def test_validate_parquet_writes_reports(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory) / "storage"
            output_directory = Path(directory) / "reports"
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            argv = [
                "flash-dataset",
                VALIDATE_PARQUET_COMMAND,
                "--storage-directory",
                str(root),
                "--output-directory",
                str(output_directory),
            ]
            with patch.object(sys, "argv", argv):
                exit_code = main()
            self.assertEqual(exit_code, 0)
            self.assertTrue((output_directory / "summary.md").exists())
            self.assertTrue((output_directory / "findings.jsonl").exists())


if __name__ == "__main__":
    unittest.main()
