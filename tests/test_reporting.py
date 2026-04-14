from __future__ import annotations

from datetime import date
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest

from flash_dataset.validator import run_validation

from tests.helpers import validator_config
from tests.helpers import write_day_tables


class ReportingTests(unittest.TestCase):
    def test_run_validation_writes_summary_and_findings_reports(self) -> None:
        with TemporaryDirectory() as directory:
            root = Path(directory)
            write_day_tables(root, date(2026, 4, 5), layout="hour")
            config = validator_config(root)

            result = run_validation(config)

            summary_path = config.output_directory / "summary.md"
            findings_path = config.output_directory / "findings.jsonl"
            self.assertTrue(summary_path.is_file())
            self.assertTrue(findings_path.is_file())

            summary_text = summary_path.read_text(encoding="utf-8")
            self.assertIn("# feed-base parquet validation", summary_text)
            self.assertIn("reviewed partitions", summary_text)
            self.assertIn("received_time_slot_coverage", summary_text)
            self.assertIn("dataset_physical_stats", summary_text)

            findings = [
                json.loads(line)
                for line in findings_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(findings), len(result.findings))
            self.assertTrue(
                any(
                    finding["rule_id"] == "received_time_slot_coverage"
                    for finding in findings
                )
            )
            self.assertTrue(
                any(
                    finding["rule_id"] == "dataset_physical_stats"
                    for finding in findings
                )
            )


if __name__ == "__main__":
    unittest.main()
