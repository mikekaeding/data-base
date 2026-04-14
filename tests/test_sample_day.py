from __future__ import annotations

from pathlib import Path
import unittest

from flash_dataset.validator import run_validation
from tests.helpers import validator_config


class SampleDayTests(unittest.TestCase):
    def test_sample_day_has_no_failures(self) -> None:
        root = Path(__file__).resolve().parents[1] / "data"
        if not any(root.rglob("*.parquet")):
            self.skipTest("sample parquet data is not present in this workspace")
        output_directory = (
            Path(__file__).resolve().parents[1] / "out" / "sample-validation"
        )
        config = validator_config(root, output_directory=output_directory)

        result = run_validation(config)

        self.assertFalse(result.has_failures())
        self.assertTrue(
            any(
                finding.rule_id == "payload_starts_after_zero"
                for finding in result.findings
            )
        )


if __name__ == "__main__":
    unittest.main()
