"""Report generation for parquet validation runs."""

from __future__ import annotations

import json

from ..errors import ValidatorConfigurationError
from .common import ValidationResult
from .common import ValidatorConfig


def write_reports(config: ValidatorConfig, result: ValidationResult) -> None:
    """Write summary and findings reports for one validation run."""
    try:
        config.output_directory.mkdir(parents=True, exist_ok=True)
        summary_path = config.output_directory / "summary.md"
        findings_path = config.output_directory / "findings.jsonl"
        summary_path.write_text(_summary_markdown(config, result), encoding="utf-8")
        findings_path.write_text(
            "".join(f"{finding.to_json()}\n" for finding in result.findings),
            encoding="utf-8",
        )
    except OSError as error:
        raise ValidatorConfigurationError(
            f"failed to write reports: {config.output_directory}"
        ) from error


def summary_payload(result: ValidationResult) -> dict[str, object]:
    """Return one compact JSON-safe summary payload."""
    counts = result.finding_counts_by_status()
    return {
        "scanned_files": result.scanned_files,
        "scanned_rows": result.scanned_rows,
        "reviewed_partitions": result.reviewed_partitions,
        "fail_count": counts["fail"],
        "warn_count": counts["warn"],
        "metric_count": counts["metric"],
        "has_failures": result.has_failures(),
        "rule_counts": result.rule_counts,
    }


def _summary_markdown(config: ValidatorConfig, result: ValidationResult) -> str:
    payload = summary_payload(result)
    lines = [
        "# feed-base parquet validation",
        "",
        f"- storage root: `{config.storage_root}`",
        f"- scanned files: `{payload['scanned_files']}`",
        f"- scanned rows: `{payload['scanned_rows']}`",
        f"- reviewed partitions: `{', '.join(result.reviewed_partitions) if result.reviewed_partitions else 'none'}`",
        f"- fail findings: `{payload['fail_count']}`",
        f"- warn findings: `{payload['warn_count']}`",
        f"- metric findings: `{payload['metric_count']}`",
        "",
        "## Rule Counts",
    ]
    if not result.rule_counts:
        lines.append("- no findings")
    else:
        for rule_id in sorted(result.rule_counts):
            lines.append(f"- `{rule_id}`: `{result.rule_counts[rule_id]}`")
    lines.extend(
        [
            "",
            "## Summary Payload",
            "",
            "```json",
            json.dumps(payload, sort_keys=True, indent=2),
            "```",
        ]
    )
    return "\n".join(lines) + "\n"
