"""Shared validator types and low-level helpers."""

from __future__ import annotations

from collections.abc import Iterator
import json
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING
from typing import Any
from typing import Literal
from typing import cast

import pyarrow as pa

if TYPE_CHECKING:
    from .baselines import PartitionSnapshot

Severity = Literal["critical", "high", "medium", "low"]
Status = Literal["fail", "warn", "metric"]
MAX_STORED_EXAMPLES_PER_RULE = 25

SEVERITY_RANK: dict[Severity, int] = {
    "critical": 0,
    "high": 1,
    "medium": 2,
    "low": 3,
}


def _empty_findings() -> list[Finding]:
    """Return one empty findings list with a precise element type."""
    return []


def _empty_partition_labels() -> list[str]:
    """Return one empty reviewed-partition list with a precise element type."""
    return []


def _empty_rule_counts() -> dict[str, int]:
    """Return one empty integer counter map for rule counts."""
    return {}


def _empty_partition_snapshots() -> list[PartitionSnapshot]:
    """Return one empty partition-snapshot list with a precise element type."""
    return []


def _empty_status_counts() -> dict[Status, int]:
    """Return one empty status counter map."""
    return {"fail": 0, "warn": 0, "metric": 0}


@dataclass(frozen=True)
class ColumnProfile:
    """One parquet column shape.

    Args:
        name: Stable parquet column name.
        type_name: Arrow type string expected in the parquet footer schema.
        nullable: Whether the parquet field may be null.
    """

    name: str
    type_name: str
    nullable: bool

    @classmethod
    def from_field(cls, field: pa.Field) -> ColumnProfile:
        """Build one profile from one Arrow schema field."""
        return cls(
            name=field.name,
            type_name=str(field.type),
            nullable=field.nullable,
        )


@dataclass(frozen=True)
class TableProfile:
    """Declares the exact schema expected for one parquet dataset.

    Args:
        columns: Ordered physical parquet columns from the file footer.
    """

    columns: tuple[ColumnProfile, ...]


@dataclass(frozen=True)
class RuleDefinition:
    """Metadata for one validation rule.

    Args:
        rule_id: Stable rule identifier.
        severity: Rule severity used in reports.
        status: Rule outcome classification.
        description: Short human description.
    """

    rule_id: str
    severity: Severity
    status: Status
    description: str


@dataclass(frozen=True)
class ValidatorConfig:
    """Runtime configuration for parquet validation.

    Args:
        storage_root: Root directory containing feed-base parquet output.
        output_directory: Directory where reports are written.
        date_from: Optional inclusive lower day bound.
        date_to: Optional inclusive upper day bound.
        verify_transaction_hashes: Whether to run keccak(transaction_rlp) checks.
    """

    storage_root: Path
    output_directory: Path
    date_from: date | None
    date_to: date | None
    verify_transaction_hashes: bool = False


@dataclass(frozen=True, order=True)
class PartitionIdentity:
    """Logical validation partition.

    Args:
        partition_date: Calendar date assigned by the storage layout.
        partition_hour: Optional UTC hour bucket for hour-partitioned storage.
    """

    partition_date: date
    partition_hour: int | None = None

    def label(self) -> str:
        """Return one stable string label for reports and findings."""
        if self.partition_hour is None:
            return self.partition_date.isoformat()
        return f"{self.partition_date.isoformat()}T{self.partition_hour:02d}:00Z"


@dataclass(frozen=True)
class ParsedStoragePath:
    """Path metadata extracted from one parquet file location.

    Args:
        dataset: Logical feed-base dataset name.
        partition: Parsed partition assigned by the storage layout.
        layout: Storage layout identifier.
        warning: Optional warning produced while parsing the path.
    """

    dataset: str
    partition: PartitionIdentity
    layout: str
    warning: Finding | None = None


@dataclass(frozen=True)
class FileIndexEntry:
    """Indexed metadata for one parquet file.

    Args:
        path: Absolute parquet file path.
        dataset: Logical feed-base dataset name.
        partition: Parsed partition assigned by the storage layout.
        layout: Storage layout identifier.
        row_count: Physical parquet row count.
        file_size_bytes: On-disk parquet file size in bytes.
        row_groups: Physical parquet row-group count.
        compressed_bytes: Sum of parquet column-chunk compressed sizes.
        uncompressed_bytes: Sum of parquet column-chunk uncompressed sizes.
        schema: Exact footer schema profile.
    """

    path: Path
    dataset: str
    partition: PartitionIdentity
    layout: str
    row_count: int
    file_size_bytes: int
    row_groups: int
    compressed_bytes: int
    uncompressed_bytes: int
    schema: tuple[ColumnProfile, ...]

    def compression_ratio(self) -> float | None:
        """Return the uncompressed-to-compressed parquet ratio when available."""
        if self.compressed_bytes <= 0:
            return None
        return self.uncompressed_bytes / self.compressed_bytes


@dataclass(frozen=True)
class BuildFileEntryOutcome:
    """Metadata extraction result for one parquet file.

    Args:
        partition: Parsed partition when the path layout was understood.
        entry: Indexed metadata when the parquet file is readable.
        warning: Optional warning or failure finding for this file.
    """

    partition: PartitionIdentity | None
    entry: FileIndexEntry | None
    warning: Finding | None


@dataclass(frozen=True)
class Finding:
    """One machine-readable validation result.

    Args:
        rule_id: Stable rule identifier.
        status: Fail, warn, or metric classification.
        severity: Rule severity.
        message: Human-readable finding summary.
        day: Optional affected partition day.
        partition: Optional affected partition label.
        dataset: Optional affected dataset name.
        file: Optional affected file path.
        count: Optional aggregate count associated with the finding.
        example: Optional compact structured context.
    """

    rule_id: str
    status: Status
    severity: Severity
    message: str
    day: str | None = None
    partition: str | None = None
    dataset: str | None = None
    file: str | None = None
    count: int | None = None
    example: dict[str, str | int | float | bool | None] | None = None

    def to_json(self) -> str:
        """Serialize one finding into a JSON line."""
        payload: dict[str, Any] = {
            "rule_id": self.rule_id,
            "status": self.status,
            "severity": self.severity,
            "message": self.message,
            "day": self.day,
            "partition": self.partition,
            "dataset": self.dataset,
            "file": self.file,
            "count": self.count,
            "example": self.example,
        }
        return json.dumps(payload, sort_keys=True)


@dataclass
class ValidationResult:
    """Aggregated findings and scan stats for one validation run."""

    findings: list[Finding] = field(default_factory=_empty_findings)
    scanned_files: int = 0
    scanned_rows: int = 0
    partition_snapshots: list[PartitionSnapshot] = field(
        default_factory=_empty_partition_snapshots
    )
    reviewed_partitions: list[str] = field(default_factory=_empty_partition_labels)
    rule_counts: dict[str, int] = field(default_factory=_empty_rule_counts)
    examples_per_rule: dict[str, int] = field(default_factory=_empty_rule_counts)
    max_examples_per_rule: int = MAX_STORED_EXAMPLES_PER_RULE
    _status_counts: dict[Status, int] = field(default_factory=_empty_status_counts)

    def add(self, finding: Finding) -> None:
        """Append one finding and update aggregate counters."""
        self.rule_counts[finding.rule_id] = self.rule_counts.get(finding.rule_id, 0) + 1
        self._status_counts[finding.status] += 1
        if finding.example is not None:
            stored = self.examples_per_rule.get(finding.rule_id, 0)
            if stored >= self.max_examples_per_rule:
                finding = Finding(
                    rule_id=finding.rule_id,
                    status=finding.status,
                    severity=finding.severity,
                    message=finding.message,
                    day=finding.day,
                    partition=finding.partition,
                    dataset=finding.dataset,
                    file=finding.file,
                    count=finding.count,
                    example=None,
                )
            else:
                self.examples_per_rule[finding.rule_id] = stored + 1
        self.findings.append(finding)

    def has_failures(self) -> bool:
        """Return whether the run produced at least one failing finding."""
        return self._status_counts["fail"] > 0

    def finding_counts_by_status(self) -> dict[Status, int]:
        """Return total findings grouped by status."""
        return dict(self._status_counts)

    def failure_count(self) -> int:
        """Return the current number of failing findings in the run."""
        return self._status_counts["fail"]

    def issue_count(self) -> int:
        """Return the current number of fail and warn findings in the run."""
        return self._status_counts["fail"] + self._status_counts["warn"]

    def max_severity(self) -> Severity | None:
        """Return the highest-severity finding in the run."""
        if not self.findings:
            return None
        return min(
            (finding.severity for finding in self.findings),
            key=lambda severity: SEVERITY_RANK[severity],
        )


def iter_table_rows(
    table: pa.Table, batch_size: int = 65_536
) -> Iterator[dict[str, Any]]:
    """Yield Arrow rows in bounded Python batches to avoid full-table materialization."""
    for batch in table.to_batches(max_chunksize=batch_size):
        for row in batch.to_pylist():
            yield cast(dict[str, Any], row)
