"""Error definitions for feed-base parquet validation."""

from __future__ import annotations


class ValidatorConfigurationError(ValueError):
    """Raised when validator configuration points at invalid filesystem inputs."""
