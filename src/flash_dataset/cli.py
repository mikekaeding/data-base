"""CLI for feed-base parquet validation and daily scheduling."""

from __future__ import annotations

import argparse
from argparse import Namespace
from datetime import date
from datetime import time
import json
from pathlib import Path
import sys

from .errors import ValidatorConfigurationError
from .runtime import DEFAULT_MAX_DAYS_BACK
from .runtime import DEFAULT_RUN_AT
from .runtime import DEFAULT_STORAGE_DIRECTORY
from .runtime import DEFAULT_WORKING_DIRECTORY
from .runtime import parse_daily_utc_time
from .runtime import RunSummary
from .runtime import run_once
from .runtime import run_scheduled
from .runtime import RuntimeConfig
from .validator import run_validation
from .validator import ValidatorConfig

RUN_ONCE_TOKEN = "off"
RUN_DAILY_COMMAND = "run-daily"
VALIDATE_PARQUET_COMMAND = "validate-parquet"
DEFAULT_OUTPUT_DIRECTORY = Path("reports")


def main() -> int:
    """Run the CLI program and return a process exit code."""
    parser = build_parser()
    arguments = parser.parse_args()
    if arguments.command is None:
        parser.print_help()
        return 0
    if arguments.command == VALIDATE_PARQUET_COMMAND:
        return run_validate_parquet_command(parser, arguments)
    if arguments.command == RUN_DAILY_COMMAND:
        return run_daily_command(arguments)
    parser.print_help()
    return 0


def build_parser() -> argparse.ArgumentParser:
    """Build the CLI argument parser."""
    parser = argparse.ArgumentParser(
        prog="flash-dataset",
        description="Parquet validation and daily scheduling.",
    )
    subparsers = parser.add_subparsers(dest="command")

    validate_parquet_parser = subparsers.add_parser(
        VALIDATE_PARQUET_COMMAND,
        help="Run parquet validation over one optional date range.",
    )
    validate_parquet_parser.add_argument(
        "--storage-directory",
        default=str(DEFAULT_STORAGE_DIRECTORY),
        help="feed-base parquet storage directory",
    )
    validate_parquet_parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
        help="directory that receives validation reports",
    )
    validate_parquet_parser.add_argument(
        "--date-from",
        type=_parse_optional_date,
        help="inclusive lower ISO day bound such as 2026-04-07",
    )
    validate_parquet_parser.add_argument(
        "--date-to",
        type=_parse_optional_date,
        help="inclusive upper ISO day bound such as 2026-04-07",
    )
    validate_parquet_parser.add_argument(
        "--verify-transaction-hashes",
        action="store_true",
        help="run keccak(transaction_rlp) == transaction_hash validation",
    )

    run_daily_parser = subparsers.add_parser(
        RUN_DAILY_COMMAND,
        help="Run the UTC-scheduled validation loop over unseen day folders.",
    )
    run_daily_parser.add_argument(
        "--working-directory",
        default=str(DEFAULT_WORKING_DIRECTORY),
        help="directory that receives reports and the latest-reviewed watermark",
    )
    run_daily_parser.add_argument(
        "--storage-directory",
        default=str(DEFAULT_STORAGE_DIRECTORY),
        help="feed-base parquet storage directory",
    )
    run_daily_parser.add_argument(
        "--run-at",
        type=_parse_run_at,
        default=DEFAULT_RUN_AT,
        help=(
            "daily UTC run time in 12-hour format, for example 6:00AM; "
            "set off to run once immediately"
        ),
    )
    run_daily_parser.add_argument(
        "--max-days-back",
        type=int,
        default=DEFAULT_MAX_DAYS_BACK,
        help="maximum day distance from the newest discovered folder to inspect",
    )
    run_daily_parser.add_argument(
        "--verify-transaction-hashes",
        action="store_true",
        help="run keccak(transaction_rlp) == transaction_hash validation",
    )
    return parser


def run_validate_parquet_command(
    parser: argparse.ArgumentParser,
    arguments: Namespace,
) -> int:
    """Run the one-shot parquet validator command."""
    date_from = arguments.date_from
    date_to = arguments.date_to
    if date_from is not None and date_to is not None and date_from > date_to:
        parser.error("--date-from must be on or before --date-to")

    config = ValidatorConfig(
        storage_root=Path(arguments.storage_directory),
        output_directory=Path(arguments.output_directory),
        date_from=date_from,
        date_to=date_to,
        verify_transaction_hashes=arguments.verify_transaction_hashes,
    )
    try:
        result = run_validation(config)
    except ValidatorConfigurationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 1 if result.has_failures() else 0


def run_daily_command(arguments: Namespace) -> int:
    """Run the scheduled validator command."""
    config = RuntimeConfig(
        working_directory=Path(arguments.working_directory),
        storage_directory=Path(arguments.storage_directory),
        run_at=arguments.run_at,
        max_days_back=arguments.max_days_back,
        verify_transaction_hashes=arguments.verify_transaction_hashes,
    )
    try:
        if config.run_at is None:
            summary = run_once(config)
            _print_summary(summary)
            return summary.exit_code()
        run_scheduled(config, emit_summary=_print_summary)
        return 0
    except ValidatorConfigurationError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2


def _print_summary(summary: RunSummary) -> None:
    """Emit one compact JSON summary for logs and one-shot execution."""
    print(json.dumps(summary.to_payload(), sort_keys=True))


def _parse_optional_date(value: str) -> date:
    """Parse one ISO date argument for argparse."""
    try:
        return date.fromisoformat(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"invalid ISO date: {value}") from error


def _parse_run_at(value: str) -> time | None:
    """Parse one CLI daily UTC time string."""
    if value.strip().lower() == RUN_ONCE_TOKEN:
        return None
    try:
        return parse_daily_utc_time(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(str(error)) from error


if __name__ == "__main__":
    raise SystemExit(main())
