"""CLI entrypoint: `python -m src.pipeline [command] --start YYYY-MM --end YYYY-MM`.

With no command the whole pipeline runs: extract -> load -> transform -> validate.
Exit codes: 0 success, 1 a step failed, 2 invalid configuration.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Callable, Sequence

from src.config import ConfigError, PipelineConfig, SnowflakeConfig
from src.extract import run_extract
from src.load import run_load
from src.logging_utils import configure_logging
from src.snowflake_client import check_connection
from src.transform import run_transform
from src.validate import run_analytics, run_validation

logger = logging.getLogger("pipeline")

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2


def _extract(config: PipelineConfig) -> None:
    run_extract(config)


def _load(config: PipelineConfig) -> None:
    run_load(config, SnowflakeConfig.from_env())


def _check_connection(config: PipelineConfig) -> None:
    check_connection(SnowflakeConfig.from_env())


def _transform(config: PipelineConfig) -> None:
    run_transform()


def _validate(config: PipelineConfig) -> None:
    run_validation(SnowflakeConfig.from_env())


def _analytics(config: PipelineConfig) -> None:
    run_analytics(SnowflakeConfig.from_env())


def _all(config: PipelineConfig) -> None:
    """Run every step in order, stopping at the first failure."""
    for name in ("extract", "load", "transform", "validate"):
        logger.info("Pipeline step starting", extra={"step": name})
        COMMANDS[name](config)


# Each command takes the resolved config and raises on failure.
COMMANDS: dict[str, Callable[[PipelineConfig], None]] = {
    "all": _all,
    "analytics": _analytics,
    "check-connection": _check_connection,
    "extract": _extract,
    "load": _load,
    "transform": _transform,
    "validate": _validate,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.pipeline",
        description="NYC taxi + weather batch ELT: extract public data and load it to Snowflake.",
    )
    parser.add_argument(
        "command",
        nargs="?",
        default="all",
        choices=sorted(COMMANDS),
        help="step to run (default: all = extract, load, transform, validate)",
    )
    parser.add_argument("--start", help="first month, YYYY-MM (default: PIPELINE_START_MONTH)")
    parser.add_argument("--end", help="last month, YYYY-MM (default: PIPELINE_END_MONTH)")
    parser.add_argument(
        "--force", action="store_true", help="reload files even if unchanged since the last load"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    try:
        config = PipelineConfig.from_env(start=args.start, end=args.end, force_reload=args.force)
    except ConfigError as exc:
        logger.error("Invalid configuration: %s", exc)
        return EXIT_CONFIG

    logger.info(
        "Starting %s",
        args.command,
        extra={"command": args.command, "months": [str(m) for m in config.months]},
    )
    try:
        COMMANDS[args.command](config)
    except ConfigError as exc:
        logger.error("Invalid configuration: %s", exc)
        return EXIT_CONFIG
    except Exception:
        logger.exception("%s failed", args.command)
        return EXIT_FAILED
    logger.info("Finished %s", args.command, extra={"command": args.command})
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
