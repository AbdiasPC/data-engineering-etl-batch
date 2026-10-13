"""CLI entrypoint: `python -m src.pipeline [command] --start YYYY-MM --end YYYY-MM`."""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Callable, Sequence

from src.config import ConfigError, PipelineConfig, SnowflakeConfig
from src.extract import run_extract
from src.logging_utils import configure_logging
from src.snowflake_client import check_connection

logger = logging.getLogger("pipeline")

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2


def _extract(config: PipelineConfig) -> None:
    run_extract(config)


def _check_connection(config: PipelineConfig) -> None:
    check_connection(SnowflakeConfig.from_env())


# Each command takes the resolved config and raises on failure.
COMMANDS: dict[str, Callable[[PipelineConfig], None]] = {
    "check-connection": _check_connection,
    "extract": _extract,
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m src.pipeline",
        description="NYC taxi + weather batch ELT: extract public data and load it to Snowflake.",
    )
    parser.add_argument("command", choices=sorted(COMMANDS), help="step to run")
    parser.add_argument("--start", help="first month, YYYY-MM (default: PIPELINE_START_MONTH)")
    parser.add_argument("--end", help="last month, YYYY-MM (default: PIPELINE_END_MONTH)")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(os.environ.get("LOG_LEVEL", "INFO"))
    try:
        config = PipelineConfig.from_env(start=args.start, end=args.end)
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
