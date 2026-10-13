"""Transform: run dbt (deps, source freshness, build) as subprocesses."""

from __future__ import annotations

import logging
import subprocess

logger = logging.getLogger(__name__)

# dbt reads DBT_PROJECT_DIR and DBT_PROFILES_DIR, both set in the image.
DBT_STEPS: tuple[tuple[str, ...], ...] = (
    ("dbt", "deps"),
    ("dbt", "source", "freshness"),  # load-recency check; warns, never fails the run
    ("dbt", "build"),  # models and tests in dependency order; test errors fail the run
)


def run_transform() -> None:
    """Run each dbt step in order; a non-zero exit raises CalledProcessError."""
    for step in DBT_STEPS:
        logger.info("Running %s", " ".join(step))
        subprocess.run(step, check=True)  # noqa: S603 - fixed argv, no shell
