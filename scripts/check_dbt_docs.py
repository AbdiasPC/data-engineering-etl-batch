"""Fail if any dbt model, source table, or declared column lacks a description.

Reads dbt/target/manifest.json (produced by `dbt parse`). If dbt/target/catalog.json
exists (produced by `dbt docs generate`), also fails when a column that exists in the
warehouse is missing from schema.yml.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

TARGET = Path(__file__).resolve().parent.parent / "dbt" / "target"
PROJECT = "nyc_taxi"


def find_problems(manifest: dict, catalog: dict | None) -> list[str]:
    problems: list[str] = []
    nodes = {**manifest["nodes"], **manifest["sources"]}
    for unique_id, node in sorted(nodes.items()):
        if node["package_name"] != PROJECT or node["resource_type"] not in ("model", "source"):
            continue
        if not node.get("description", "").strip():
            problems.append(f"{unique_id}: no description")
        documented = {name.lower(): column for name, column in node["columns"].items()}
        problems += [
            f"{unique_id}.{name}: no column description"
            for name, column in documented.items()
            if not column.get("description", "").strip()
        ]
        if catalog and node["resource_type"] == "model":
            actual = catalog["nodes"].get(unique_id, {}).get("columns", {})
            problems += [
                f"{unique_id}.{name.lower()}: column exists in the warehouse but is undocumented"
                for name in actual
                if name.lower() not in documented
            ]
    return problems


def main() -> int:
    manifest = json.loads((TARGET / "manifest.json").read_text())
    catalog_path = TARGET / "catalog.json"
    catalog = json.loads(catalog_path.read_text()) if catalog_path.exists() else None
    problems = find_problems(manifest, catalog)
    for problem in problems:
        print(problem)
    scope = "manifest and catalog" if catalog else "manifest only"
    print(f"dbt docs check ({scope}): {len(problems)} problem(s)")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
