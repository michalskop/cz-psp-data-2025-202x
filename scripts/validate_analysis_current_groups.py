import argparse
import json
import logging
from pathlib import Path

import requests


SCHEMA_URL = "https://michalskop.github.io/legislature-data-standard/dt.analyses/current-groups/latest/schemas/current-groups.dt.analyses.json"


def _load_schema(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def validate_current_groups_json(json_path: Path, schema_url: str = SCHEMA_URL) -> None:
    schema = _load_schema(schema_url)
    defs = schema.get("definitions", {})
    dt = defs.get("DtAnalysesCurrentGroups", {})

    records = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(records, list):
        raise ValueError(f"{json_path}: expected list")

    item = dt.get("items", {})
    required = set(item.get("required", []))
    allowed = set(item.get("properties", {}).keys())

    if not records:
        raise ValueError(f"{json_path}: empty list")

    for i, r in enumerate(records[:50]):
        if not isinstance(r, dict):
            raise ValueError(f"{json_path}: record {i} expected object")

        missing = sorted([k for k in required if k not in r])
        if missing:
            raise ValueError(f"{json_path}: record {i} missing required keys: {missing}")

        unexpected = sorted([k for k in r.keys() if k not in allowed])
        if unexpected:
            raise ValueError(f"{json_path}: record {i} unexpected keys: {unexpected}")

        if not isinstance(r.get("name"), str) or not r["name"]:
            raise ValueError(f"{json_path}: record {i} name must be non-empty string")

        # type checks for array fields
        for k, prop in item.get("properties", {}).items():
            t = prop.get("type")
            if k not in r or r[k] is None:
                continue
            if t == "array" and not isinstance(r[k], list):
                raise ValueError(f"{json_path}: record {i} field {k} expected array")

    logging.info("Validated %s against %s", json_path, schema_url)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default="analyses/current-groups/outputs/current_groups.json")
    parser.add_argument("--schema-url", default=SCHEMA_URL)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    validate_current_groups_json(Path(args.json), args.schema_url)


if __name__ == "__main__":
    main()
