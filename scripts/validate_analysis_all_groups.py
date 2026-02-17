import argparse
import json
import logging
from pathlib import Path

import pandas as pd
import requests


SCHEMA_URL_JSON = "https://michalskop.github.io/legislature-data-standard/dt.analyses/all-groups/latest/schemas/all-groups.dt.analyses.json"
SCHEMA_URL_TABLE = "https://michalskop.github.io/legislature-data-standard/dt.analyses/all-groups/latest/schemas/all-groups.dt.analyses.table.json"


def _load_schema(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def _validate_records(records: list[dict], allowed: set[str], required: set[str], label: str) -> None:
    if not records:
        raise ValueError(f"{label}: empty")

    for i, r in enumerate(records[:50]):
        if not isinstance(r, dict):
            raise ValueError(f"{label}: record {i} expected object")
        missing = sorted([k for k in required if k not in r])
        if missing:
            raise ValueError(f"{label}: record {i} missing required keys: {missing}")
        unexpected = sorted([k for k in r.keys() if k not in allowed])
        if unexpected:
            raise ValueError(f"{label}: record {i} unexpected keys: {unexpected}")
        if not isinstance(r.get("name"), str) or not r["name"]:
            raise ValueError(f"{label}: record {i} name must be non-empty string")


def validate_all_groups(csv_path: Path, json_path: Path) -> None:
    table_schema = _load_schema(SCHEMA_URL_TABLE)
    fields = table_schema.get("fields", [])
    allowed = {f["name"] for f in fields}
    required = {f["name"] for f in fields if f.get("constraints", {}).get("required") is True}

    df = pd.read_csv(csv_path, dtype=str)
    cols = set(df.columns)
    missing_cols = sorted(required - cols)
    unexpected_cols = sorted(cols - allowed)
    if missing_cols:
        raise ValueError(f"CSV {csv_path}: missing required columns: {missing_cols}")
    if unexpected_cols:
        raise ValueError(f"CSV {csv_path}: unexpected columns: {unexpected_cols}")
    logging.info("Validated %s against %s", csv_path, SCHEMA_URL_TABLE)

    records = json.loads(json_path.read_text(encoding="utf-8"))
    json_schema = _load_schema(SCHEMA_URL_JSON)
    item = json_schema.get("definitions", {}).get("DtAnalysesAllGroups", {}).get("items", {})
    required_json = set(item.get("required", []))
    allowed_json = set(item.get("properties", {}).keys())
    _validate_records(records, allowed_json, required_json, f"JSON {json_path}")
    logging.info("Validated %s against %s", json_path, SCHEMA_URL_JSON)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="analyses/all-groups/outputs/all_groups.csv")
    parser.add_argument("--json", default="analyses/all-groups/outputs/all_groups.json")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    validate_all_groups(Path(args.csv), Path(args.json))


if __name__ == "__main__":
    main()
