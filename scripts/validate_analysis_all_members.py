import argparse
import json
import logging
from pathlib import Path

import pandas as pd
import requests


SCHEMA_URL_JSON = "https://michalskop.github.io/legislature-data-standard/dt.analyses/all-members/latest/schemas/all-members.dt.analyses.json"
SCHEMA_URL_TABLE = "https://michalskop.github.io/legislature-data-standard/dt.analyses/all-members/latest/schemas/all-members.dt.analyses.table.json"


def _load_schema(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def _validate_memberships_struct(m: object, label: str) -> None:
    if not isinstance(m, dict):
        raise ValueError(f"{label}: memberships must be object")
    for key in ["parliament", "groups", "candidate_list", "constituency"]:
        v = m.get(key)
        if v is None:
            continue
        if not isinstance(v, list):
            raise ValueError(f"{label}: memberships.{key} must be array")
        for i, it in enumerate(v[:50]):
            if not isinstance(it, dict):
                raise ValueError(f"{label}: memberships.{key}[{i}] must be object")
            for req in ["id", "name"]:
                if req not in it or not isinstance(it[req], str) or not it[req]:
                    raise ValueError(f"{label}: memberships.{key}[{i}] missing/empty {req}")
            for dkey in ["start_date", "end_date"]:
                if dkey in it and it[dkey] is not None and not isinstance(it[dkey], str):
                    raise ValueError(f"{label}: memberships.{key}[{i}].{dkey} must be string or null")


def validate_all_members(csv_path: Path, json_path: Path) -> None:
    # CSV: schema drift only
    table_schema = _load_schema(SCHEMA_URL_TABLE)
    fields = table_schema.get("fields", [])
    required = {f["name"] for f in fields if f.get("constraints", {}).get("required") is True}
    allowed = {f["name"] for f in fields}

    df = pd.read_csv(csv_path, dtype=str)
    cols = set(df.columns)
    missing = sorted(required - cols)
    unexpected = sorted(cols - allowed)
    if missing:
        raise ValueError(f"CSV {csv_path}: missing required columns: {missing}")
    if unexpected:
        raise ValueError(f"CSV {csv_path}: unexpected columns: {unexpected}")
    logging.info("Validated %s against %s", csv_path, SCHEMA_URL_TABLE)

    # JSON: enforce memberships structure
    json_schema = _load_schema(SCHEMA_URL_JSON)
    item = json_schema.get("definitions", {}).get("DtAnalysesAllMembers", {}).get("items", {})
    required_json = set(item.get("required", []))
    allowed_json = set(item.get("properties", {}).keys())

    records = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(records, list) or not records:
        raise ValueError(f"JSON {json_path}: expected non-empty list")

    for i, r in enumerate(records[:50]):
        if not isinstance(r, dict):
            raise ValueError(f"JSON {json_path}: record {i} expected object")
        miss = sorted([k for k in required_json if k not in r])
        if miss:
            raise ValueError(f"JSON {json_path}: record {i} missing required keys: {miss}")
        unexp = sorted([k for k in r.keys() if k not in allowed_json])
        if unexp:
            raise ValueError(f"JSON {json_path}: record {i} unexpected keys: {unexp}")
        _validate_memberships_struct(r.get("memberships") or {}, f"JSON {json_path} record {i}")

    logging.info("Validated %s against %s", json_path, SCHEMA_URL_JSON)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="analyses/all-members/outputs/all_members.csv")
    parser.add_argument("--json", default="analyses/all-members/outputs/all_members.json")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    validate_all_members(Path(args.csv), Path(args.json))


if __name__ == "__main__":
    main()
