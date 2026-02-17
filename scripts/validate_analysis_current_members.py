import argparse
import json
import logging
from pathlib import Path

import pandas as pd
import requests


SCHEMA_URL = "https://michalskop.github.io/legislature-data-standard/dt.analyses/current-members/latest/schemas/current-members.dt.analyses.table.json"


def _load_schema(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def _validate_records(records: list[dict], schema: dict, label: str) -> None:
    fields = schema.get("fields", [])
    required = {f["name"] for f in fields if f.get("constraints", {}).get("required") is True}
    allowed = {f["name"] for f in fields}

    if not records:
        raise ValueError(f"{label}: no records")

    keys = set(records[0].keys())
    missing = sorted(required - keys)
    unexpected = sorted(keys - allowed)

    if missing:
        raise ValueError(f"{label}: missing required keys: {missing}")
    if unexpected:
        raise ValueError(f"{label}: unexpected keys (schema drift): {unexpected}")

    for i, r in enumerate(records[:50]):
        for k in required:
            if r.get(k) in (None, ""):
                raise ValueError(f"{label}: record {i} has empty required field {k}")


def _validate_json_types(records: list[dict], schema: dict, label: str) -> None:
    fields = schema.get("fields", [])
    types = {f["name"]: f.get("type") for f in fields}

    for i, r in enumerate(records[:50]):
        for k, t in types.items():
            if k not in r:
                continue
            v = r.get(k)
            if v is None:
                continue
            if t == "array" and not isinstance(v, list):
                raise ValueError(f"{label}: record {i} field {k} expected array, got {type(v).__name__}")
            if t == "object" and not isinstance(v, dict):
                raise ValueError(f"{label}: record {i} field {k} expected object, got {type(v).__name__}")


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


def validate_outputs(csv_path: Path, json_path: Path, schema_url: str = SCHEMA_URL) -> None:
    schema = _load_schema(schema_url)

    df = pd.read_csv(csv_path, dtype=str).fillna("")
    records_csv = df.to_dict(orient="records")
    _validate_records(records_csv, schema, f"CSV {csv_path}")
    logging.info("Validated %s against %s", csv_path, schema_url)

    records_json = json.loads(json_path.read_text(encoding="utf-8"))
    if not isinstance(records_json, list):
        raise ValueError(f"JSON {json_path}: expected a list")
    _validate_records(records_json, schema, f"JSON {json_path}")
    _validate_json_types(records_json, schema, f"JSON {json_path}")
    for i, r in enumerate(records_json[:50]):
        if "memberships" in r and r["memberships"] is not None:
            _validate_memberships_struct(r["memberships"], f"JSON {json_path} record {i}")
    logging.info("Validated %s against %s", json_path, schema_url)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", default="analyses/current-members/outputs/current_members.csv")
    parser.add_argument("--json", default="analyses/current-members/outputs/current_members.json")
    parser.add_argument("--schema-url", default=SCHEMA_URL)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    validate_outputs(Path(args.csv), Path(args.json), args.schema_url)


if __name__ == "__main__":
    main()
