import argparse
import json
import logging
from pathlib import Path

import requests


SCHEMA_URL = "https://michalskop.github.io/legislature-data-standard/dt.analyses/current-term/latest/schemas/current-term.dt.analyses.json"


def _load_schema(url: str) -> dict:
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    return r.json()


def validate_current_term_json(json_path: Path, schema_url: str = SCHEMA_URL) -> None:
    schema = _load_schema(schema_url)
    defs = schema.get("definitions", {})
    dt = defs.get("DtAnalysesCurrentTerm", {})

    obj = json.loads(json_path.read_text(encoding="utf-8"))
    if obj is None:
        raise ValueError(f"{json_path}: must not be null")
    if not isinstance(obj, dict):
        raise ValueError(f"{json_path}: expected object")

    required = set(dt.get("anyOf", [])[0].get("required", []))
    missing = sorted([k for k in required if k not in obj])
    if missing:
        raise ValueError(f"{json_path}: missing required keys: {missing}")

    allowed = set(dt.get("anyOf", [])[0].get("properties", {}).keys())
    unexpected = sorted([k for k in obj.keys() if k not in allowed])
    if unexpected:
        raise ValueError(f"{json_path}: unexpected keys: {unexpected}")

    if not isinstance(obj.get("id"), str) or not obj["id"]:
        raise ValueError(f"{json_path}: id must be non-empty string")
    if not isinstance(obj.get("name"), str) or not obj["name"]:
        raise ValueError(f"{json_path}: name must be non-empty string")
    if not isinstance(obj.get("since"), str) or not obj["since"]:
        raise ValueError(f"{json_path}: since must be non-empty string")

    identifiers = obj.get("identifiers")
    if identifiers is not None:
        if not isinstance(identifiers, list):
            raise ValueError(f"{json_path}: identifiers must be array")
        for i, it in enumerate(identifiers[:20]):
            if not isinstance(it, dict):
                raise ValueError(f"{json_path}: identifiers[{i}] must be object")
            if set(it.keys()) != {"scheme", "identifier"}:
                raise ValueError(f"{json_path}: identifiers[{i}] must have exactly scheme+identifier")

    logging.info("Validated %s against %s", json_path, schema_url)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default="analyses/current-term/outputs/current_term.json")
    parser.add_argument("--schema-url", default=SCHEMA_URL)
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    validate_current_term_json(Path(args.json), args.schema_url)


if __name__ == "__main__":
    main()
